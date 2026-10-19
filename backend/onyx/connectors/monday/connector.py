"""monday.com connector: one document per item, with column values and updates."""

import json
import os
from datetime import datetime, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil import parser

from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import ConnectorValidationError, CredentialExpiredError
from onyx.connectors.interfaces import (
    CheckpointedConnector,
    CheckpointOutput,
    GenerateSlimDocumentOutput,
    IndexingHeartbeatInterface,
    SecondsSinceUnixEpoch,
    SlimConnector,
)
from onyx.connectors.models import (
    ConnectorCheckpoint,
    ConnectorFailure,
    ConnectorMissingCredentialError,
    Document,
    DocumentFailure,
    SlimDocument,
    TextSection,
)
from onyx.connectors.monday.client import MondayApiError, MondayClient
from onyx.utils.logger import setup_logger

logger = setup_logger()

_SLIM_BATCH_SIZE = 500
# Used when the API user has no time zone set in monday.com.
_DEFAULT_TIME_ZONE = os.environ.get("MONDAY_TIME_ZONE") or "Europe/Berlin"


class MondayConnectorCheckpoint(ConnectorCheckpoint):
    # Boards still to sync. None until the first call lists them.
    todo_board_ids: list[str] | None = None


def _document_id(item_id: str) -> str:
    return f"monday_{item_id}"


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parser.isoparse(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _zone(name: str | None) -> tzinfo | None:
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def _column_text(column: dict[str, Any], zone: tzinfo) -> str | None:
    """The column's text. A date column with a time comes as UTC in "text";
    the board shows it in local time, so convert it."""
    text = column.get("text")
    if column.get("type") != "date" or not column.get("value"):
        return text
    try:
        value = json.loads(column["value"])
    except (TypeError, ValueError):
        return text
    if not value.get("date") or not value.get("time"):
        return text
    try:
        moment = datetime.fromisoformat(f"{value['date']}T{value['time']}")
    except ValueError:
        return text
    return (
        moment.replace(tzinfo=timezone.utc).astimezone(zone).strftime("%Y-%m-%d %H:%M")
    )


def _last_activity(item: dict[str, Any]) -> datetime | None:
    """Comments do not change the item's updated_at, so use the newest of both."""
    times = [_parse_time(item.get("updated_at"))]
    times += [
        _parse_time(update.get("created_at")) for update in item.get("updates") or []
    ]
    known = [moment for moment in times if moment is not None]
    return max(known) if known else None


class MondayConnector(CheckpointedConnector[MondayConnectorCheckpoint], SlimConnector):
    def __init__(self) -> None:
        self._client: MondayClient | None = None
        self._time_zone: tzinfo | None = None

    @property
    def client(self) -> MondayClient:
        if self._client is None:
            raise ConnectorMissingCredentialError("monday.com")
        return self._client

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        token = credentials.get("monday_api_token")
        if not token:
            raise ConnectorMissingCredentialError("monday.com")
        self._client = MondayClient(str(token))
        return None

    def validate_connector_settings(self) -> None:
        try:
            self.client.execute("query { me { id } }")
        except PermissionError as e:
            raise CredentialExpiredError(str(e)) from e
        except MondayApiError as e:
            raise ConnectorValidationError(str(e)) from e

    def build_dummy_checkpoint(self) -> MondayConnectorCheckpoint:
        return MondayConnectorCheckpoint(has_more=True)

    def validate_checkpoint_json(
        self, checkpoint_json: str
    ) -> MondayConnectorCheckpoint:
        return MondayConnectorCheckpoint.model_validate_json(checkpoint_json)

    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: MondayConnectorCheckpoint,
    ) -> CheckpointOutput[MondayConnectorCheckpoint]:
        checkpoint = checkpoint.model_copy(deep=True)
        if checkpoint.todo_board_ids is None:
            checkpoint.todo_board_ids = [
                str(board["id"]) for board in self.client.get_boards()
            ]
            checkpoint.has_more = bool(checkpoint.todo_board_ids)
            return checkpoint

        if not checkpoint.todo_board_ids:
            checkpoint.has_more = False
            return checkpoint

        # One board per call, so a failed run resumes at the next board.
        board_id = checkpoint.todo_board_ids.pop()
        board = self._get_board(board_id)
        if board is not None:
            for item in self.client.get_items(board_id):
                updated_at = _last_activity(item)
                if updated_at and not (start <= updated_at.timestamp() <= end):
                    continue
                try:
                    yield self._item_to_document(board, item, updated_at)
                except Exception as e:
                    logger.exception(
                        "Failed to convert monday.com item %s", item.get("id")
                    )
                    yield ConnectorFailure(
                        failed_document=DocumentFailure(
                            document_id=_document_id(str(item.get("id"))),
                            document_link=item.get("url") or None,
                        ),
                        failure_message=str(e),
                        exception=e,
                    )

        checkpoint.has_more = bool(checkpoint.todo_board_ids)
        return checkpoint

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        batch: list[SlimDocument] = []
        for board in self.client.get_boards():
            for item in self.client.get_items(str(board["id"]), slim=True):
                batch.append(SlimDocument(id=_document_id(str(item["id"]))))
                if len(batch) >= _SLIM_BATCH_SIZE:
                    yield batch
                    batch = []
            if callback:
                if callback.should_stop():
                    raise RuntimeError(
                        "monday.com slim retrieval: stop signal detected"
                    )
                callback.progress("monday_slim_retrieval", 1)
        if batch:
            yield batch

    @property
    def time_zone(self) -> tzinfo:
        """The API user's time zone, else MONDAY_TIME_ZONE / Europe/Berlin."""
        if self._time_zone is None:
            name = None
            try:
                me = self.client.execute("query { me { time_zone_identifier } }")
                name = (me.get("me") or {}).get("time_zone_identifier")
            except MondayApiError as e:
                logger.warning("Could not read the monday.com time zone: %s", e)
            self._time_zone = _zone(name) or _zone(_DEFAULT_TIME_ZONE) or timezone.utc
        return self._time_zone

    def _get_board(self, board_id: str) -> dict[str, Any] | None:
        boards = (
            self.client.execute(
                "query($ids: [ID!]) { boards(ids: $ids) { id name url } }",
                {"ids": [board_id]},
            ).get("boards")
            or []
        )
        return boards[0] if boards else None

    def _item_to_document(
        self, board: dict[str, Any], item: dict[str, Any], updated_at: datetime | None
    ) -> Document:
        zone = self.time_zone
        item_id = str(item["id"])
        item_name = item.get("name") or f"Item {item_id}"
        board_name = board.get("name") or "Board"
        group = (item.get("group") or {}).get("title")

        lines = [f"Board: {board_name}"]
        if group:
            lines.append(f"Group: {group}")
        lines.append(f"Item: {item_name}")
        for column in item.get("column_values") or []:
            text = _column_text(column or {}, zone)
            title = ((column or {}).get("column") or {}).get("title")
            if text:
                lines.append(f"{title}: {text}" if title else text)

        updates = [
            update for update in item.get("updates") or [] if update.get("text_body")
        ]
        if updates:
            lines.append("")
            lines.append("Updates:")
            # The API returns the newest first; read them in order.
            for update in reversed(updates):
                author = (update.get("creator") or {}).get("name") or "Unknown"
                created = _parse_time(update.get("created_at"))
                when = (
                    created.astimezone(zone).strftime("%Y-%m-%d %H:%M")
                    if created
                    else ""
                )
                lines.append(f"{author} ({when}): {update['text_body']}")

        metadata: dict[str, str | list[str]] = {"board": board_name}
        if group:
            metadata["group"] = group

        return Document(
            id=_document_id(item_id),
            sections=[TextSection(text="\n".join(lines), link=item.get("url") or None)],
            source=DocumentSource.MONDAY,
            semantic_identifier=item_name,
            # Onyx puts the title in front of every chunk.
            title=f"{board_name}: {item_name}",
            metadata=metadata,
            doc_updated_at=updated_at,
        )
