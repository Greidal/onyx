"""monday.com connector: one document per item, with column values and updates."""

from datetime import datetime
from typing import Any

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


class MondayConnectorCheckpoint(ConnectorCheckpoint):
    # Boards still to sync. None until the first call lists them.
    todo_board_ids: list[str] | None = None


def _document_id(item_id: str) -> str:
    return f"monday_{item_id}"


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return parser.isoparse(value)
    except (TypeError, ValueError):
        return None


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

    def _get_board(self, board_id: str) -> dict[str, Any] | None:
        boards = (
            self.client.execute(
                "query($ids: [ID!]) { boards(ids: $ids) { id name url } }",
                {"ids": [board_id]},
            ).get("boards")
            or []
        )
        return boards[0] if boards else None

    @staticmethod
    def _item_to_document(
        board: dict[str, Any], item: dict[str, Any], updated_at: datetime | None
    ) -> Document:
        item_id = str(item["id"])
        item_name = item.get("name") or f"Item {item_id}"
        board_name = board.get("name") or "Board"
        group = (item.get("group") or {}).get("title")

        lines = [f"Board: {board_name}"]
        if group:
            lines.append(f"Group: {group}")
        lines.append(f"Item: {item_name}")
        for column in item.get("column_values") or []:
            text = (column or {}).get("text")
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
                lines.append(
                    f"{author} ({update.get('created_at', '')[:10]}): {update['text_body']}"
                )

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
