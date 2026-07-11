from typing import Any, Iterator

from pydantic import BaseModel
from dateutil import parser

from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import CheckpointedConnector
from onyx.connectors.interfaces import CheckpointOutput
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.interfaces import SlimConnector
from onyx.connectors.interfaces import GenerateSlimDocumentOutput
from onyx.connectors.interfaces import IndexingHeartbeatInterface
from onyx.connectors.models import ConnectorFailure
from onyx.connectors.models import Document
from onyx.connectors.models import DocumentFailure
from onyx.connectors.models import TextSection
from onyx.connectors.models import SlimDocument
from onyx.connectors.monday.client import MondayClient
from onyx.utils.logger import setup_logger

logger = setup_logger()


class MondayConnectorCheckpoint(BaseModel):
    # To keep things simple for V1, we'll just track the last processed board ID
    # or keep no state and fetch everything. For now, we'll fetch everything.
    has_more: bool = True


class MondayCredentialsNotSetUpError(PermissionError):
    def __init__(self) -> None:
        super().__init__("Monday Credentials are not set up, was load_credentials called?")


class MondayConnector(CheckpointedConnector[MondayConnectorCheckpoint], SlimConnector):
    def __init__(self) -> None:
        self.client: MondayClient | None = None

    def validate_checkpoint_json(self, checkpoint_json: str) -> MondayConnectorCheckpoint:
        return MondayConnectorCheckpoint.model_validate_json(checkpoint_json)

    def build_dummy_checkpoint(self) -> MondayConnectorCheckpoint:
        return MondayConnectorCheckpoint()

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        token = credentials.get("monday_api_token")
        if not token:
            raise ValueError("monday_api_token is required")
            
        self.client = MondayClient(token)
        return None

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        if self.client is None:
            raise MondayCredentialsNotSetUpError()
            
        slim_docs = []
        for board in self.client.get_boards():
            for item in self.client.get_slim_items_for_board(board["id"]):
                doc_updated_at = None
                if item.get("updated_at"):
                    try:
                        doc_updated_at = parser.isoparse(item["updated_at"])
                    except Exception:
                        pass
                slim_docs.append(
                    SlimDocument(
                        id=f"monday_{item['id']}",
                        doc_created_at=doc_updated_at
                    )
                )
                if len(slim_docs) >= 100:
                    yield slim_docs
                    slim_docs = []
                    if callback:
                        callback.heartbeat()
                        
        if slim_docs:
            yield slim_docs

    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: MondayConnectorCheckpoint,
    ) -> CheckpointOutput[MondayConnectorCheckpoint]:
        if self.client is None:
            raise MondayCredentialsNotSetUpError()

        logger.info("Starting Monday connector sync...")
        
        try:
            for board in self.client.get_boards():
                board_id = board["id"]
                board_name = board.get("name", f"Board {board_id}")
                
                logger.info(f"Syncing board: {board_name} (ID: {board_id})")
                
                try:
                    for item in self.client.get_items_for_board(board_id):
                        item_id = item["id"]
                        
                        # Filter by updated_at if we can
                        doc_updated_at = None
                        if item.get("updated_at"):
                            try:
                                doc_updated_at = parser.isoparse(item["updated_at"])
                                if start and doc_updated_at.timestamp() < start:
                                    continue
                                if end and doc_updated_at.timestamp() > end:
                                    continue
                            except Exception:
                                pass

                        try:
                            # updates are now fetched directly on the item
                            updates = item.get("updates") or []
                            document = self._item_to_document(board, item, updates)
                            if document:
                                document.doc_updated_at = doc_updated_at
                                yield document
                                
                        except Exception as e:
                            logger.error(f"Error processing item {item_id}: {e}")
                            yield ConnectorFailure(
                                failed_document=DocumentFailure(
                                    document_id=f"monday_item_{item_id}",
                                    document_link=item.get("url", ""),
                                ),
                                failure_message=str(e),
                            )
                except Exception as e:
                    logger.error(f"Error processing board {board_id}: {e}")
                    # Continue with other boards
                    
        except Exception as e:
            logger.error(f"Error fetching Monday boards: {e}")
            raise

        return MondayConnectorCheckpoint(has_more=False)

    def _item_to_document(self, board: dict, item: dict, updates: list) -> Document | None:
        item_id = item["id"]
        item_name = item.get("name", f"Item {item_id}")
        board_name = board.get("name", "Unknown Board")
        
        # Build text content from columns and updates
        content_parts = [f"Board: {board_name}", f"Item: {item_name}"]
        
        column_values = item.get("column_values") or []
        for col in column_values:
            if not col:
                continue
            col_text = col.get("text")
            if col_text:
                content_parts.append(f"{col_text}")
                
        if updates:
            content_parts.append("\nUpdates (Comments):")
            for update in updates:
                if not update:
                    continue
                creator = update.get("creator") or {}
                creator_name = creator.get("name", "Unknown User")
                text_body = update.get("text_body", "")
                if text_body:
                    content_parts.append(f"{creator_name}: {text_body}")
                    
        full_text = "\n".join(content_parts)
        
        doc_id = f"monday_{item_id}"

        return Document(
            id=doc_id,
            sections=[TextSection(text=full_text, link=item.get("url", ""))],
            source=DocumentSource.MONDAY,
            semantic_identifier=f"{board_name} - {item_name}",
            metadata={},
            doc_updated_at=None, # Populated by caller
            primary_owners=None,
            secondary_owners=None,
        )
