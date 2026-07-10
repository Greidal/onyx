from typing import Any, Iterator

from pydantic import BaseModel

from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import CheckpointedConnector
from onyx.connectors.interfaces import CheckpointOutput
from onyx.connectors.interfaces import CredentialsConnector
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import ConnectorFailure
from onyx.connectors.models import Document
from onyx.connectors.models import DocumentFailure
from onyx.connectors.models import TextSection
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


class MondayConnector(CheckpointedConnector[MondayConnectorCheckpoint]):
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
                        
                        try:
                            # Also fetch updates (comments)
                            updates = self.client.get_updates_for_item(item_id)
                            
                            document = self._item_to_document(board, item, updates)
                            if document:
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

        return MondayConnectorCheckpoint()

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
                # We can't always get the column name easily from the item's column_values without fetching the board's columns
                # For V1, we'll just include the column's raw text content
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
        updated_at_str = item.get("updated_at")
        doc_updated_at = None
        if updated_at_str:
            # Format: '2024-03-22T10:30:00Z'
            try:
                from dateutil import parser
                doc_updated_at = parser.isoparse(updated_at_str)
            except Exception:
                pass

        return Document(
            id=doc_id,
            sections=[TextSection(text=full_text, link=item.get("url", ""))],
            source=DocumentSource.MONDAY,
            semantic_identifier=f"{board_name} - {item_name}",
            metadata={},
            doc_updated_at=doc_updated_at,
            primary_owners=None,
            secondary_owners=None,
        )
