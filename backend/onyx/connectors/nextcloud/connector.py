"""
Nextcloud connector for Onyx using WebDAV API.
"""

import os
from datetime import datetime
from datetime import timezone
from typing import Any

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.interfaces import PollConnector
from onyx.connectors.interfaces import SecondsSinceUnixEpoch
from onyx.connectors.models import ConnectorMissingCredentialError
from onyx.connectors.models import Document
from onyx.connectors.models import TextSection
from onyx.connectors.nextcloud.client import NextcloudWebDAVClient
from onyx.utils.logger import setup_logger

logger = setup_logger()

# File extension filter for acceptable file types
ACCEPTED_FILE_EXTENSIONS = {
    ".txt",
    ".md",
    ".pdf",
    ".doc",
    ".docx",
    ".ppt",
    ".pptx",
    ".xls",
    ".xlsx",
    ".csv",
    ".rtf",
    ".html",
    ".htm",
    ".xml",
    ".json",
    ".py",
    ".js",
    ".css",
    ".java",
    ".cpp",
    ".c",
    ".h",
}


def _extract_file_text(file_content: bytes, file_path: str) -> str:
    """Simple text extraction for common file types."""
    try:
        # For now, just handle plain text files
        if file_path.lower().endswith(
            (
                ".txt",
                ".md",
                ".py",
                ".js",
                ".css",
                ".html",
                ".htm",
                ".xml",
                ".json",
            )
        ):
            return file_content.decode("utf-8", errors="ignore")
        else:
            # For other files, return basic info
            return f"File: {file_path}\nSize: {len(file_content)} bytes"
    except Exception:
        return f"Could not extract text from {file_path}"


def _is_accepted_file_ext(
    file_path: str,
    accepted_extensions: set[str] | None = None,
) -> bool:
    """Check if file extension is supported."""
    if accepted_extensions is None:
        accepted_extensions = ACCEPTED_FILE_EXTENSIONS

    ext = file_path.lower().split(".")[-1] if "." in file_path else ""
    return f".{ext}" in accepted_extensions


class NextcloudConnector(LoadConnector, PollConnector):
    """Connector for indexing files from Nextcloud instances via WebDAV."""

    def __init__(
        self,
        path_filter: str | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
        file_extensions: list[str] | None = None,
    ) -> None:
        """Initialize the Nextcloud connector.

        Args:
            path_filter: Optional path to limit indexing scope
            batch_size: Number of documents to process in each batch
            file_extensions: Optional list of file extensions to include
        """
        self.server_url: str | None = None
        self.username: str | None = None
        self.password: str | None = None
        self.verify_ssl: bool = True
        self.path_filter = path_filter or ""
        self.batch_size = batch_size
        self.file_extensions = (
            set(file_extensions) if file_extensions else ACCEPTED_FILE_EXTENSIONS
        )

        self._client: NextcloudWebDAVClient | None = None

    def load_credentials(
        self, credentials: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Load credentials for accessing Nextcloud.

        Args:
            credentials: Dictionary containing authentication credentials

        Returns:
            None
        """
        self.server_url = credentials["nextcloud_server_url"]
        self.username = credentials["nextcloud_username"]
        self.password = credentials["nextcloud_password"]

        # Optional configuration from connector_specific_config
        self.verify_ssl = credentials.get("verify_ssl", True)

        # Handle file extensions
        file_extensions = credentials.get("file_extensions", [])
        if file_extensions:
            # Ensure extensions start with a dot
            self.file_extensions = set(
                ext if ext.startswith(".") else f".{ext}" for ext in file_extensions
            )
        else:
            self.file_extensions = ACCEPTED_FILE_EXTENSIONS

        # Reset client to force recreation with new credentials
        self._client = None

        return None

    @property
    def client(self) -> NextcloudWebDAVClient:
        """Get or create WebDAV client instance."""
        if self._client is None:
            if not all([self.server_url, self.username, self.password]):
                raise ConnectorMissingCredentialError("Nextcloud")

            assert self.server_url is not None
            assert self.username is not None
            assert self.password is not None

            self._client = NextcloudWebDAVClient(
                server_url=self.server_url,
                username=self.username,
                password=self.password,
                verify_ssl=self.verify_ssl,
            )

        return self._client

    def load_from_state(self) -> GenerateDocumentsOutput:
        """Load all accessible files from Nextcloud.

        Yields:
            Batches of Document objects
        """
        yield from self._get_all_documents()

    def poll_source(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
    ) -> GenerateDocumentsOutput:
        """Load files modified within the specified time range.

        Args:
            start: Start timestamp
            end: End timestamp

        Yields:
            Batches of Document objects
        """
        start_datetime = datetime.fromtimestamp(start, tz=timezone.utc)

        yield from self._get_all_documents(modified_since=start_datetime)

    def _get_all_documents(
        self,
        modified_since: datetime | None = None,
    ) -> GenerateDocumentsOutput:
        """Get all documents, optionally filtered by modification date.

        Args:
            modified_since: Only include files modified after this date

        Yields:
            Batches of Document objects
        """
        try:
            # Get list of all files
            logger.info(
                f"Listing files from path: '{self.path_filter}' "
                f"with WebDAV URL: {self.client.webdav_url}"
            )
            files = self.client.list_files(
                path=self.path_filter,
                depth="infinity",
                modified_since=modified_since,
            )

            # Filter to only include regular files (not directories)
            regular_files = [
                f for f in files if not f.get("is_directory", False)
            ]
            logger.info(
                f"Found {len(regular_files)} regular files "
                f"(excluding directories)"
            )

            # Process files in batches
            doc_batch: list[Document] = []

            for file_info in regular_files:
                try:
                    # Check if file type is supported
                    if not self._is_supported_file(file_info):
                        continue

                    # Create document from file
                    document = self._create_document_from_file(file_info)
                    if document:
                        doc_batch.append(document)

                        # Yield batch when it reaches the target size
                        if len(doc_batch) >= self.batch_size:
                            logger.info(
                                f"Yielding batch of {len(doc_batch)} documents"
                            )
                            yield doc_batch
                            doc_batch = []

                except Exception as e:
                    logger.error(
                        f"Error processing file "
                        f"{file_info.get('path', 'unknown')}: {e}"
                    )
                    continue

            # Yield any remaining documents
            if doc_batch:
                logger.info(
                    f"Yielding final batch of {len(doc_batch)} documents"
                )
                yield doc_batch

        except Exception as e:
            logger.error(f"Error getting documents from Nextcloud: {e}")
            raise

    def _create_document_from_file(
        self, file_info: dict[str, Any]
    ) -> Document | None:
        """Create a Document object from Nextcloud file information.

        Args:
            file_info: File information dictionary from WebDAV response

        Returns:
            Document object or None if creation fails
        """
        try:
            file_path = file_info.get("path", "")
            file_name = file_info.get("name", file_path.split("/")[-1])

            # Get file content
            try:
                file_content = self.client.get_file_content(file_path)

                # Extract text content from file
                extracted_text = _extract_file_text(
                    file_content=file_content,
                    file_path=file_path,
                )

                if not extracted_text:
                    return None

            except Exception as e:
                logger.warning(
                    f"Failed to extract content from {file_path}: {e}"
                )
                return None

            # Create document sections
            sections = [
                TextSection(
                    link=self._build_file_url(file_path),
                    text=extracted_text,
                )
            ]

            # Build metadata
            metadata = self._build_metadata(file_info)

            # Create document
            document = Document(
                id=f"nextcloud_{file_info.get('file_id', file_path)}",
                sections=sections,
                source=DocumentSource.NEXTCLOUD,
                semantic_identifier=file_name,
                doc_updated_at=file_info.get("last_modified"),
                metadata=metadata,
            )

            return document

        except Exception as e:
            logger.error(
                f"Error creating document from file "
                f"{file_info.get('path', 'unknown')}: {e}"
            )
            return None

    def _build_file_url(self, file_path: str) -> str:
        """Build a URL to view the file in Nextcloud web interface.

        Args:
            file_path: Path to the file

        Returns:
            Web URL for the file
        """
        # Ensure the path starts with a slash
        if not file_path.startswith("/"):
            file_path = "/" + file_path

        # Use the direct file access URL format for Nextcloud
        return (
            f"{self.server_url}/index.php/apps/files/"
            f"?dir={os.path.dirname(file_path)}"
            f"&openfile={os.path.basename(file_path)}"
        )

    def _build_metadata(self, file_info: dict[str, Any]) -> dict[str, str]:
        """Build metadata dictionary from file information.

        Args:
            file_info: File information from WebDAV

        Returns:
            Metadata dictionary
        """
        metadata: dict[str, str] = {}

        if "content_type" in file_info:
            metadata["content_type"] = str(file_info["content_type"])

        if "size" in file_info:
            metadata["file_size"] = str(file_info["size"])

        if "owner" in file_info:
            metadata["owner"] = str(file_info["owner"])

        if "permissions" in file_info:
            metadata["permissions"] = str(file_info["permissions"])

        if "etag" in file_info:
            metadata["etag"] = str(file_info["etag"])

        metadata["file_path"] = str(file_info.get("path", ""))
        metadata["server_url"] = str(self.server_url)

        return metadata

    def _is_supported_file(self, file_info: dict[str, Any]) -> bool:
        """Check if the file type is supported for indexing.

        Args:
            file_info: File information dictionary

        Returns:
            True if file should be indexed, False otherwise
        """
        file_path = file_info.get("path", "")

        # Check file extension using instance-specific extensions
        if not _is_accepted_file_ext(file_path, self.file_extensions):
            return False

        # Skip very large files (over 50MB by default)
        file_size = file_info.get("size", 0)
        max_file_size = 50 * 1024 * 1024  # 50MB
        if file_size > max_file_size:
            logger.debug(
                f"Skipping large file {file_path} "
                f"({file_size} bytes > {max_file_size})"
            )
            return False

        return True

    def validate_connector_settings(self) -> None:
        """Validate that the connector configuration is correct."""
        if not all([self.server_url, self.username, self.password]):
            raise ConnectorMissingCredentialError(
                "Nextcloud connector requires server_url, username, and password"
            )

        assert self.server_url is not None

        # Validate server URL format
        if not (
            self.server_url.startswith("http://")
            or self.server_url.startswith("https://")
        ):
            raise ValueError("Server URL must start with http:// or https://")

        # Test connection
        try:
            if not self.client.test_connection():
                raise ConnectionError(
                    f"Failed to connect to Nextcloud server at "
                    f"{self.server_url}. "
                    "Please check your credentials and server URL."
                )
            logger.info(
                f"Successfully connected to Nextcloud at {self.server_url}"
            )
        except Exception as e:
            raise ConnectionError(
                f"Connection test failed: {e}. Please verify your Nextcloud "
                "server URL, username, and password/app token."
            )
