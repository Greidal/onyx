"""Nextcloud connector: indexes the files of one Nextcloud user via WebDAV."""

from collections.abc import Iterator
from datetime import datetime, timezone
from io import BytesIO
from typing import Any
from urllib.parse import urlencode

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import (
    ConnectorValidationError,
    CredentialExpiredError,
    InsufficientPermissionsError,
)
from onyx.connectors.interfaces import (
    GenerateDocumentsOutput,
    GenerateSlimDocumentOutput,
    IndexingHeartbeatInterface,
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
    SlimConnector,
)
from onyx.connectors.models import (
    BasicExpertInfo,
    ConnectorMissingCredentialError,
    Document,
    SlimDocument,
    TextSection,
)
from onyx.connectors.nextcloud.client import (
    NextcloudClientError,
    NextcloudFile,
    NextcloudWebDAVClient,
)
from onyx.file_processing.extract_file_text import extract_file_text, get_file_ext
from onyx.file_processing.file_types import OnyxFileExtensions
from onyx.utils.logger import setup_logger

logger = setup_logger()

MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024
_SLIM_BATCH_SIZE = 500


def _normalize_extensions(extensions: list[str] | None) -> set[str]:
    if not extensions:
        return set(OnyxFileExtensions.TEXT_AND_DOCUMENT_EXTENSIONS)
    return {
        ext if ext.startswith(".") else f".{ext}"
        for ext in (raw.strip().lower() for raw in extensions)
        if ext
    }


class NextcloudConnector(LoadConnector, PollConnector, SlimConnector):
    def __init__(
        self,
        path_filter: str | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
        file_extensions: list[str] | None = None,
    ) -> None:
        self.path_filter = (path_filter or "").strip()
        self.batch_size = batch_size
        self.file_extensions = _normalize_extensions(file_extensions)
        self._client: NextcloudWebDAVClient | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        server_url = credentials.get("nextcloud_server_url")
        username = credentials.get("nextcloud_username")
        password = credentials.get("nextcloud_password")
        if not server_url or not username or not password:
            raise ConnectorMissingCredentialError("Nextcloud")
        if not str(server_url).startswith(("https://", "http://")):
            raise ConnectorValidationError("The Nextcloud URL must start with https://")
        self._client = NextcloudWebDAVClient(
            server_url=str(server_url),
            username=str(username),
            password=str(password),
            verify_ssl=bool(credentials.get("verify_ssl", True)),
        )
        return None

    @property
    def client(self) -> NextcloudWebDAVClient:
        if self._client is None:
            raise ConnectorMissingCredentialError("Nextcloud")
        return self._client

    def validate_connector_settings(self) -> None:
        try:
            self.client.test_connection()
        except NextcloudClientError as e:
            if e.status_code == 401:
                raise CredentialExpiredError(
                    "Nextcloud rejected the credentials. Use an app password."
                ) from e
            if e.status_code == 403:
                raise InsufficientPermissionsError(str(e)) from e
            raise ConnectorValidationError(str(e)) from e

    # LoadConnector / PollConnector

    def load_from_state(self) -> GenerateDocumentsOutput:
        yield from self._yield_documents(modified_since=None)

    def poll_source(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,  # noqa: ARG002
    ) -> GenerateDocumentsOutput:
        yield from self._yield_documents(
            modified_since=datetime.fromtimestamp(start, tz=timezone.utc)
        )

    # SlimConnector (pruning of deleted files)

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        batch: list[SlimDocument] = []
        for file in self._iter_indexable_files(modified_since=None):
            batch.append(SlimDocument(id=self._document_id(file)))
            if len(batch) >= _SLIM_BATCH_SIZE:
                yield batch
                batch = []
                if callback:
                    if callback.should_stop():
                        raise RuntimeError(
                            "Nextcloud slim retrieval: stop signal detected"
                        )
                    callback.progress("nextcloud_slim_retrieval", 1)
        if batch:
            yield batch

    # helpers

    def _iter_indexable_files(
        self, modified_since: datetime | None
    ) -> Iterator[NextcloudFile]:
        for file in self.client.iter_files(
            root=self.path_filter, modified_since=modified_since
        ):
            if get_file_ext(file.name) not in self.file_extensions:
                continue
            if file.size is not None and file.size > MAX_FILE_SIZE_BYTES:
                logger.info("Skipping %s: %s bytes", file.path, file.size)
                continue
            yield file

    def _yield_documents(
        self, modified_since: datetime | None
    ) -> GenerateDocumentsOutput:
        batch: list[Document] = []
        for file in self._iter_indexable_files(modified_since=modified_since):
            document = self._to_document(file)
            if document is None:
                continue
            batch.append(document)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def _document_id(file: NextcloudFile) -> str:
        # The file id survives renames and moves; the path does not.
        return f"nextcloud_{file.file_id or file.path}"

    def _file_url(self, file: NextcloudFile) -> str:
        if file.file_id:
            return f"{self.client.server_url}/f/{file.file_id}"
        folder, _, name = file.path.rpartition("/")
        query = urlencode({"dir": folder or "/", "openfile": name})
        return f"{self.client.server_url}/index.php/apps/files/?{query}"

    def _to_document(self, file: NextcloudFile) -> Document | None:
        try:
            content = self.client.get_file_content(file.path, MAX_FILE_SIZE_BYTES)
        except NextcloudClientError as e:
            logger.warning("Skipping %s: %s", file.path, e)
            return None

        text = extract_file_text(
            file=BytesIO(content),
            file_name=file.name,
            break_on_unprocessable=False,
        ).strip()
        if not text:
            logger.info("Skipping %s: no text could be extracted", file.path)
            return None

        folder = file.path.rpartition("/")[0] or "/"
        metadata: dict[str, str | list[str]] = {"folder": folder}
        if file.owner:
            metadata["owner"] = file.owner

        return Document(
            id=self._document_id(file),
            sections=[TextSection(link=self._file_url(file), text=text)],
            source=DocumentSource.NEXTCLOUD,
            semantic_identifier=file.name,
            title=file.name,
            doc_updated_at=file.last_modified,
            metadata=metadata,
            primary_owners=(
                [BasicExpertInfo(display_name=file.owner)] if file.owner else None
            ),
        )
