"""BookStack connector: indexes pages, and books, chapters and shelves that have a
description, through the BookStack API. What it sees is what the API user may read."""

import html
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.bookstack.client import (
    BookStackApiClient,
    BookStackClientRequestFailedError,
)
from onyx.connectors.cross_connector_utils.miscellaneous_utils import time_str_to_utc
from onyx.connectors.cross_connector_utils.year_in_name import (
    year_in_name,
    year_in_names,
)
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
    HierarchyNode,
    SlimDocument,
    TextSection,
)
from onyx.file_processing.html_utils import parse_html_page_basic

# BookStack's maximum page size for list endpoints.
_LIST_PAGE_SIZE = 500
# Tag names whose value is taken as the project year ("Jahr: 2026").
_YEAR_TAGS = {"jahr", "projektjahr", "year"}

Item = dict[str, Any]


class BookstackConnector(LoadConnector, PollConnector, SlimConnector):
    def __init__(
        self,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.batch_size = batch_size
        self.bookstack_client: BookStackApiClient | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        self.bookstack_client = BookStackApiClient(
            base_url=credentials["bookstack_base_url"],
            token_id=credentials["bookstack_api_token_id"],
            token_secret=credentials["bookstack_api_token_secret"],
        )
        return None

    @property
    def client(self) -> BookStackApiClient:
        if self.bookstack_client is None:
            raise ConnectorMissingCredentialError("Bookstack")
        return self.bookstack_client

    # LoadConnector / PollConnector

    def load_from_state(self) -> GenerateDocumentsOutput:
        return self.poll_source(None, None)

    def poll_source(
        self, start: SecondsSinceUnixEpoch | None, end: SecondsSinceUnixEpoch | None
    ) -> GenerateDocumentsOutput:
        # Names of all books and chapters, for page titles. Also needed when only
        # pages changed in this poll window.
        books = {b["id"]: b for b in self._list("/books")}
        chapters = {c["id"]: c for c in self._list("/chapters")}

        batch: list[Document | HierarchyNode] = []
        for document in self._iter_documents(books, chapters, start, end):
            batch.append(document)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    # SlimConnector (pruning of deleted or emptied items)

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        ids = [
            *(_shelf_id(s) for s in self._list("/shelves") if _has_description(s)),
            *(_book_id(b) for b in self._list("/books") if _has_description(b)),
            *(_chapter_id(c) for c in self._list("/chapters") if _has_description(c)),
            *(_page_id(p) for p in self._list("/pages") if _is_published(p)),
        ]
        for offset in range(0, len(ids), _LIST_PAGE_SIZE):
            if callback and callback.should_stop():
                raise RuntimeError("BookStack slim retrieval: stop signal detected")
            yield [SlimDocument(id=i) for i in ids[offset : offset + _LIST_PAGE_SIZE]]
            if callback:
                callback.progress("bookstack_slim_retrieval", 1)

    # helpers

    def _list(
        self,
        endpoint: str,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
    ) -> Iterator[Item]:
        params = {"count": str(_LIST_PAGE_SIZE), "sort": "+id"}
        # BookStack reads a date-only "updated_at" filter (YYYY-MM-DD) as
        # "<= YYYY-MM-DD 00:00:00", which drops documents edited later on the
        # boundary day. Full datetimes keep the bounds inclusive.
        if start:
            params["filter[updated_at:gte]"] = _iso(start)
        if end:
            params["filter[updated_at:lte]"] = _iso(end)
        offset = 0
        while True:
            data = self.client.get(endpoint, {**params, "offset": str(offset)}).get(
                "data", []
            )
            yield from data
            if len(data) < _LIST_PAGE_SIZE:
                return
            offset += len(data)
            time.sleep(0.2)

    def _iter_documents(
        self,
        books: dict[int, Item],
        chapters: dict[int, Item],
        start: SecondsSinceUnixEpoch | None,
        end: SecondsSinceUnixEpoch | None,
    ) -> Iterator[Document]:
        for shelf in self._list("/shelves", start, end):
            if _has_description(shelf):
                yield self._container_document(
                    shelf, _shelf_id(shelf), "shelf", f"/shelves/{shelf.get('slug')}"
                )
        for book in self._list("/books", start, end):
            if _has_description(book):
                yield self._container_document(
                    book, _book_id(book), "book", f"/books/{book.get('slug')}"
                )
        for chapter in self._list("/chapters", start, end):
            if _has_description(chapter):
                book = books.get(chapter.get("book_id"), {})
                yield self._container_document(
                    chapter,
                    _chapter_id(chapter),
                    "chapter",
                    f"/books/{chapter.get('book_slug')}/chapter/{chapter.get('slug')}",
                    parents=[str(book.get("name", ""))],
                )
        for page in self._list("/pages", start, end):
            if _is_published(page):
                yield self._page_document(page, books, chapters)
                time.sleep(0.1)

    def _container_document(
        self,
        item: Item,
        doc_id: str,
        kind: str,
        path: str,
        parents: list[str] | None = None,
    ) -> Document:
        name = str(item.get("name", ""))
        names = [p for p in (parents or []) if p] + [name]
        metadata: dict[str, str | list[str]] = {"type": kind}
        if year := year_in_names(names):
            metadata["year"] = year
        return Document(
            id=doc_id,
            sections=[
                TextSection(
                    link=self.client.build_app_url(path),
                    text=f"{name}\n{item.get('description', '')}".strip(),
                )
            ],
            source=DocumentSource.BOOKSTACK,
            semantic_identifier=name,
            title=" / ".join(names),
            doc_updated_at=_time(item.get("updated_at")),
            doc_created_at=_time(item.get("created_at")),
            metadata=metadata,
        )

    def _page_document(
        self, page: Item, books: dict[int, Item], chapters: dict[int, Item]
    ) -> Document:
        detail = self.client.get(f"/pages/{page.get('id')}", {})
        name = str(detail.get("name") or page.get("name", ""))
        book_name = str(books.get(page.get("book_id"), {}).get("name", ""))
        chapter_name = str(chapters.get(page.get("chapter_id"), {}).get("name", ""))
        # "Messetag / Infopunkt / Ablauf": the page name alone often says too little.
        names = [n for n in (book_name, chapter_name, name) if n]

        metadata: dict[str, str | list[str]] = {"type": "page"}
        if book_name:
            metadata["book"] = book_name
        if chapter_name:
            metadata["chapter"] = chapter_name
        tags = detail.get("tags") or []
        if tags:
            metadata["tags"] = [_tag_text(tag) for tag in tags]
        if year := _year_from_tags(tags) or year_in_names(names[:-1]):
            metadata["year"] = year

        editor = (detail.get("updated_by") or {}).get("name") or (
            detail.get("owned_by") or {}
        ).get("name")
        text = parse_html_page_basic(
            "<h1>" + html.escape(name) + "</h1>" + str(detail.get("html") or "")
        )
        slug = detail.get("slug") or page.get("slug")
        link = self.client.build_app_url(f"/books/{page.get('book_slug')}/page/{slug}")
        return Document(
            id=_page_id(page),
            sections=[TextSection(link=link, text=text)],
            source=DocumentSource.BOOKSTACK,
            semantic_identifier=name,
            title=" / ".join(names),
            doc_updated_at=_time(detail.get("updated_at") or page.get("updated_at")),
            doc_created_at=_time(detail.get("created_at") or page.get("created_at")),
            metadata=metadata,
            primary_owners=(
                [BasicExpertInfo(display_name=str(editor))] if editor else None
            ),
        )

    def validate_connector_settings(self) -> None:
        """Checks that the token can make an authenticated request."""
        if not self.bookstack_client:
            raise ConnectorMissingCredentialError(
                "BookStack credentials have not been loaded."
            )
        try:
            self.bookstack_client.get("/books", params={"count": "1", "offset": "0"})
        except BookStackClientRequestFailedError as e:
            if e.status_code == 401:
                raise CredentialExpiredError(
                    "Your BookStack credentials appear to be invalid or expired (HTTP 401)."
                ) from e
            if e.status_code == 403:
                raise InsufficientPermissionsError(
                    "The configured BookStack token does not have sufficient permissions (HTTP 403)."
                ) from e
            raise ConnectorValidationError(
                f"Unexpected BookStack error (status={e.status_code}): {e}"
            ) from e
        except Exception as exc:
            raise ConnectorValidationError(
                f"Unexpected error while validating BookStack connector settings: {exc}"
            ) from exc


# Document ids (unchanged from the upstream connector, so existing indexes stay valid)


def _shelf_id(item: Item) -> str:
    return f"shelf:{item.get('id')}"


def _book_id(item: Item) -> str:
    return f"book__{item.get('id')}"


def _chapter_id(item: Item) -> str:
    return f"chapter__{item.get('id')}"


def _page_id(item: Item) -> str:
    return f"page:{item.get('id')}"


def _has_description(item: Item) -> bool:
    """Books, chapters and shelves without a description would only add their
    name to the index; the page titles already carry it."""
    return bool(str(item.get("description") or "").strip())


def _is_published(page: Item) -> bool:
    return not page.get("draft") and not page.get("template")


def _tag_text(tag: Item) -> str:
    name = str(tag.get("name", "")).strip()
    value = str(tag.get("value") or "").strip()
    return f"{name}: {value}" if value else name


def _year_from_tags(tags: list[Item]) -> str | None:
    for tag in tags:
        if str(tag.get("name", "")).strip().lower() in _YEAR_TAGS:
            if year := year_in_name(str(tag.get("value") or "")):
                return year
    return None


def _time(value: Any) -> datetime | None:
    return time_str_to_utc(str(value)) if value else None


def _iso(seconds: SecondsSinceUnixEpoch) -> str:
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
