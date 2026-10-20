from typing import Any

import pytest

from onyx.connectors.bookstack import connector as bookstack_connector
from onyx.connectors.bookstack.connector import BookstackConnector
from onyx.connectors.cross_connector_utils.year_in_name import year_in_name
from onyx.connectors.models import Document

_T = "2026-10-18T10:00:00.000000Z"

_DATA: dict[str, list[dict[str, Any]]] = {
    "/shelves": [
        {"id": 1, "name": "Verein", "slug": "verein", "description": ""},
        {
            "id": 2,
            "name": "Messe",
            "slug": "messe",
            "description": "Alles zur Messe",
            "updated_at": _T,
        },
    ],
    "/books": [
        {"id": 10, "name": "Messetag 2026", "slug": "messetag-2026", "description": ""},
        {
            "id": 11,
            "name": "Wissen",
            "slug": "wissen",
            "description": "Glossar und Zuständigkeiten",
        },
    ],
    "/chapters": [
        {
            "id": 20,
            "book_id": 10,
            "book_slug": "messetag-2026",
            "name": "Infopunkt",
            "slug": "infopunkt",
            "description": "",
        }
    ],
    "/pages": [
        {
            "id": 30,
            "book_id": 10,
            "chapter_id": 20,
            "book_slug": "messetag-2026",
            "name": "Ablauf",
            "slug": "ablauf",
        },
        {
            "id": 31,
            "book_id": 11,
            "chapter_id": 0,
            "book_slug": "wissen",
            "name": "Glossar",
            "slug": "glossar",
        },
        {"id": 32, "book_id": 11, "name": "Entwurf", "slug": "x", "draft": True},
        {"id": 33, "book_id": 11, "name": "Vorlage", "slug": "y", "template": True},
    ],
}

_DETAILS: dict[str, dict[str, Any]] = {
    "/pages/30": {
        "name": "Ablauf",
        "slug": "ablauf",
        "html": "<p>Ab 7 Uhr Aufbau.</p>",
        "updated_at": _T,
        "updated_by": {"name": "Erika"},
        "tags": [
            {"name": "Ressort", "value": "MAUS"},
            {"name": "wichtig", "value": ""},
        ],
    },
    "/pages/31": {
        "name": "Glossar",
        "slug": "glossar",
        "html": "<p>MKOR: Messekoordination</p>",
        "updated_at": _T,
        "owned_by": {"name": "Max"},
        "tags": [{"name": "Jahr", "value": "2025"}],
    },
}


class _FakeClient:
    def __init__(self, page_size: int | None = None) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self.page_size = page_size

    def get(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        self.calls.append((endpoint, params))
        if endpoint in _DETAILS:
            return _DETAILS[endpoint]
        items = _DATA[endpoint]
        offset = int(params.get("offset", "0"))
        count = self.page_size or int(params["count"])
        return {"data": items[offset : offset + count]}

    def build_app_url(self, path: str) -> str:
        return "https://wiki.example.de" + path


def _connector(page_size: int | None = None) -> BookstackConnector:
    connector = BookstackConnector()
    connector.bookstack_client = _FakeClient(page_size)  # type: ignore[assignment]
    return connector


def _documents(connector: BookstackConnector) -> dict[str, Document]:
    return {
        doc.id: doc
        for batch in connector.load_from_state()
        for doc in batch
        if isinstance(doc, Document)
    }


def test_pages_carry_book_and_chapter_tags_editor_and_year() -> None:
    docs = _documents(_connector())
    ablauf = docs["page:30"]
    assert ablauf.title == "Messetag 2026 / Infopunkt / Ablauf"
    assert ablauf.semantic_identifier == "Ablauf"
    assert ablauf.metadata == {
        "type": "page",
        "book": "Messetag 2026",
        "chapter": "Infopunkt",
        "tags": ["Ressort: MAUS", "wichtig"],
        "year": "2026",
    }
    assert ablauf.primary_owners and ablauf.primary_owners[0].display_name == "Erika"
    assert "Ab 7 Uhr Aufbau." in ablauf.sections[0].text
    assert ablauf.sections[0].link == (
        "https://wiki.example.de/books/messetag-2026/page/ablauf"
    )


def test_year_tag_wins_over_names_and_owner_is_fallback_editor() -> None:
    glossar = _documents(_connector())["page:31"]
    assert glossar.title == "Wissen / Glossar"
    assert glossar.metadata["year"] == "2025"
    assert glossar.primary_owners and glossar.primary_owners[0].display_name == "Max"


def test_drafts_templates_and_empty_containers_are_skipped() -> None:
    assert set(_documents(_connector())) == {
        "shelf:2",
        "book__11",
        "page:30",
        "page:31",
    }


def test_slim_docs_match_indexed_documents_for_pruning() -> None:
    connector = _connector()
    slim = {d.id for batch in connector.retrieve_all_slim_docs() for d in batch}
    assert slim == set(_documents(connector))


def test_list_endpoints_are_paged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bookstack_connector, "_LIST_PAGE_SIZE", 1)
    monkeypatch.setattr(bookstack_connector.time, "sleep", lambda _s: None)
    connector = _connector()
    assert {"page:30", "page:31"} <= set(_documents(connector))
    offsets = [
        p["offset"]
        for e, p in connector.bookstack_client.calls  # type: ignore[union-attr]
        if e == "/pages"
    ]
    assert offsets[:5] == ["0", "1", "2", "3", "4"]


def test_poll_uses_full_datetime_bounds() -> None:
    connector = _connector()
    list(connector.poll_source(1760000000, 1760003600))
    page_calls = [
        p
        for e, p in connector.bookstack_client.calls  # type: ignore[union-attr]
        if e == "/pages"
    ]
    assert page_calls[0]["filter[updated_at:gte]"] == "2025-10-09T08:53:20"
    assert page_calls[0]["filter[updated_at:lte]"] == "2025-10-09T09:53:20"


@pytest.mark.parametrize(
    "name, year",
    [
        ("MKOR_26", "2026"),
        ("Messe 2026", "2026"),
        ("DES25", "2025"),
        ("K002", None),
        ("Wissen", None),
    ],
)
def test_year_in_name(name: str, year: str | None) -> None:
    assert year_in_name(name) == year
