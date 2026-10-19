from typing import Any
from unittest.mock import MagicMock, patch

from onyx.connectors.models import ConnectorFailure, Document
from onyx.connectors.monday.connector import MondayConnector, MondayConnectorCheckpoint

_ITEM = {
    "id": "101",
    "name": "Standaufbau Halle 3",
    "updated_at": "2026-10-01T08:00:00Z",
    "url": "https://acme.monday.com/boards/1/pulses/101",
    "group": {"title": "Aufbau"},
    "column_values": [
        {"text": "In Arbeit", "column": {"title": "Status"}},
        {"text": "", "column": {"title": "Leer"}},
        {"text": "2026-10-20", "column": {"title": "Termin"}},
        {
            "type": "date",
            "text": "2026-10-19 16:00",
            "value": '{"date":"2026-10-19","icon":"","time":"16:00:00"}',
            "column": {"title": "Zeitpunkt"},
        },
    ],
    "updates": [
        {
            "text_body": "Neu: Strom ist bestellt",
            "created_at": "2026-10-15T12:00:00Z",
            "creator": {"name": "Max"},
        },
        {
            "text_body": "Erster Kommentar",
            "created_at": "2026-10-02T09:00:00Z",
            "creator": {"name": "Erika"},
        },
    ],
}


def _graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    if "me {" in query:
        return {"me": {"time_zone_identifier": None}}
    if "boards(limit" in query:
        return {
            "boards": [{"id": "1", "name": "Messe"}] if variables["page"] == 1 else []
        }
    if "boards(ids: $ids)" in query:
        return {"boards": [{"id": "1", "name": "Messe"}]}
    if "next_items_page" in query:
        broken = {
            "id": "102",
            "name": None,
            "updated_at": "2026-10-16T00:00:00Z",
            "column_values": "x",
        }
        return {"next_items_page": {"cursor": None, "items": [broken]}}
    if "items_page" in query:
        return {"boards": [{"items_page": {"cursor": "c1", "items": [_ITEM]}}]}
    raise AssertionError(query)


def _response(data: Any = None, errors: Any = None, status: int = 200) -> MagicMock:
    response = MagicMock(status_code=status, headers={})
    response.json.return_value = {
        "data": data,
        **({"errors": errors} if errors else {}),
    }
    return response


def _connector() -> MondayConnector:
    connector = MondayConnector()
    connector.load_credentials({"monday_api_token": "token"})
    connector.client.session = MagicMock()
    connector.client.session.post.side_effect = lambda _url, json, **_kwargs: _response(
        _graphql(json["query"], json["variables"])
    )
    return connector


def _run(
    connector: MondayConnector, start: float, end: float
) -> list[Document | ConnectorFailure]:
    checkpoint = connector.build_dummy_checkpoint()
    results: list[Document | ConnectorFailure] = []
    while checkpoint.has_more:
        generator = connector.load_from_checkpoint(start, end, checkpoint)
        while True:
            try:
                results.append(next(generator))
            except StopIteration as stop:
                checkpoint = stop.value
                break
    return results


def test_item_document_has_column_titles_group_and_ordered_updates() -> None:
    results = _run(_connector(), 0, 2_000_000_000)
    document = next(result for result in results if isinstance(result, Document))

    text = document.sections[0].text
    assert "Status: In Arbeit" in text
    assert "Termin: 2026-10-20" in text
    # 16:00 UTC in the API is 18:00 summer time on the board.
    assert "Zeitpunkt: 2026-10-19 18:00" in text
    assert "Max (2026-10-15 14:00): Neu: Strom ist bestellt" in text
    assert "Leer" not in text
    assert text.index("Erster Kommentar") < text.index("Strom ist bestellt")
    assert document.metadata == {"board": "Messe", "group": "Aufbau"}
    assert document.semantic_identifier == "Standaufbau Halle 3"
    assert document.title == "Messe: Standaufbau Halle 3"
    # A new comment counts as activity even though updated_at is older.
    assert document.doc_updated_at is not None
    assert document.doc_updated_at.isoformat() == "2026-10-15T12:00:00+00:00"


def test_broken_item_is_reported_with_the_document_id() -> None:
    failures = [
        r
        for r in _run(_connector(), 0, 2_000_000_000)
        if isinstance(r, ConnectorFailure)
    ]
    assert len(failures) == 1
    assert failures[0].failed_document is not None
    assert failures[0].failed_document.document_id == "monday_102"


def test_poll_window_uses_last_activity() -> None:
    # Window after the comment: the item is skipped.
    results = _run(_connector(), 1_792_500_000, 2_000_000_000)
    assert not [r for r in results if isinstance(r, Document)]


def test_checkpoint_round_trips_as_json() -> None:
    connector = _connector()
    checkpoint = MondayConnectorCheckpoint(has_more=True, todo_board_ids=["1", "2"])
    assert (
        connector.validate_checkpoint_json(checkpoint.model_dump_json()) == checkpoint
    )


def test_slim_docs() -> None:
    ids = {doc.id for batch in _connector().retrieve_all_slim_docs() for doc in batch}
    assert ids == {"monday_101", "monday_102"}


@patch("onyx.connectors.monday.client.time.sleep")
def test_complexity_budget_error_is_retried(sleep: MagicMock) -> None:
    connector = _connector()
    connector.client.session.post.side_effect = [
        _response(
            errors=[
                {
                    "message": "budget",
                    "extensions": {
                        "code": "COMPLEXITY_BUDGET_EXHAUSTED",
                        "retry_in_seconds": 7,
                    },
                }
            ]
        ),
        _response({"me": {"id": "1"}}),
    ]
    assert connector.client.execute("query { me { id } }") == {"me": {"id": "1"}}
    sleep.assert_called_once_with(7)
