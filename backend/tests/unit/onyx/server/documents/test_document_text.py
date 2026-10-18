from types import SimpleNamespace
from typing import cast

from onyx.context.search.models import InferenceChunk
from onyx.server.documents.document_text import render_document_text


def _chunk(chunk_id: int, content: str) -> InferenceChunk:
    return cast(
        InferenceChunk,
        SimpleNamespace(
            chunk_id=chunk_id,
            content=content,
            title="Grüße aus München",
            semantic_identifier="Grüße aus München",
            metadata={
                "mailbox": "INBOX",
                "from": "Erika <erika@example.com>",
                "to": ["max@example.com", "team@example.com"],
                "date": "2026-10-16T09:30:00+02:00",
                "x-custom": "1",
            },
        ),
    )


def test_header_comes_first_and_chunks_are_in_order() -> None:
    text = render_document_text([_chunk(1, "zweiter Teil"), _chunk(0, "Hallo Max,\n")])

    lines = text.splitlines()
    assert lines[:6] == [
        "Grüße aus München",
        "from: Erika <erika@example.com>",
        "to: max@example.com, team@example.com",
        "date: 2026-10-16T09:30:00+02:00",
        "mailbox: INBOX",
        "x-custom: 1",
    ]
    assert text.index("Hallo Max,") < text.index("zweiter Teil")
