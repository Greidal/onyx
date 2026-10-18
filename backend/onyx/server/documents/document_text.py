from onyx.context.search.models import InferenceChunk

# Shown first, in this order, when a document has them (e.g. emails).
_HEADER_KEYS = (
    "from",
    "to",
    "cc",
    "date",
    "mailbox",
    "board",
    "group",
    "folder",
    "owner",
)


def render_document_text(chunks: list[InferenceChunk]) -> str:
    """Plain-text view of an indexed document: a header from the title and
    metadata, then the chunks in their original order."""
    ordered = sorted(chunks, key=lambda chunk: chunk.chunk_id)
    first = ordered[0]
    header = [first.title or first.semantic_identifier]
    metadata = first.metadata
    keys = [key for key in _HEADER_KEYS if key in metadata] + sorted(
        key for key in metadata if key not in _HEADER_KEYS
    )
    for key in keys:
        value = metadata[key]
        header.append(
            f"{key}: {', '.join(value) if isinstance(value, list) else value}"
        )
    body = "\n".join(chunk.content.strip("\n") for chunk in ordered)
    return "\n".join(header) + "\n" + "-" * 40 + "\n\n" + body + "\n"
