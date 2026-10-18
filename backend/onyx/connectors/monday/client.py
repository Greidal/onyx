"""Minimal monday.com GraphQL client with pagination and rate-limit handling."""

import time
from collections.abc import Iterator
from typing import Any

import requests

from onyx.utils.logger import setup_logger

logger = setup_logger()

_API_URL = "https://api.monday.com/v2"
_API_VERSION = "2026-04"
_PAGE_SIZE = 100
_MAX_RETRIES = 5
_DEFAULT_RETRY_SECONDS = 30

_ITEM_FIELDS = """
    id
    name
    updated_at
    url
    group { title }
    column_values { text column { title } }
    updates(limit: 50) { text_body created_at creator { name } }
"""
_SLIM_ITEM_FIELDS = "id"


class MondayApiError(RuntimeError):
    pass


class MondayClient:
    def __init__(self, api_token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": api_token,
                "API-Version": _API_VERSION,
                "Content-Type": "application/json",
            }
        )

    def execute(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Runs a query. Waits and retries on HTTP 429/5xx and on an exhausted
        complexity budget, which monday reports as a GraphQL error."""
        for attempt in range(_MAX_RETRIES + 1):
            response = self.session.post(
                _API_URL,
                json={"query": query, "variables": variables or {}},
                timeout=60,
            )
            retry_in = _retry_after(response)
            if retry_in is not None and attempt < _MAX_RETRIES:
                logger.info("monday.com rate limit; retrying in %ss", retry_in)
                time.sleep(retry_in)
                continue
            if response.status_code == 401:
                raise PermissionError("monday.com rejected the API token")
            response.raise_for_status()
            body = response.json()
            if body.get("errors"):
                raise MondayApiError(f"monday.com API error: {body['errors']}")
            return body["data"]
        raise MondayApiError("monday.com rate limit: retries exhausted")

    def get_boards(self) -> Iterator[dict[str, Any]]:
        query = """
        query($limit: Int!, $page: Int!) {
            boards(limit: $limit, page: $page, state: active) { id name url }
        }
        """
        page = 1
        while True:
            boards = (
                self.execute(query, {"limit": _PAGE_SIZE, "page": page}).get("boards")
                or []
            )
            if not boards:
                return
            yield from boards
            page += 1

    def get_items(self, board_id: str, slim: bool = False) -> Iterator[dict[str, Any]]:
        fields = _SLIM_ITEM_FIELDS if slim else _ITEM_FIELDS
        first_query = f"""
        query($boardId: [ID!], $limit: Int!) {{
            boards(ids: $boardId) {{
                items_page(limit: $limit) {{ cursor items {{ {fields} }} }}
            }}
        }}
        """
        next_query = f"""
        query($cursor: String!, $limit: Int!) {{
            next_items_page(cursor: $cursor, limit: $limit) {{ cursor items {{ {fields} }} }}
        }}
        """
        boards = (
            self.execute(first_query, {"boardId": [board_id], "limit": _PAGE_SIZE}).get(
                "boards"
            )
            or []
        )
        if not boards:
            return
        page = boards[0].get("items_page") or {}
        while True:
            yield from page.get("items") or []
            cursor = page.get("cursor")
            if not cursor:
                return
            page = (
                self.execute(next_query, {"cursor": cursor, "limit": _PAGE_SIZE}).get(
                    "next_items_page"
                )
                or {}
            )


def _retry_after(response: requests.Response) -> int | None:
    if response.status_code == 429 or response.status_code >= 500:
        header = response.headers.get("Retry-After", "")
        return int(header) if header.isdigit() else _DEFAULT_RETRY_SECONDS
    if response.status_code != 200:
        return None
    try:
        errors = response.json().get("errors") or []
    except ValueError:
        return None
    for error in errors:
        extensions = error.get("extensions") or {}
        code = str(extensions.get("code", ""))
        if "COMPLEXITY" in code.upper() or "RATE_LIMIT" in code.upper():
            return int(extensions.get("retry_in_seconds") or _DEFAULT_RETRY_SECONDS)
    return None
