import requests
from typing import Any, Dict, Generator, List

from onyx.utils.logger import setup_logger

logger = setup_logger()

class MondayClient:
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = "https://api.monday.com/v2"
        self.headers = {
            "Authorization": self.api_token,
            "API-Version": "2026-04",
            "Content-Type": "application/json",
        }

    def _execute_query(self, query: str, variables: Dict[str, Any] | None = None) -> Dict[str, Any]:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if "errors" in data:
            raise RuntimeError(f"Monday API Error: {data['errors']}")
            
        return data["data"]

    def get_boards(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all boards accessible to the user."""
        query = """
        query($limit: Int!, $page: Int!) {
            boards(limit: $limit, page: $page) {
                id
                name
                description
                workspace_id
                updated_at
                url
            }
        }
        """
        page = 1
        limit = 100
        while True:
            variables = {"limit": limit, "page": page}
            data = self._execute_query(query, variables)
            boards = data.get("boards", [])
            if not boards:
                break
                
            for board in boards:
                yield board
                
            page += 1

    def get_items_for_board(self, board_id: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch all items for a given board using cursor pagination."""
        # Initial query
        query = """
        query($boardId: [ID!], $limit: Int) {
            boards(ids: $boardId) {
                items_page(limit: $limit) {
                    cursor
                    items {
                        id
                        name
                        created_at
                        updated_at
                        url
                        column_values {
                            id
                            text
                            type
                        }
                        updates {
                            id
                            text_body
                            created_at
                            creator {
                                name
                                email
                            }
                        }
                    }
                }
            }
        }
        """
        # Next page query
        next_query = """
        query($cursor: String!) {
            next_items_page(cursor: $cursor, limit: 100) {
                cursor
                items {
                    id
                    name
                    created_at
                    updated_at
                    url
                    column_values {
                        id
                        text
                        type
                    }
                    updates {
                        id
                        text_body
                        created_at
                        creator {
                            name
                            email
                        }
                    }
                }
            }
        }
        """
        
        variables = {"boardId": [board_id], "limit": 100}
        data = self._execute_query(query, variables)
        boards = data.get("boards", [])
        if not boards:
            return
            
        items_page = boards[0].get("items_page", {})
        cursor = items_page.get("cursor")
        items = items_page.get("items", [])
        
        for item in items:
            yield item
            
        while cursor:
            variables = {"cursor": cursor}
            data = self._execute_query(next_query, variables)
            next_page = data.get("next_items_page", {})
            cursor = next_page.get("cursor")
            items = next_page.get("items", [])
            
            for item in items:
                yield item

    def get_slim_items_for_board(self, board_id: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch slim item data (id and updated_at) for deletion syncing."""
        query = """
        query($boardId: [ID!], $limit: Int) {
            boards(ids: $boardId) {
                items_page(limit: $limit) {
                    cursor
                    items {
                        id
                        updated_at
                    }
                }
            }
        }
        """
        next_query = """
        query($cursor: String!) {
            next_items_page(cursor: $cursor, limit: 100) {
                cursor
                items {
                    id
                    updated_at
                }
            }
        }
        """
        
        variables = {"boardId": [board_id], "limit": 100}
        data = self._execute_query(query, variables)
        boards = data.get("boards", [])
        if not boards:
            return
            
        items_page = boards[0].get("items_page", {})
        cursor = items_page.get("cursor")
        items = items_page.get("items", [])
        
        for item in items:
            yield item
            
        while cursor:
            variables = {"cursor": cursor}
            data = self._execute_query(next_query, variables)
            next_page = data.get("next_items_page", {})
            cursor = next_page.get("cursor")
            items = next_page.get("items", [])
            
            for item in items:
                yield item
