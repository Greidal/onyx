from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from onyx.connectors.nextcloud import connector as nextcloud_connector
from onyx.connectors.nextcloud.client import NextcloudWebDAVClient
from onyx.connectors.nextcloud.connector import NextcloudConnector

_SERVER = "https://cloud.example.de"
_USER_ID = "erika"
_OLD = "Mon, 05 Oct 2026 10:00:00 GMT"
_NEW = "Fri, 16 Oct 2026 10:00:00 GMT"

# folder -> [(name, is_dir, last_modified, file_id)]
_TREE: dict[str, list[tuple[str, bool, str, str]]] = {
    "/": [
        ("Messe 2026", True, _NEW, "10"),
        ("Archiv", True, _OLD, "11"),
        ("Notizen.md", False, _OLD, "12"),
    ],
    "/Messe 2026": [
        ("Pläne & Aufbau.pdf", False, _NEW, "20"),
        ("foto.jpg", False, _NEW, "21"),
    ],
    "/Archiv": [("alt.docx", False, _OLD, "30")],
}


def _entry(path: str, is_dir: bool, last_modified: str, file_id: str) -> str:
    href = quote(f"/remote.php/dav/files/{_USER_ID}{path}") + ("/" if is_dir else "")
    resource = "<d:collection/>" if is_dir else ""
    return f"""<d:response><d:href>{href}</d:href><d:propstat><d:prop>
<d:getlastmodified>{last_modified}</d:getlastmodified>
<d:resourcetype>{resource}</d:resourcetype>
<d:getcontentlength>{0 if is_dir else 1000}</d:getcontentlength>
<oc:fileid>{file_id}</oc:fileid><oc:owner-display-name>Erika</oc:owner-display-name>
</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"""


def _multistatus(folder: str) -> bytes:
    own = _entry(folder.rstrip("/") or "", True, _NEW, "1")
    children = [
        _entry(f"{folder.rstrip('/')}/{name}", is_dir, modified, file_id)
        for name, is_dir, modified, file_id in _TREE[folder]
    ]
    body = "".join([own, *children])
    return (
        '<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" '
        f'xmlns:oc="http://owncloud.org/ns">{body}</d:multistatus>'
    ).encode()


def _response(
    status: int = 207, content: bytes = b"", payload: Any = None
) -> MagicMock:
    response = MagicMock(status_code=status, content=content)
    response.json.return_value = payload
    response.iter_content.return_value = [content]
    response.__enter__.return_value = response
    return response


def _fake_session() -> MagicMock:
    session = MagicMock()
    session.headers = {}
    webdav_prefix = f"{_SERVER}/remote.php/dav/files/{_USER_ID}/"

    def request(method: str, url: str, **_kwargs: Any) -> MagicMock:
        assert method == "PROPFIND"
        assert _kwargs["headers"]["Depth"] in ("0", "1")
        folder = "/" + url.removeprefix(webdav_prefix)
        from urllib.parse import unquote

        return _response(content=_multistatus(unquote(folder).rstrip("/") or "/"))

    def get(url: str, **_kwargs: Any) -> MagicMock:
        if url.endswith("/ocs/v2.php/cloud/user"):
            return _response(200, payload={"ocs": {"data": {"id": _USER_ID}}})
        return _response(200, content=b"inhalt")

    session.request.side_effect = request
    session.get.side_effect = get
    return session


def _connector(**kwargs: Any) -> NextcloudConnector:
    connector = NextcloudConnector(**kwargs)
    with patch(
        "onyx.connectors.nextcloud.client.requests.Session",
        return_value=_fake_session(),
    ):
        connector.load_credentials(
            {
                "nextcloud_server_url": _SERVER,
                # Login with email; the WebDAV path needs the user id.
                "nextcloud_username": "erika@example.de",
                "nextcloud_password": "app-password",
            }
        )
    return connector


def _fake_extract(file_name: str, **_kwargs: Any) -> str:
    return f"text of {file_name}"


def test_walks_folders_level_by_level_and_decodes_paths() -> None:
    connector = _connector()
    files = {file.path for file in connector.client.iter_files()}
    assert files == {
        "/Notizen.md",
        "/Messe 2026/Pläne & Aufbau.pdf",
        "/Messe 2026/foto.jpg",
        "/Archiv/alt.docx",
    }
    assert connector.client.webdav_root.endswith("/remote.php/dav/files/erika")


def test_poll_skips_unchanged_folders_and_files() -> None:
    connector = _connector()
    since = datetime(2026, 10, 10, tzinfo=timezone.utc)
    files = {file.path for file in connector.client.iter_files(modified_since=since)}
    assert files == {"/Messe 2026/Pläne & Aufbau.pdf", "/Messe 2026/foto.jpg"}


def test_documents_use_extracted_text_links_and_extension_filter() -> None:
    connector = _connector()
    with patch.object(
        nextcloud_connector, "extract_file_text", side_effect=_fake_extract
    ):
        documents = [doc for batch in connector.load_from_state() for doc in batch]

    by_name = {doc.semantic_identifier: doc for doc in documents}
    # Images are not text documents.
    assert set(by_name) == {"Notizen.md", "Pläne & Aufbau.pdf", "alt.docx"}
    pdf = by_name["Pläne & Aufbau.pdf"]
    assert pdf.id == "nextcloud_20"
    assert pdf.sections[0].text == "text of Pläne & Aufbau.pdf"
    assert pdf.sections[0].link == f"{_SERVER}/f/20"
    assert pdf.metadata == {"folder": "/Messe 2026", "owner": "Erika"}
    assert pdf.doc_updated_at == datetime(2026, 10, 16, 10, tzinfo=timezone.utc)


def test_configured_extensions_are_respected() -> None:
    connector = _connector(file_extensions=["pdf"])
    with patch.object(
        nextcloud_connector, "extract_file_text", side_effect=_fake_extract
    ):
        documents = [doc for batch in connector.load_from_state() for doc in batch]
    assert [doc.semantic_identifier for doc in documents] == ["Pläne & Aufbau.pdf"]


def test_path_filter_limits_the_walk() -> None:
    connector = _connector(path_filter="/Archiv")
    assert [
        file.path for file in connector.client.iter_files(root=connector.path_filter)
    ] == ["/Archiv/alt.docx"]


def test_slim_docs_list_every_indexable_file() -> None:
    connector = _connector()
    ids = {doc.id for batch in connector.retrieve_all_slim_docs() for doc in batch}
    assert ids == {"nextcloud_12", "nextcloud_20", "nextcloud_30"}


def test_client_falls_back_to_login_name_without_ocs_id() -> None:
    with patch("onyx.connectors.nextcloud.client.requests.Session") as session_cls:
        session_cls.return_value.get.return_value = _response(200, payload={"ocs": {}})
        client = NextcloudWebDAVClient(_SERVER, "erika", "pw")
        assert client.user_id == "erika"
