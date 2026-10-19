"""WebDAV client for Nextcloud (files of one user, including shares)."""

from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote, unquote, urlsplit
from xml.etree.ElementTree import Element

import requests
from defusedxml import ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from onyx.utils.logger import setup_logger

logger = setup_logger()

_NS = {
    "d": "DAV:",
    "oc": "http://owncloud.org/ns",
    "nc": "http://nextcloud.org/ns",
}
_PROPFIND_BODY = """<?xml version="1.0" encoding="UTF-8"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop>
    <d:getlastmodified />
    <d:getcontenttype />
    <d:resourcetype />
    <d:getcontentlength />
    <oc:fileid />
    <oc:owner-display-name />
  </d:prop>
</d:propfind>"""
_WEBDAV_FILES_MARKER = "/remote.php/dav/files/"


@dataclass(frozen=True)
class NextcloudFile:
    # Path below the user's root, decoded, starting with "/".
    path: str
    file_id: str | None
    is_directory: bool
    last_modified: datetime | None
    size: int | None
    content_type: str | None
    owner: str | None

    @property
    def name(self) -> str:
        return self.path.rstrip("/").rsplit("/", 1)[-1]


class NextcloudClientError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class NextcloudWebDAVClient:
    def __init__(
        self,
        server_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.username = username
        self._user_id: str | None = None

        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = verify_ssl
        self.session.headers["User-Agent"] = "Onyx-Nextcloud-Connector/2.0"
        retries = Retry(
            total=4,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,  # PROPFIND and GET are both safe to repeat
            respect_retry_after_header=True,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    @property
    def user_id(self) -> str:
        """The WebDAV path uses the user id, which differs from the login name
        when users log in with their email address."""
        if self._user_id is None:
            response = self.session.get(
                f"{self.server_url}/ocs/v2.php/cloud/user",
                params={"format": "json"},
                headers={"OCS-APIRequest": "true"},
                timeout=15,
            )
            self._raise_for_status(response, "Failed to read the Nextcloud user")
            user_id = response.json().get("ocs", {}).get("data", {}).get("id")
            self._user_id = str(user_id) if user_id else self.username
        return self._user_id

    @property
    def webdav_root(self) -> str:
        return f"{self.server_url}{_WEBDAV_FILES_MARKER}{quote(self.user_id)}"

    def _url(self, path: str) -> str:
        return f"{self.webdav_root}/{quote(path.strip('/'))}"

    @staticmethod
    def _raise_for_status(response: requests.Response, message: str) -> None:
        if response.status_code >= 400:
            raise NextcloudClientError(
                f"{message}: HTTP {response.status_code}",
                status_code=response.status_code,
            )

    def test_connection(self) -> None:
        response = self.session.request(
            "PROPFIND",
            self._url(""),
            data=_PROPFIND_BODY,
            headers={"Depth": "0", "Content-Type": "application/xml; charset=utf-8"},
            timeout=15,
        )
        self._raise_for_status(response, "Nextcloud WebDAV is not reachable")

    def _propfind_children(self, path: str) -> list[NextcloudFile]:
        response = self.session.request(
            "PROPFIND",
            self._url(path),
            data=_PROPFIND_BODY,
            # Many servers reject or disable Depth: infinity; walk level by level.
            headers={"Depth": "1", "Content-Type": "application/xml; charset=utf-8"},
            timeout=60,
        )
        self._raise_for_status(response, f"PROPFIND failed for '{path}'")
        # The response also lists the folder itself; iter_files skips it.
        return [
            entry
            for element in ET.fromstring(response.content).findall("d:response", _NS)
            if (entry := self._parse_response(element)) is not None
        ]

    def iter_files(
        self,
        root: str = "",
        modified_since: datetime | None = None,
        skip: Callable[[str], bool] | None = None,
    ) -> Iterator[NextcloudFile]:
        """All files below root. With modified_since, folders that did not
        change are skipped: Nextcloud propagates a change's mtime to every
        parent folder. skip(path) drops a file or a whole folder without
        listing it."""
        queue: deque[str] = deque(["/" + root.strip("/")])
        while queue:
            folder = queue.popleft()
            for entry in self._propfind_children(folder):
                if entry.path.rstrip("/") == folder.rstrip("/"):
                    continue
                if skip is not None and skip(entry.path):
                    continue
                changed = (
                    modified_since is None
                    or entry.last_modified is None
                    or entry.last_modified >= modified_since
                )
                if entry.is_directory:
                    if changed:
                        queue.append(entry.path)
                elif changed:
                    yield entry

    def get_file_content(self, path: str, max_bytes: int) -> bytes:
        with self.session.get(self._url(path), stream=True, timeout=120) as response:
            self._raise_for_status(response, f"Download failed for '{path}'")
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                size += len(chunk)
                if size > max_bytes:
                    raise NextcloudClientError(
                        f"'{path}' is larger than {max_bytes} bytes"
                    )
                chunks.append(chunk)
            return b"".join(chunks)

    def _parse_response(self, element: Element) -> NextcloudFile | None:
        href = element.findtext("d:href", namespaces=_NS)
        prop = element.find("d:propstat[d:status='HTTP/1.1 200 OK']/d:prop", _NS)
        if not href or prop is None:
            return None

        decoded_href = unquote(urlsplit(href).path)
        marker = decoded_href.find(_WEBDAV_FILES_MARKER)
        if marker < 0:
            return None
        # "/remote.php/dav/files/<user id>/<path>"
        rest = decoded_href[marker + len(_WEBDAV_FILES_MARKER) :]
        path = "/" + rest.split("/", 1)[1] if "/" in rest else "/"

        last_modified = None
        if raw_date := prop.findtext("d:getlastmodified", namespaces=_NS):
            try:
                last_modified = parsedate_to_datetime(raw_date).astimezone(timezone.utc)
            except (TypeError, ValueError):
                logger.warning("Could not parse last modified date: %s", raw_date)

        size_text = prop.findtext("d:getcontentlength", namespaces=_NS)
        resource_type = prop.find("d:resourcetype", _NS)
        return NextcloudFile(
            path=path,
            file_id=prop.findtext("oc:fileid", namespaces=_NS) or None,
            is_directory=resource_type is not None
            and resource_type.find("d:collection", _NS) is not None,
            last_modified=last_modified,
            size=int(size_text) if size_text and size_text.isdigit() else None,
            content_type=prop.findtext("d:getcontenttype", namespaces=_NS) or None,
            owner=prop.findtext("oc:owner-display-name", namespaces=_NS) or None,
        )
