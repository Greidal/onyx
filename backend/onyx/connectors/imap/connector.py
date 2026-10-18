import base64
import copy
import email
import imaplib
import os
import re
import ssl
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from email.message import Message
from email.utils import getaddresses, parseaddr
from enum import Enum
from io import BytesIO
from typing import Any, cast

from pydantic import BaseModel

from onyx.access.models import ExternalAccess
from onyx.configs.app_configs import (
    IMAP_CONNECTOR_ATTACHMENT_CHAR_COUNT_THRESHOLD,
    IMAP_CONNECTOR_ATTACHMENT_SIZE_THRESHOLD,
)
from onyx.configs.constants import DocumentSource
from onyx.connectors.imap.config import DEFAULT_IMAP_PORT_NUMBER
from onyx.connectors.imap.models import EmailHeaders
from onyx.connectors.interfaces import (
    CheckpointedConnectorWithPermSync,
    CheckpointOutput,
    CredentialsConnector,
    CredentialsProviderInterface,
    GenerateSlimDocumentOutput,
    IndexingHeartbeatInterface,
    SecondsSinceUnixEpoch,
    SlimConnector,
)
from onyx.connectors.models import (
    BasicExpertInfo,
    ConnectorCheckpoint,
    Document,
    SlimDocument,
    TextSection,
)
from onyx.file_processing.extract_file_text import extract_file_text
from onyx.file_processing.html_utils import parse_html_page_basic
from onyx.utils.logger import setup_logger

logger = setup_logger()


_IMAP_OKAY_STATUS = "OK"
_PAGE_SIZE = 100
_SLIM_BATCH_SIZE = 500
# IMAP SEARCH compares dates only, in the server's time zone. Widen the window
# by a day on both ends so a poll never misses mail; re-yielded documents are
# unchanged (same id and doc_updated_at) and are skipped by the pipeline.
_SEARCH_DATE_MARGIN = timedelta(days=1)
_USERNAME_KEY = "imap_username"
_PASSWORD_KEY = "imap_password"


class CurrentMailbox(BaseModel):
    mailbox: str
    todo_email_ids: list[str]


# An email has a list of mailboxes.
# Each mailbox has a list of email-ids inside of it.
#
# Usage:
# To use this checkpointer, first fetch all the mailboxes.
# Then, pop a mailbox and fetch all of its email-ids.
# Then, pop each email-id and fetch its content (and parse it, etc..).
# When you have popped all email-ids for this mailbox, pop the next mailbox and repeat the above process until you're done.
#
# For initial checkpointing, set both fields to `None`.
class ImapCheckpoint(ConnectorCheckpoint):
    todo_mailboxes: list[str] | None = None
    current_mailbox: CurrentMailbox | None = None


class LoginState(str, Enum):
    LoggedIn = "logged_in"
    LoggedOut = "logged_out"


class ImapConnector(
    CredentialsConnector,
    CheckpointedConnectorWithPermSync[ImapCheckpoint],
    SlimConnector,
):
    def __init__(
        self,
        host: str,
        port: int = DEFAULT_IMAP_PORT_NUMBER,
        mailboxes: list[str] | None = None,
        index_attachments: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._mailboxes = mailboxes
        self._index_attachments = index_attachments
        self._credentials: dict[str, Any] | None = None

    @property
    def credentials(self) -> dict[str, Any]:
        if not self._credentials:
            raise RuntimeError(
                "Credentials have not been initialized; call `set_credentials_provider` first"
            )
        return self._credentials

    def _get_mail_client(self) -> imaplib.IMAP4_SSL:
        """
        Returns a new `imaplib.IMAP4_SSL` instance.

        The `imaplib.IMAP4_SSL` object is supposed to be an "ephemeral" object; it's not something that you can login,
        logout, then log back into again. I.e., the following will fail:

        ```py
        mail_client.login(..)
        mail_client.logout();
        mail_client.login(..)
        ```

        Therefore, you need a fresh, new instance in order to operate with IMAP. This function gives one to you.

        # Notes
        This function will throw an error if the credentials have not yet been set.
        """

        def get_or_raise(name: str) -> str:
            value = self.credentials.get(name)
            if not value:
                raise RuntimeError(f"Credential item {name=} was not found")
            if not isinstance(value, str):
                raise RuntimeError(
                    f"Credential item {name=} must be of type str, instead received {type(name)=}"
                )
            return value

        username = get_or_raise(_USERNAME_KEY)
        password = get_or_raise(_PASSWORD_KEY)

        # imaplib defaults to an unverified context; pass an explicit one so the
        # certificate and hostname are checked before credentials are sent.
        mail_client = imaplib.IMAP4_SSL(
            host=self._host,
            port=self._port,
            ssl_context=ssl.create_default_context(),
        )
        status, _data = mail_client.login(user=username, password=password)

        if status != _IMAP_OKAY_STATUS:
            raise RuntimeError(f"Failed to log into imap server; {status=}")

        return mail_client

    def _load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: ImapCheckpoint,
        include_perm_sync: bool,
    ) -> CheckpointOutput[ImapCheckpoint]:
        checkpoint = copy.deepcopy(checkpoint)
        checkpoint.has_more = True

        mail_client = self._get_mail_client()
        try:
            if checkpoint.todo_mailboxes is None:
                # This is the dummy checkpoint.
                # Fill it with mailboxes first.
                checkpoint.todo_mailboxes = self._get_mailboxes(mail_client)
                return checkpoint

            if (
                not checkpoint.current_mailbox
                or not checkpoint.current_mailbox.todo_email_ids
            ):
                if not checkpoint.todo_mailboxes:
                    checkpoint.has_more = False
                    return checkpoint

                mailbox = checkpoint.todo_mailboxes.pop()
                email_ids = _fetch_email_uids_in_mailbox(
                    mail_client=mail_client,
                    mailbox=mailbox,
                    start=start,
                    end=end,
                )
                checkpoint.current_mailbox = CurrentMailbox(
                    mailbox=mailbox,
                    todo_email_ids=email_ids,
                )

            mailbox = checkpoint.current_mailbox.mailbox
            _select_mailbox(mail_client=mail_client, mailbox=mailbox)
            current_todos = cast(
                list,
                copy.deepcopy(checkpoint.current_mailbox.todo_email_ids[:_PAGE_SIZE]),
            )
            checkpoint.current_mailbox.todo_email_ids = (
                checkpoint.current_mailbox.todo_email_ids[_PAGE_SIZE:]
            )

            for email_uid in current_todos:
                email_msg = _fetch_email(mail_client=mail_client, email_uid=email_uid)
                if not email_msg:
                    logger.warning(
                        "Failed to fetch message email_uid=%r; skipping", email_uid
                    )
                    continue

                email_headers = EmailHeaders.from_email_msg(email_msg=email_msg)

                yield _convert_email_headers_and_body_into_document(
                    email_msg=email_msg,
                    email_headers=email_headers,
                    mailbox=_unquote_mailbox(mailbox),
                    include_perm_sync=include_perm_sync,
                    index_attachments=self._index_attachments,
                )

            return checkpoint
        finally:
            _logout(mail_client)

    def _get_mailboxes(self, mail_client: imaplib.IMAP4_SSL) -> list[str]:
        if self._mailboxes:
            # Names typed in the UI are Unicode; IMAP wants modified UTF-7.
            return _sanitize_mailbox_names(
                [_encode_mailbox_name(name) for name in self._mailboxes]
            )
        fetched_mailboxes = _fetch_all_mailboxes_for_email_account(
            mail_client=mail_client
        )
        if not fetched_mailboxes:
            raise RuntimeError("Failed to find any mailboxes for this email account")
        return _sanitize_mailbox_names(fetched_mailboxes)

    # impls for SlimConnector (pruning of deleted emails)

    def retrieve_all_slim_docs(
        self,
        start: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        end: SecondsSinceUnixEpoch | None = None,  # noqa: ARG002
        callback: IndexingHeartbeatInterface | None = None,
    ) -> GenerateSlimDocumentOutput:
        mail_client = self._get_mail_client()
        try:
            batch: list[SlimDocument] = []
            for mailbox in self._get_mailboxes(mail_client):
                _select_mailbox(mail_client=mail_client, mailbox=mailbox)
                status, data = mail_client.uid("SEARCH", None, "ALL")
                if status != _IMAP_OKAY_STATUS or not data:
                    raise RuntimeError(f"Failed to list emails in {mailbox=}")
                uids = [uid.decode() for uid in data[0].split()]
                for offset in range(0, len(uids), _SLIM_BATCH_SIZE):
                    for email_msg in _fetch_headers(
                        mail_client, uids[offset : offset + _SLIM_BATCH_SIZE]
                    ):
                        headers = EmailHeaders.from_email_msg(email_msg=email_msg)
                        batch.append(SlimDocument(id=headers.id))
                    if batch:
                        yield batch
                        batch = []
                    if callback:
                        if callback.should_stop():
                            raise RuntimeError(
                                "IMAP slim retrieval: stop signal detected"
                            )
                        callback.progress("imap_slim_retrieval", 1)
        finally:
            _logout(mail_client)

    # impls for BaseConnector

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        raise NotImplementedError("Use `set_credentials_provider` instead")

    def validate_connector_settings(self) -> None:
        self._get_mail_client()

    # impls for CredentialsConnector

    def set_credentials_provider(
        self, credentials_provider: CredentialsProviderInterface
    ) -> None:
        self._credentials = credentials_provider.get_credentials()

    # impls for CheckpointedConnector

    def load_from_checkpoint(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: ImapCheckpoint,
    ) -> CheckpointOutput[ImapCheckpoint]:
        return self._load_from_checkpoint(
            start=start, end=end, checkpoint=checkpoint, include_perm_sync=False
        )

    def build_dummy_checkpoint(self) -> ImapCheckpoint:
        return ImapCheckpoint(has_more=True)

    def validate_checkpoint_json(self, checkpoint_json: str) -> ImapCheckpoint:
        return ImapCheckpoint.model_validate_json(json_data=checkpoint_json)

    # impls for CheckpointedConnectorWithPermSync

    def load_from_checkpoint_with_perm_sync(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
        checkpoint: ImapCheckpoint,
    ) -> CheckpointOutput[ImapCheckpoint]:
        return self._load_from_checkpoint(
            start=start, end=end, checkpoint=checkpoint, include_perm_sync=True
        )


def _fetch_all_mailboxes_for_email_account(mail_client: imaplib.IMAP4_SSL) -> list[str]:
    status, mailboxes_data = mail_client.list(directory="*", pattern="*")
    if status != _IMAP_OKAY_STATUS:
        raise RuntimeError(f"Failed to fetch mailboxes; {status=}")

    mailboxes = []

    for mailboxes_raw in mailboxes_data:
        if isinstance(mailboxes_raw, bytes):
            mailboxes_str = mailboxes_raw.decode()
        elif isinstance(mailboxes_raw, str):
            mailboxes_str = mailboxes_raw
        else:
            logger.warning(
                "Expected the mailbox data to be of type str, instead got type(mailboxes_raw)=%r %s; skipping",
                type(mailboxes_raw),
                mailboxes_raw,
            )
            continue

        # The mailbox LIST response output can be found here:
        # https://www.rfc-editor.org/rfc/rfc3501.html#section-7.2.2
        #
        # The general format is:
        # `(<name-attributes>) <hierarchy-delimiter> <mailbox-name>`
        #
        # The below regex matches on that pattern; from there, we select the 3rd match (index 2), which is the mailbox-name.
        match = re.match(r'\([^)]*\)\s+"([^"]+)"\s+"?(.+?)"?$', mailboxes_str)
        if not match:
            logger.warning(
                "Invalid mailbox-data formatting structure: mailboxes_str=%r; skipping",
                mailboxes_str,
            )
            continue

        mailbox = match.group(2)
        mailboxes.append(mailbox)

    return mailboxes


def _select_mailbox(mail_client: imaplib.IMAP4_SSL, mailbox: str) -> None:
    status, _ids = mail_client.select(mailbox=mailbox, readonly=True)
    if status != _IMAP_OKAY_STATUS:
        raise RuntimeError(f"Failed to select {mailbox=}")


def _logout(mail_client: imaplib.IMAP4_SSL) -> None:
    # Servers limit parallel connections per account; never leave one open.
    try:
        mail_client.logout()
    except (imaplib.IMAP4.error, OSError) as e:
        logger.debug("IMAP logout failed: %s", e)


def _imap_date(value: datetime) -> str:
    return value.strftime("%d-%b-%Y")


def _fetch_email_uids_in_mailbox(
    mail_client: imaplib.IMAP4_SSL,
    mailbox: str,
    start: SecondsSinceUnixEpoch,
    end: SecondsSinceUnixEpoch,
) -> list[str]:
    """UIDs (stable across sessions, unlike sequence numbers) of the emails
    received in [start, end], widened to whole days."""
    _select_mailbox(mail_client=mail_client, mailbox=mailbox)

    epoch = datetime.fromtimestamp(0, tz=timezone.utc)
    since = max(
        datetime.fromtimestamp(start, tz=timezone.utc) - _SEARCH_DATE_MARGIN, epoch
    )
    # BEFORE is exclusive: without the margin, a poll that starts and ends on
    # the same day (every incremental run) would find nothing.
    before = datetime.fromtimestamp(end, tz=timezone.utc) + _SEARCH_DATE_MARGIN
    search_criteria = f'(SINCE "{_imap_date(since)}" BEFORE "{_imap_date(before)}")'

    status, email_ids_byte_array = mail_client.uid("SEARCH", None, search_criteria)

    if status != _IMAP_OKAY_STATUS or not email_ids_byte_array:
        raise RuntimeError(f"Failed to fetch email ids; {status=}")

    email_ids: bytes = email_ids_byte_array[0]

    return [email_id.decode() for email_id in email_ids.split()]


def _fetch_email(mail_client: imaplib.IMAP4_SSL, email_uid: str) -> Message | None:
    # BODY.PEEK leaves the \Seen flag alone (the mailbox is also opened read-only).
    status, msg_data = mail_client.uid("FETCH", email_uid, "(BODY.PEEK[])")
    if status != _IMAP_OKAY_STATUS or not msg_data:
        return None

    data = msg_data[0]
    if not isinstance(data, tuple):
        # An expunged UID returns no message part.
        return None

    _metadata, raw_email = data
    return email.message_from_bytes(raw_email)


def _fetch_headers(
    mail_client: imaplib.IMAP4_SSL, email_uids: list[str]
) -> Iterator[Message]:
    if not email_uids:
        return
    status, msg_data = mail_client.uid(
        "FETCH",
        ",".join(email_uids),
        "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID FROM DATE SUBJECT)])",
    )
    if status != _IMAP_OKAY_STATUS:
        raise RuntimeError(f"Failed to fetch email headers; {status=}")
    for item in msg_data or []:
        if isinstance(item, tuple):
            yield email.message_from_bytes(item[1])


def _convert_email_headers_and_body_into_document(
    email_msg: Message,
    email_headers: EmailHeaders,
    mailbox: str,
    include_perm_sync: bool,
    index_attachments: bool,
) -> Document:
    sender_name, sender_addr = _parse_singular_addr(raw_header=email_headers.sender)
    parsed_recipients = [
        *(_parse_addrs(email_headers.recipients) if email_headers.recipients else []),
        *(_parse_addrs(email_headers.cc) if email_headers.cc else []),
    ]

    expert_info_map = {
        recipient_addr: BasicExpertInfo(
            display_name=recipient_name or None, email=recipient_addr
        )
        for recipient_name, recipient_addr in parsed_recipients
    }
    if sender_addr not in expert_info_map:
        expert_info_map[sender_addr] = BasicExpertInfo(
            display_name=sender_name or None, email=sender_addr
        )

    email_sections = _parse_email_sections(
        email_msg=email_msg,
        email_headers=email_headers,
        index_attachments=index_attachments,
    )
    primary_owners = list(expert_info_map.values())
    external_access = (
        ExternalAccess(
            external_user_emails=set(expert_info_map.keys()),
            external_user_group_ids=set(),
            is_public=False,
        )
        if include_perm_sync
        else None
    )

    # Shown in the UI and passed to the LLM with every chunk.
    metadata: dict[str, str | list[str]] = {
        "from": email_headers.sender,
        "mailbox": mailbox,
    }
    if email_headers.recipients:
        metadata["to"] = email_headers.recipients
    if email_headers.cc:
        metadata["cc"] = email_headers.cc
    if email_headers.date:
        metadata["date"] = email_headers.date.isoformat()

    return Document(
        id=email_headers.id,
        title=email_headers.subject,
        semantic_identifier=email_headers.subject,
        metadata=metadata,
        source=DocumentSource.IMAP,
        sections=email_sections,
        primary_owners=primary_owners,
        external_access=external_access,
        doc_updated_at=email_headers.date,
    )


def _decode_part(part: Message) -> str | None:
    raw_payload = part.get_payload(decode=True)
    if not isinstance(raw_payload, bytes):
        return None
    charset = part.get_content_charset() or "utf-8"
    try:
        return raw_payload.decode(charset, errors="replace")
    except LookupError:
        return raw_payload.decode("utf-8", errors="replace")


def _is_attachment(part: Message) -> bool:
    return part.get_content_disposition() == "attachment" or (
        part.get_filename() is not None
        and not part.get_content_type().startswith("text/")
    )


def _attachment_section(part: Message) -> TextSection | None:
    filename = part.get_filename() or "attachment"
    if part.get_content_maintype() == "image":
        # Inline logos, signatures and photos carry no text.
        return None
    raw_payload = part.get_payload(decode=True)
    if not isinstance(raw_payload, bytes) or not raw_payload:
        return None
    if len(raw_payload) > IMAP_CONNECTOR_ATTACHMENT_SIZE_THRESHOLD:
        logger.warning(
            "Skipping attachment %s due to size. size=%s threshold=%s",
            filename,
            len(raw_payload),
            IMAP_CONNECTOR_ATTACHMENT_SIZE_THRESHOLD,
        )
        return None
    try:
        extracted_text = extract_file_text(
            file=BytesIO(raw_payload),
            file_name=filename,
            break_on_unprocessable=False,
        )
    except Exception as e:
        logger.warning("Could not extract text from attachment %s: %s", filename, e)
        return None
    if not extracted_text.strip():
        return None
    if len(extracted_text) > IMAP_CONNECTOR_ATTACHMENT_CHAR_COUNT_THRESHOLD:
        logger.warning(
            "Skipping attachment %s due to extracted text length. size=%s threshold=%s",
            filename,
            len(extracted_text),
            IMAP_CONNECTOR_ATTACHMENT_CHAR_COUNT_THRESHOLD,
        )
        return None
    return TextSection(text=f"Attachment: {filename}\n\n{extracted_text.strip()}")


def _parse_email_sections(
    email_msg: Message,
    email_headers: EmailHeaders,
    index_attachments: bool,
) -> list[TextSection]:
    plain_body: str | None = None
    html_body: str | None = None
    attachment_sections: list[TextSection] = []

    for part in email_msg.walk():
        if part.is_multipart():
            # Multipart parts are *containers* for other parts, not the actual content itself.
            continue

        if _is_attachment(part):
            if index_attachments and (section := _attachment_section(part)):
                attachment_sections.append(section)
            continue

        content_type = part.get_content_type()
        if content_type == "text/plain" and plain_body is None:
            plain_body = _decode_part(part)
        elif content_type == "text/html" and html_body is None:
            html_body = _decode_part(part)

    # Prefer text/plain: it keeps the line breaks the sender wrote. Plain text
    # must not go through an HTML parser, which drops "<name@example.com>".
    if plain_body and plain_body.strip():
        body = _normalize_plain_text(plain_body)
    elif html_body:
        body = parse_html_page_basic(html_body)
    else:
        body = ""

    if not body:
        logger.warning(
            "Email with email_headers.id=%r has an empty body", email_headers.id
        )

    sections = [TextSection(text=body)] if body else []
    return sections + attachment_sections


def _normalize_plain_text(text: str) -> str:
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").split("\n")]
    # Collapse runs of blank lines.
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def _unquote_mailbox(mailbox: str) -> str:
    """Display name of a quoted, modified-UTF-7 mailbox (e.g. "Entw&APw-rfe")."""
    name = (
        mailbox[1:-1] if mailbox.startswith('"') and mailbox.endswith('"') else mailbox
    )
    return _decode_mailbox_name(name)


def _encode_mailbox_name(name: str) -> str:
    """Modified UTF-7 as defined in RFC 3501, section 5.1.3."""
    if all(0x20 <= ord(char) <= 0x7E for char in name):
        return name.replace("&", "&-")
    result: list[str] = []
    pending: list[str] = []

    def flush() -> None:
        if pending:
            encoded = base64.b64encode("".join(pending).encode("utf-16-be"))
            result.append("&" + encoded.decode().rstrip("=").replace("/", ",") + "-")
            pending.clear()

    for char in name:
        if 0x20 <= ord(char) <= 0x7E:
            flush()
            result.append("&-" if char == "&" else char)
        else:
            pending.append(char)
    flush()
    return "".join(result)


def _decode_mailbox_name(name: str) -> str:
    def replace(match: re.Match[str]) -> str:
        chunk = match.group(1)
        if not chunk:
            return "&"
        padded = chunk.replace(",", "/") + "=" * (-len(chunk) % 4)
        try:
            return base64.b64decode(padded).decode("utf-16-be")
        except (ValueError, UnicodeDecodeError):
            return match.group(0)

    return re.sub(r"&([A-Za-z0-9+,]*)-", replace, name)


def _sanitize_mailbox_names(mailboxes: list[str]) -> list[str]:
    """
    Mailboxes with special characters in them must be enclosed by double-quotes, as per the IMAP protocol.
    Just to be safe, we wrap *all* mailboxes with double-quotes.
    """
    return [f'"{mailbox}"' for mailbox in mailboxes if mailbox]


def _parse_addrs(raw_header: str) -> list[tuple[str, str]]:
    name_addr_pairs = getaddresses([raw_header])
    valid_pairs = [(name, addr) for name, addr in name_addr_pairs if addr]
    if not valid_pairs and raw_header.strip():
        return [(raw_header, raw_header)]
    return valid_pairs


def _parse_singular_addr(raw_header: str) -> tuple[str, str]:
    name, addr = parseaddr(raw_header)
    return name, addr or raw_header


if __name__ == "__main__":
    import time

    from onyx.connectors.credentials_provider import OnyxStaticCredentialsProvider
    from tests.daily.connectors.utils import load_all_from_connector

    host = os.environ.get("IMAP_HOST")
    mailboxes_str = os.environ.get("IMAP_MAILBOXES")
    username = os.environ.get("IMAP_USERNAME")
    password = os.environ.get("IMAP_PASSWORD")

    mailboxes = (
        [mailbox.strip() for mailbox in mailboxes_str.split(",")]
        if mailboxes_str
        else []
    )

    if not host:
        raise RuntimeError("`IMAP_HOST` must be set")

    imap_connector = ImapConnector(
        host=host,
        mailboxes=mailboxes,
    )

    imap_connector.set_credentials_provider(
        OnyxStaticCredentialsProvider(
            tenant_id=None,
            connector_name=DocumentSource.IMAP,
            credential_json={
                _USERNAME_KEY: username,
                _PASSWORD_KEY: password,
            },
        )
    )

    for doc in load_all_from_connector(
        connector=imap_connector,
        start=0,
        end=time.time(),
    ).documents:
        print(doc)
