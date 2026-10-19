from email.message import EmailMessage, Message
from typing import IO, Any
from unittest.mock import MagicMock, patch

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.credentials_provider import OnyxStaticCredentialsProvider
from onyx.connectors.imap import connector as imap_connector
from onyx.connectors.imap.connector import (
    _IMAP_OKAY_STATUS,
    ImapConnector,
    _convert_email_headers_and_body_into_document,
    _decode_mailbox_name,
    _encode_mailbox_name,
    _fetch_email_uids_in_mailbox,
)
from onyx.connectors.imap.models import EmailHeaders
from onyx.connectors.models import TextSection


def _message(
    *,
    subject: str = "=?utf-8?q?Gr=C3=BC=C3=9Fe?= aus =?utf-8?q?M=C3=BCnchen?=",
    plain: str
    | None = "Hallo Max,\n\nsiehe Anhang.\n\nMax Muster <max@example.com> schrieb:\n> alt",
    html: str | None = "<p>Hallo <b>Max</b></p>",
    message_id: str | None = "<abc@example.com>",
) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "Erika Muster <erika@example.com>"
    msg["To"] = "Max Muster <max@example.com>, team@example.com"
    msg["Cc"] = "chef@example.com"
    msg["Date"] = "Fri, 16 Oct 2026 09:30:00 +0200"
    if message_id:
        msg["Message-ID"] = message_id
    if plain is not None:
        msg.set_content(plain)
    if html is not None:
        if plain is None:
            msg.set_content(html, subtype="html")
        else:
            msg.add_alternative(html, subtype="html")
    return msg


def _document(msg: Message, index_attachments: bool = True) -> Any:
    return _convert_email_headers_and_body_into_document(
        email_msg=msg,
        email_headers=EmailHeaders.from_email_msg(email_msg=msg),
        mailbox="INBOX",
        include_perm_sync=True,
        index_attachments=index_attachments,
    )


def _texts(doc: Any) -> list[str]:
    return [
        section.text for section in doc.sections if isinstance(section, TextSection)
    ]


def test_subject_with_several_encoded_words_is_decoded_completely() -> None:
    doc = _document(_message())
    assert doc.semantic_identifier == "Grüße aus München"


def test_plain_text_body_is_preferred_and_keeps_addresses_and_lines() -> None:
    body = _texts(_document(_message()))[0]
    assert "Max Muster <max@example.com> schrieb:" in body
    assert "Hallo Max,\n\nsiehe Anhang." in body


def test_html_only_body_is_converted_to_text() -> None:
    body = _texts(_document(_message(plain=None)))[0]
    assert "Hallo" in body and "Max" in body
    assert "<b>" not in body


def test_metadata_date_and_cc_recipients() -> None:
    doc = _document(_message())
    assert doc.metadata["from"] == "Erika Muster <erika@example.com>"
    assert doc.metadata["cc"] == "chef@example.com"
    assert doc.metadata["mailbox"] == "INBOX"
    assert doc.doc_updated_at is not None
    assert doc.doc_updated_at.isoformat() == "2026-10-16T09:30:00+02:00"
    assert doc.external_access is not None
    assert doc.external_access.external_user_emails == {
        "erika@example.com",
        "max@example.com",
        "team@example.com",
        "chef@example.com",
    }


def test_missing_message_id_gives_a_stable_id() -> None:
    first = _document(_message(message_id=None))
    second = _document(_message(message_id=None))
    assert first.id == second.id
    assert first.id.startswith("gen_")


def _fake_extract(file: IO[Any], file_name: str, **_kwargs: Any) -> str:
    return f"text of {file_name}: {file.read().decode()}"


def test_attachments_are_indexed_and_inline_images_skipped() -> None:
    msg = _message()
    msg.add_attachment(
        b"Rechnung 42", maintype="application", subtype="pdf", filename="rechnung.pdf"
    )
    msg.add_attachment(b"\x89PNG", maintype="image", subtype="png", filename="logo.png")

    with patch.object(
        imap_connector, "extract_file_text", side_effect=_fake_extract
    ) as extract:
        texts = _texts(_document(msg))
        assert extract.call_count == 1

    assert len(texts) == 2
    assert texts[1].startswith("Attachment: rechnung.pdf")
    assert "Rechnung 42" in texts[1]

    with patch.object(
        imap_connector, "extract_file_text", side_effect=_fake_extract
    ) as extract:
        assert len(_texts(_document(msg, index_attachments=False))) == 1
        extract.assert_not_called()


def test_same_day_poll_window_is_widened_to_whole_days() -> None:
    client = MagicMock()
    client.select.return_value = (_IMAP_OKAY_STATUS, [b"1"])
    client.uid.return_value = (_IMAP_OKAY_STATUS, [b"7 8"])
    start = 1_792_137_600  # 2026-10-16 08:00 UTC
    end = start + 30 * 60

    uids = _fetch_email_uids_in_mailbox(client, '"INBOX"', start, end)

    assert uids == ["7", "8"]
    client.uid.assert_called_once_with(
        "SEARCH", None, '(SINCE "15-Oct-2026" BEFORE "17-Oct-2026")'
    )


@pytest.mark.parametrize(
    "name,encoded",
    [
        ("INBOX", "INBOX"),
        ("Entwürfe", "Entw&APw-rfe"),
        ("A&B", "A&-B"),
        ("Gelöschte Elemente", "Gel&APY-schte Elemente"),
    ],
)
def test_mailbox_names_use_modified_utf7(name: str, encoded: str) -> None:
    assert _encode_mailbox_name(name) == encoded
    assert _decode_mailbox_name(encoded) == name


@patch("onyx.connectors.imap.connector.imaplib.IMAP4_SSL")
def test_connection_is_logged_out_after_each_checkpoint(
    mock_imap4_ssl: MagicMock,
) -> None:
    client = mock_imap4_ssl.return_value
    client.login.return_value = (_IMAP_OKAY_STATUS, [b""])
    connector = ImapConnector(host="imap.example.com", mailboxes=["INBOX"])
    connector.set_credentials_provider(
        OnyxStaticCredentialsProvider(
            tenant_id=None,
            connector_name=DocumentSource.IMAP,
            credential_json={"imap_username": "u", "imap_password": "p"},
        )
    )

    generator = connector.load_from_checkpoint(0, 1, connector.build_dummy_checkpoint())
    with pytest.raises(StopIteration):
        next(generator)

    client.logout.assert_called_once()


def test_fetch_uses_peek_and_uids() -> None:
    client = MagicMock()
    raw = _message().as_bytes()
    client.uid.return_value = (_IMAP_OKAY_STATUS, [(b"1 (UID 7 BODY[] {1}", raw), b")"])

    msg = imap_connector._fetch_email(client, "7")

    assert msg is not None
    client.uid.assert_called_once_with("FETCH", "7", "(BODY.PEEK[])")
    assert msg["Message-ID"] == "<abc@example.com>"


@pytest.mark.parametrize(
    "reply,kept,dropped",
    [
        (
            "Passt, ich bringe die Kabel mit.\n\nAm 16.10.2026 um 09:30 schrieb Erika Muster <erika@example.com>:\n> Wer bringt die Kabel?\n> Gruß Erika",
            "Passt, ich bringe die Kabel mit.",
            "Wer bringt die Kabel?",
        ),
        (
            "Danke!\n\nVon: Erika Muster <erika@example.com>\nGesendet: Freitag, 16. Oktober 2026 09:30\nAn: Max\nBetreff: Kabel\n\nWer bringt die Kabel?",
            "Danke!",
            "Wer bringt die Kabel?",
        ),
        (
            "Sure.\n\nOn Fri, Oct 16, 2026 at 9:30 AM Erika <erika@example.com> wrote:\n> Who brings the cables?",
            "Sure.",
            "Who brings the cables?",
        ),
    ],
)
def test_quoted_history_is_dropped(reply: str, kept: str, dropped: str) -> None:
    body = _texts(_document(_message(plain=reply, html=None)))[0]
    assert kept in body
    assert dropped not in body


def test_mail_from_line_inside_text_is_kept() -> None:
    text = "Von: Montag an gilt der neue Plan.\nBitte beachten."
    body = _texts(_document(_message(plain=text, html=None)))[0]
    assert "Bitte beachten." in body


def test_only_quoted_mail_keeps_its_text() -> None:
    body = _texts(_document(_message(plain="> nur ein Zitat", html=None)))[0]
    assert "nur ein Zitat" in body
