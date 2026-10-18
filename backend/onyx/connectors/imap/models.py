import email.header
import email.utils
import hashlib
from datetime import datetime
from email.message import Message
from enum import Enum

from pydantic import BaseModel


class Header(str, Enum):
    SUBJECT_HEADER = "subject"
    FROM_HEADER = "from"
    TO_HEADER = "to"
    CC_HEADER = "cc"
    DELIVERED_TO_HEADER = (
        "Delivered-To"  # Used in mailing lists instead of the "to" header.
    )
    DATE_HEADER = "date"
    MESSAGE_ID_HEADER = "Message-ID"


def decode_header_value(value: str | None) -> str | None:
    """Decodes all RFC 2047 encoded words of a header, not only the first one
    (e.g. "Re: =?utf-8?q?Gr=C3=BC=C3=9Fe?= aus Berlin")."""
    if not value:
        return None
    try:
        decoded = str(email.header.make_header(email.header.decode_header(value)))
    except (LookupError, UnicodeDecodeError, ValueError):
        # Unknown charset or broken encoding: keep the raw header.
        decoded = str(value)
    # Folded headers keep their line breaks.
    return " ".join(decoded.split()) or None


class EmailHeaders(BaseModel):
    """
    Model for email headers extracted from IMAP messages.
    """

    id: str
    subject: str
    sender: str
    recipients: str | None
    cc: str | None
    date: datetime | None

    @classmethod
    def from_email_msg(cls, email_msg: Message) -> "EmailHeaders":
        def _decode(header: str) -> str | None:
            return decode_header_value(email_msg.get(header))

        def _parse_date(date_str: str | None) -> datetime | None:
            if not date_str:
                return None
            try:
                return email.utils.parsedate_to_datetime(date_str)
            except (TypeError, ValueError):
                return None

        subject = _decode(Header.SUBJECT_HEADER) or "(no subject)"
        from_ = _decode(Header.FROM_HEADER) or "Unknown Sender"
        to = _decode(Header.TO_HEADER) or _decode(Header.DELIVERED_TO_HEADER)
        date_str = _decode(Header.DATE_HEADER)

        message_id = _decode(Header.MESSAGE_ID_HEADER)
        if not message_id:
            # Without a Message-ID, derive a stable id so re-indexing updates
            # the same document instead of creating a new one.
            raw_id = f"{from_}\n{date_str or ''}\n{subject}"
            message_id = f"gen_{hashlib.sha256(raw_id.encode('utf-8', errors='ignore')).hexdigest()}"

        return cls(
            id=message_id,
            subject=subject,
            sender=from_,
            recipients=to,
            cc=_decode(Header.CC_HEADER),
            date=_parse_date(date_str),
        )
