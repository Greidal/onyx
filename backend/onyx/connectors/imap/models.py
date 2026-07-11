import email.header
import email.utils
from datetime import datetime
from email.message import Message
from enum import Enum

from pydantic import BaseModel


class Header(str, Enum):
    SUBJECT_HEADER = "subject"
    FROM_HEADER = "from"
    TO_HEADER = "to"
    DELIVERED_TO_HEADER = (
        "Delivered-To"  # Used in mailing lists instead of the "to" header.
    )
    DATE_HEADER = "date"
    MESSAGE_ID_HEADER = "Message-ID"


class EmailHeaders(BaseModel):
    """
    Model for email headers extracted from IMAP messages.
    """

    id: str
    subject: str
    sender: str
    recipients: str | None
    date: datetime

    @classmethod
    def from_email_msg(cls, email_msg: Message) -> "EmailHeaders":
        def _decode(header: str, default: str | None = None) -> str | None:
            value = email_msg.get(header, default)
            if not value:
                return None

            decoded_value, encoding = email.header.decode_header(value)[0]
            if isinstance(decoded_value, bytes):
                encoding = encoding or "utf-8"
                return decoded_value.decode(encoding, errors="replace")
            elif isinstance(decoded_value, str):
                return decoded_value
            else:
                return None

        def _parse_date(date_str: str | None) -> datetime | None:
            if not date_str:
                return None
            try:
                return email.utils.parsedate_to_datetime(date_str)
            except (TypeError, ValueError):
                return None

        message_id = _decode(header=Header.MESSAGE_ID_HEADER)
        subject = _decode(header=Header.SUBJECT_HEADER) or "Unknown Subject"
        from_ = _decode(header=Header.FROM_HEADER) or "Unknown Sender"
        to = _decode(header=Header.TO_HEADER)
        if not to:
            to = _decode(header=Header.DELIVERED_TO_HEADER)
            
        date_str = _decode(header=Header.DATE_HEADER)
        date = _parse_date(date_str=date_str)
        if not date:
            from datetime import timezone
            date = datetime.now(timezone.utc)

        if not message_id:
            import hashlib
            raw_id_data = f"{from_}-{date_str or ''}-{subject}"
            message_id = f"gen_{hashlib.md5(raw_id_data.encode('utf-8', errors='ignore')).hexdigest()}"

        return cls.model_validate(
            {
                "id": message_id,
                "subject": subject,
                "sender": from_,
                "recipients": to,
                "date": date,
            }
        )
