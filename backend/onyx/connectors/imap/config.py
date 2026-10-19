import os
from typing import Annotated

from onyx.connectors.connector_config import ConnectorConfig, CredentialBinding
from onyx.connectors.field_policy import (
    FieldClass,
    FieldPolicy,
    ScopeExclude,
    ScopeInclude,
    ScopeToggle,
)

DEFAULT_IMAP_PORT_NUMBER = int(os.environ.get("IMAP_PORT", 993))


class ImapCredentialBinding(CredentialBinding):
    # Document ids are Message-ID headers, so the server picks what they resolve to.
    host: Annotated[str, FieldPolicy(FieldClass.IDENTITY)]
    port: Annotated[int, FieldPolicy(FieldClass.IDENTITY)] = DEFAULT_IMAP_PORT_NUMBER


class ImapConnectorConfig(ImapCredentialBinding, ConnectorConfig):
    mailboxes: Annotated[
        list[str] | None,
        FieldPolicy(FieldClass.SCOPE, scope=ScopeInclude(empty_means_all=True)),
    ] = None
    # Glob patterns for mailboxes to skip, e.g. "Trash", "*Spam*", "INBOX.Archiv.*".
    excluded_mailboxes: Annotated[
        list[str] | None,
        FieldPolicy(FieldClass.SCOPE, scope=ScopeExclude(split_on_commas=False)),
    ] = None
    index_attachments: Annotated[
        bool, FieldPolicy(FieldClass.SCOPE, scope=ScopeToggle(widens_when=True))
    ] = True
