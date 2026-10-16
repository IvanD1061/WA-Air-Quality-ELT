"""Open a Snowflake connection with key-pair authentication.

Snowflake no longer lets service accounts log in with a password, so the
pipeline signs in with an RSA key pair: the public half is attached to the
Snowflake user, the private half stays in `secrets/` and never leaves this
machine.
"""

from __future__ import annotations

from pipeline.config import SnowflakeSettings


def connect(settings: SnowflakeSettings, *, role: str | None = None, schema: str | None = None):
    import snowflake.connector  # imported lazily so unit tests do not need it

    options = {
        "account": settings.account,
        "user": settings.user,
        "private_key_file": settings.private_key_path,
        "role": role or settings.role,
        "warehouse": settings.warehouse,
        "database": settings.database,
        "schema": schema or settings.raw_schema,
        "application": "wa_air_quality_elt",
    }
    if settings.private_key_passphrase:
        options["private_key_file_pwd"] = settings.private_key_passphrase
    return snowflake.connector.connect(**options)
