from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyConnectionBody,
    BulkCreateActionConnectionBody,
    ConnectionBody,
)


def validate_required_fields(model_class: type, data: dict[str, Any], context: str) -> None:
    missing_fields = [
        name
        for name, field in model_class.model_fields.items()
        if field.is_required() and (name not in data or data[name] is None)
    ]
    if missing_fields:
        raise SystemExit(
            f"Missing required fields for {context}: {', '.join(missing_fields)}"
        )


@provide_api_client(kind=ClientKind.CLI)
def import_(args, api_client=NEW_API_CLIENT) -> None:
    """Import connections from file."""
    filepath = Path(args.file)
    if not filepath.is_absolute():
        filepath = Path.cwd() / filepath

    if not filepath.exists():
        raise SystemExit(f"Missing connections file {args.file}")

    try:
        connections_json = json.loads(filepath.read_text())
    except Exception as e:
        raise SystemExit(f"Error reading connections file {args.file}: {e}") from e

    try:
        connections_data = {}

        for connection_id, values in connections_json.items():
            if not connection_id:
                raise SystemExit(
                    "Missing required fields for connection import: connection_id"
                )

            connection_values = {
                "connection_id": connection_id,
                "conn_type": values.get("conn_type"),
                "host": values.get("host"),
                "login": values.get("login"),
                "password": values.get("password"),
                "port": values.get("port"),
                "extra": values.get("extra"),
                "description": values.get("description", ""),
                **({"schema": values["schema"]} if "schema" in values else {}),
            }

            validate_required_fields(
                ConnectionBody,
                connection_values,
                f"connection '{connection_id}'",
            )

            connections_data[connection_id] = ConnectionBody(**connection_values)

        connection_create_action = BulkCreateActionConnectionBody(
            action="create",
            entities=list(connections_data.values()),
            action_on_existence=BulkActionOnExistence(args.action_on_existing_key),
        )
        response = api_client.connections.bulk(
            BulkBodyConnectionBody(actions=[connection_create_action])
        )

        if response.create.errors:
            rich.print(f"[red]Failed to import connections: {response.create.errors}[/red]")
            raise SystemExit(1)

        rich.print(f"[green]Successfully imported {response.create.success} connection(s)[/green]")
    except SystemExit:
        raise
    except Exception as e:
        rich.print(f"[red]Failed to import connections: {e}[/red]")
        raise SystemExit(1) from e
