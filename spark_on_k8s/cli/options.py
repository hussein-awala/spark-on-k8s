from __future__ import annotations

import click

namespace_option = click.Option(
    ("-n", "--namespace"),
    type=str,
    default="default",
    help="The namespace to operate on. Default is `default`.",
)


app_id_option = click.Option(("--app-id",), type=str, required=True, help="The ID of the app to operate on.")


force_option = click.Option(("-f", "--force"), is_flag=True, default=False, help="Force the operation.")
