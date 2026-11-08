from __future__ import annotations

import signal
import sys
from pathlib import Path

import click

from spark_on_k8s_dbt.lifecycle import InvocationOwner, invocation_scope


@click.group()
def main():
    """Experimental dbt Core commands with invocation-scoped Spark compute."""


@main.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--receipt-dir",
    type=click.Path(path_type=Path),
    help="A new directory for the private recovery receipt (created exclusively).",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED, required=True)
def run(receipt_dir, dbt_args):
    """Provision lazily, run dbt arguments after --, then delete owned compute."""
    from dbt.cli.main import dbtRunner

    try:
        owner = InvocationOwner(receipt_dir)
    except Exception as error:
        # Exit 1 is reserved for a dbt execution failure; dbt has not started.
        click.echo(f"Managed dbt failed: {error}", err=True)
        sys.exit(2)
    click.echo(f"Experimental managed dbt; recovery receipt: {owner.receipt}", err=True)
    exit_code = 2
    interrupted = None
    saved_signals = {}

    def interrupt(signum, frame):
        nonlocal interrupted
        interrupted = signum
        # dbt waits for its worker threads after an interrupt; one may still be
        # waiting for the driver. Stop that wait rather than finishing startup.
        owner.cancel()
        raise KeyboardInterrupt

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            saved_signals[signum] = signal.signal(signum, interrupt)
        with invocation_scope(owner):
            result = dbtRunner().invoke(list(dbt_args))
            exit_code = 0 if result.success else 2 if result.exception is not None else 1
            if result.exception is not None:
                click.echo(f"dbt failed: {result.exception}", err=True)
    except KeyboardInterrupt:
        exit_code = 128 + (interrupted or signal.SIGINT)
    except Exception as error:
        click.echo(f"Managed dbt failed: {error}", err=True)
    finally:
        if interrupted is not None:
            exit_code = 128 + interrupted
        # Finish bounded resource cleanup even when another termination signal arrives.
        for signum in saved_signals:
            signal.signal(signum, signal.SIG_IGN)
        try:
            owner.close(dbt_exit_code=exit_code)
        except Exception as error:
            click.echo(str(error), err=True)
            if exit_code == 0:
                exit_code = 2
        finally:
            for signum, handler in saved_signals.items():
                signal.signal(signum, handler)
    sys.exit(exit_code)


@main.command()
@click.option("--receipt", required=True, type=click.Path(exists=True, path_type=Path))
def cleanup(receipt):
    """Retry UID-checked compute cleanup using a complete recovery receipt."""
    try:
        owner = InvocationOwner.recover(receipt)
        owner.close()
    except Exception as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Owned compute is absent; receipt updated: {owner.receipt}")


if __name__ == "__main__":
    main()
