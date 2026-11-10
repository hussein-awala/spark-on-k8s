from __future__ import annotations

import json
import math
import signal
import sys
import time
from contextlib import nullcontext, redirect_stdout
from itertools import groupby, takewhile
from pathlib import Path

import click

from spark_on_k8s_dbt.lifecycle import InvocationOwner, invocation_scope
from spark_on_k8s_dbt.progress import StartupProgress


@click.group()
def main():
    """Experimental dbt Core commands with invocation-scoped Spark compute."""


def _parse_vars(ctx, param, value):
    from dbt.clients.yaml_helper import load_yaml_text

    try:
        parsed = load_yaml_text(value)
    except Exception as error:
        raise click.BadParameter("must be a YAML mapping, as for dbt --vars") from error
    if not isinstance(parsed, dict):
        raise click.BadParameter("must be a YAML mapping, as for dbt --vars")
    return parsed


def _display(report):
    from spark_on_k8s_dbt.check import CAVEAT

    selected = f"profile {report.profile or '(unresolved)'}, target {report.target or '(unresolved)'}"
    click.echo(f"Managed dbt check for {selected}; nothing was created.")
    # One line for each run of checks skipped for the same reason.
    for _, group in groupby(
        report.checks, key=lambda check: (check.status, check.message if check.status == "skip" else check.id)
    ):
        check, *rest = group
        more = f" and {len(rest)} more" if rest else ""
        click.echo(f"{check.status.upper()} {check.id}{more}: {check.message}")
        if check.status == "fail" and check.remediation:
            click.echo(f"     Next: {check.remediation}")
    counts = {status: sum(check.status == status for check in report.checks) for status in ("fail", "pass")}
    skipped = len(report.checks) - counts["fail"] - counts["pass"]
    click.echo(f"{counts['fail']} failed, {counts['pass']} passed, {skipped} skipped.")
    if report.error:
        click.echo(f"Error: {report.error['message']}", err=True)
    click.echo(CAVEAT)


@main.command()
@click.option(
    "--project-dir",
    envvar="DBT_PROJECT_DIR",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory containing dbt_project.yml. As in dbt, defaults to the nearest one at or above the "
    "current directory. Not required when --profile is given.",
)
@click.option(
    "--profiles-dir",
    envvar="DBT_PROFILES_DIR",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory containing profiles.yml. As in dbt, defaults to the current directory if it has one, "
    "otherwise ~/.dbt.",
)
@click.option("--profile", envvar="DBT_PROFILE", help="Profile to check; overrides dbt_project.yml.")
@click.option(
    "--target", "-t", envvar="DBT_TARGET", help="Output to check; defaults to the profile's target."
)
@click.option(
    "--vars", "cli_vars", default="{}", callback=_parse_vars, help="YAML mapping for var() in profiles.yml."
)
@click.option(
    "--timeout",
    type=click.FloatRange(min=0, min_open=True, max=300),
    default=30,
    show_default=True,
    help="Total seconds allowed for Kubernetes requests.",
)
@click.option("--output", type=click.Choice(["text", "json"]), default="text", show_default=True)
def check(project_dir, profiles_dir, profile, target, cli_vars, timeout, output):
    """Validate a managed profile and its cluster access without creating compute.

    Exits 0 when every check passes or is skipped, 1 when any check fails and 130
    when interrupted. A passing check does not prove that a dbt invocation succeeds.
    """
    from dbt.cli.resolvers import default_profiles_dir, default_project_dir

    from spark_on_k8s_dbt.check import CheckReport, run_checks

    report = CheckReport()
    code = 0
    try:
        # One versioned JSON document on stdout; anything else goes to stderr.
        with redirect_stdout(sys.stderr) if output == "json" else nullcontext():
            run_checks(
                report,
                project_dir=project_dir or default_project_dir(),
                profiles_dir=profiles_dir or default_profiles_dir(),
                profile=profile,
                target=target,
                cli_vars=cli_vars,
                timeout=timeout,
            )
    except KeyboardInterrupt:
        code = 130
        report.error = {"code": "interrupted", "message": "Interrupted; the check creates no resources."}
        report.skip_remaining("interrupted")
    except Exception as error:
        report.error = {"code": "operation_error", "message": str(error)}
        report.skip_remaining("the check stopped unexpectedly")
    if output == "json":
        click.echo(json.dumps(report.to_dict(), ensure_ascii=False))
    else:
        _display(report)
    sys.exit(code or (1 if report.status == "fail" else 0))


def _split_commands(dbt_args):
    """Split the arguments after the first -- into dbt commands at each further --."""
    commands = [[]]
    for arg in dbt_args:
        if arg == "--":
            commands.append([])
        else:
            commands[-1].append(arg)
    if not all(commands):
        raise click.UsageError("Each -- must be followed by the arguments of a dbt command")
    return commands


def _finite(ctx, param, value):
    if value is not None and not math.isfinite(value):
        raise click.BadParameter("must be a positive finite number of seconds")
    return value


class _Sequence:
    """Command lines and a summary on stderr when one run has several dbt commands.

    A single command prints none of these, exactly as before sequences existed.
    """

    def __init__(self, commands, progress):
        self.commands = commands
        self.progress = progress
        self.statuses = []
        self.reason = None

    def _name(self, index):
        # Leading words only, such as "docs generate": option values such as
        # --vars can carry credentials and are never echoed.
        words = list(takewhile(lambda arg: not arg.startswith("-"), self.commands[index]))[:2]
        return f"{index + 1}/{len(self.commands)} ({' '.join(['dbt', *words])})"

    def finished(self, code, elapsed, *, interrupted):
        index = len(self.statuses)
        status = "interrupted" if interrupted else "failed" if code else "succeeded"
        self.statuses.append(status)
        if len(self.commands) == 1:
            return
        seconds = f"{math.floor(elapsed)}s"
        if status == "succeeded":
            self.progress.command(f"{self._name(index)} succeeded in {seconds}")
        elif status == "failed":
            self.progress.command(f"{self._name(index)} failed with exit {code} after {seconds}", always=True)
        else:
            self.progress.command(f"{self._name(index)} interrupted after {seconds}", always=True)

    def stop(self, reason):
        if self.reason is None and len(self.statuses) < len(self.commands):
            self.reason = reason

    def summary(self, exit_code):
        if len(self.commands) == 1:
            return
        counts = {status: self.statuses.count(status) for status in ("succeeded", "failed", "interrupted")}
        counts["not run"] = len(self.commands) - len(self.statuses)
        parts = ", ".join(f"{count} {status}" for status, count in counts.items() if count)
        reason = f" ({self.reason})" if self.reason and counts["not run"] else ""
        self.progress.command(
            f"run finished with exit {exit_code}: {parts}{reason}", always=exit_code != 0 or bool(reason)
        )


@main.command(context_settings={"ignore_unknown_options": True, "allow_interspersed_args": False})
@click.option(
    "--receipt-dir",
    type=click.Path(path_type=Path),
    help="A new directory for the private recovery receipt (created exclusively).",
)
@click.option(
    "--keep-going",
    is_flag=True,
    help="Start the later commands of a sequence after a dbt command fails. The exit code is still "
    "the first failure's.",
)
@click.option(
    "--deadline",
    type=click.FloatRange(min=0, min_open=True),
    callback=_finite,
    help="Seconds from the start after which no further dbt command starts; exit 5 if one was not "
    "started. A running command is not interrupted, and owned compute is still deleted at the end.",
)
@click.argument("dbt_args", nargs=-1, type=click.UNPROCESSED, required=True)
def run(receipt_dir, keep_going, deadline, dbt_args):
    """Provision lazily, run dbt arguments after --, then delete owned compute.

    Separate several dbt commands with further -- to run them in order on one
    server: run -- seed -- build -- docs generate. The sequence stops at the
    first failing command unless --keep-going is given. Put wrapper options
    before the first dbt argument; everything after it belongs to dbt.
    """
    from dbt.cli.main import dbtRunner

    commands = _split_commands(dbt_args)
    started = time.monotonic()
    progress = StartupProgress()
    try:
        owner = InvocationOwner(receipt_dir, progress=progress)
    except Exception as error:
        # Exit 1 is reserved for a dbt execution failure; dbt has not started.
        click.echo(f"Managed dbt failed: {error}", err=True)
        sys.exit(2)
    click.echo(f"Experimental managed dbt; recovery receipt: {owner.receipt}", err=True)
    sequence = _Sequence(commands, progress)
    exit_code = 2
    outcome = 0  # The first nonzero command or deadline result.
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
            for index, args in enumerate(commands):
                if outcome and not keep_going:
                    sequence.stop("stopped after a failed command; --keep-going runs the rest")
                    break
                if deadline is not None and time.monotonic() - started >= deadline:
                    # The deadline only stops new commands; it never cancels one.
                    outcome = outcome or 5
                    sequence.stop(f"the {deadline:g}s deadline passed")
                    break
                if index:
                    owner.prepare_next_command()
                command_started = time.monotonic()
                result = dbtRunner().invoke(list(args))
                code = 0 if result.success else 2 if result.exception is not None else 1
                if result.exception is not None:
                    click.echo(f"dbt failed: {result.exception}", err=True)
                elapsed = time.monotonic() - command_started
                sequence.finished(code, elapsed, interrupted=interrupted is not None)
                if interrupted is not None:
                    break
                outcome = outcome or code
        exit_code = outcome
    except KeyboardInterrupt:
        exit_code = 128 + (interrupted or signal.SIGINT)
    except Exception as error:
        click.echo(f"Managed dbt failed: {error}", err=True)
        exit_code = outcome or 2
        sequence.stop("stopped by the error above")
    finally:
        if interrupted is not None:
            exit_code = 128 + interrupted
        if exit_code >= 128:
            sequence.stop("interrupted")
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
        sequence.summary(exit_code)
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
