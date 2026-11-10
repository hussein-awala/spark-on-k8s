"""Several dbt commands in one wrapper run share one owner, receipt and cleanup."""

from __future__ import annotations

import json
import re
import signal
from argparse import Namespace
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock

import dbt.flags
import pytest
from click.testing import CliRunner
from kubernetes.client.exceptions import ApiException
from spark_on_k8s_dbt import cli, lifecycle
from spark_on_k8s_dbt.cli import main
from spark_on_k8s_dbt.lifecycle import Endpoint, InvocationOwner, get_invocation_owner
from spark_on_k8s_dbt.progress import StartupProgress

OK = SimpleNamespace(success=True, exception=None)
FAILED = SimpleNamespace(success=False, exception=None)


class Session:
    """Record each dbt invocation, the between-command checks and cleanup, in order."""

    def __init__(self, monkeypatch, *results):
        import dbt.cli.main

        self.results = list(results)
        self.events = []
        self.owners = set()
        self.on_invoke = {}
        close, prepare = InvocationOwner.close, InvocationOwner.prepare_next_command

        def record_close(owner, **kwargs):
            self.events.append(("close", kwargs.get("dbt_exit_code")))
            return close(owner, **kwargs)

        def record_prepare(owner):
            self.events.append(("prepare",))
            return prepare(owner)

        monkeypatch.setattr(InvocationOwner, "close", record_close)
        monkeypatch.setattr(InvocationOwner, "prepare_next_command", record_prepare)
        monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: SimpleNamespace(invoke=self.invoke))
        # Startup progress follows dbt's resolved console flags; start from dbt's defaults.
        monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", Namespace(USE_COLORS=True))

    def invoke(self, args):
        self.events.append(("invoke", *args))
        self.owners.add(id(get_invocation_owner()))
        action = self.on_invoke.get(len([event for event in self.events if event[0] == "invoke"]))
        if action is not None:
            return action()
        return self.results.pop(0)

    def run(self, tmp_path, *arguments):
        result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), *arguments])
        assert result.stdout == ""  # dbt's stdout stays dbt's; the mocked dbt writes none.
        return result

    @property
    def invoked(self):
        return [list(event[1:]) for event in self.events if event[0] == "invoke"]


def receipt(tmp_path):
    return json.loads((tmp_path / "run/receipt.json").read_text())


def managed_lines(result):
    return [line for line in result.stderr.splitlines() if line.startswith("Managed dbt ")]


def test_commands_share_one_owner_and_compute_is_deleted_once_at_the_end(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, OK, OK)
    result = session.run(tmp_path, "--", "seed", "--full-refresh", "--", "build", "--", "docs", "generate")
    assert result.exit_code == 0, result.output
    assert session.events == [
        ("invoke", "seed", "--full-refresh"),
        ("prepare",),
        ("invoke", "build"),
        ("prepare",),
        ("invoke", "docs", "generate"),
        ("close", 0),
    ]
    assert len(session.owners) == 1
    assert get_invocation_owner() is None
    assert [path.name for path in tmp_path.iterdir()] == ["run"]
    assert receipt(tmp_path)["dbt_exit_code"] == 0
    assert managed_lines(result) == [
        "Managed dbt 1/3 (dbt seed) succeeded in 0s",
        "Managed dbt 2/3 (dbt build) succeeded in 0s",
        "Managed dbt 3/3 (dbt docs generate) succeeded in 0s",
        "Managed dbt run finished with exit 0: 3 succeeded",
    ]


def test_single_command_output_is_unchanged(tmp_path, monkeypatch):
    session = Session(monkeypatch, FAILED)
    result = session.run(tmp_path, "--", "build")
    assert result.exit_code == 1
    assert session.events == [("invoke", "build"), ("close", 1)]
    assert managed_lines(result) == []


def test_first_failure_stops_the_sequence_and_sets_the_exit_code(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, FAILED, OK)
    result = session.run(tmp_path, "--", "seed", "--", "build", "--select", "orders", "--", "test")
    assert result.exit_code == 1
    assert session.invoked == [["seed"], ["build", "--select", "orders"]]
    assert session.events[-1] == ("close", 1)
    assert receipt(tmp_path)["dbt_exit_code"] == 1
    assert managed_lines(result)[1:] == [
        "Managed dbt 2/3 (dbt build) failed with exit 1 after 0s",
        "Managed dbt run finished with exit 1: 1 succeeded, 1 failed, 1 not run "
        "(stopped after a failed command; --keep-going runs the rest)",
    ]


def test_keep_going_runs_every_command_and_keeps_the_first_failure_code(tmp_path, monkeypatch):
    session = Session(monkeypatch, FAILED, SimpleNamespace(success=False, exception=ValueError("bad")), OK)
    result = session.run(tmp_path, "--keep-going", "--", "run", "--", "run-operation", "x", "--", "test")
    assert result.exit_code == 1
    assert session.invoked == [["run"], ["run-operation", "x"], ["test"]]
    assert "dbt failed: bad" in result.stderr
    assert managed_lines(result) == [
        "Managed dbt 1/3 (dbt run) failed with exit 1 after 0s",
        "Managed dbt 2/3 (dbt run-operation x) failed with exit 2 after 0s",
        "Managed dbt 3/3 (dbt test) succeeded in 0s",
        "Managed dbt run finished with exit 1: 1 succeeded, 2 failed",
    ]


def test_command_lines_never_echo_option_values(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, OK)
    result = session.run(
        tmp_path, "--", "--log-format", "json", "seed", "--", "run", "--vars", "{password: secret}"
    )
    assert result.exit_code == 0
    assert session.invoked == [["--log-format", "json", "seed"], ["run", "--vars", "{password: secret}"]]
    assert not any("secret" in line or "json" in line for line in managed_lines(result))
    assert managed_lines(result)[:2] == [
        "Managed dbt 1/2 (dbt) succeeded in 0s",
        "Managed dbt 2/2 (dbt run) succeeded in 0s",
    ]


@pytest.mark.parametrize("flags", [Namespace(QUIET=True), Namespace(QUIET=False, LOG_LEVEL="warn")])
def test_quiet_dbt_hides_successes_but_never_failures_or_skipped_commands(tmp_path, monkeypatch, flags):
    session = Session(monkeypatch, OK, FAILED)
    monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", flags)
    result = session.run(tmp_path, "--", "seed", "--", "build", "--", "test")
    assert result.exit_code == 1
    assert managed_lines(result) == [
        "Managed dbt 2/3 (dbt build) failed with exit 1 after 0s",
        "Managed dbt run finished with exit 1: 1 succeeded, 1 failed, 1 not run "
        "(stopped after a failed command; --keep-going runs the rest)",
    ]


def test_quiet_dbt_hides_a_successful_sequence(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, OK)
    monkeypatch.setattr(dbt.flags, "GLOBAL_FLAGS", Namespace(QUIET=True))
    result = session.run(tmp_path, "--", "seed", "--", "build")
    assert result.exit_code == 0
    assert managed_lines(result) == []


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_interrupt_during_a_command_starts_no_later_command(tmp_path, monkeypatch, signum):
    session = Session(monkeypatch, OK, OK, OK)
    previous = signal.getsignal(signal.SIGINT)

    def interrupted():
        # dbtRunner catches the interrupt after cancelling its connections and
        # returns it as the result's exception.
        try:
            signal.raise_signal(signum)
        except KeyboardInterrupt as error:
            assert get_invocation_owner().cancelled
            return SimpleNamespace(success=False, exception=error)
        raise AssertionError("The wrapper did not handle the signal")

    session.on_invoke[2] = interrupted
    result = session.run(tmp_path, "--keep-going", "--", "seed", "--", "build", "--", "test")
    assert result.exit_code == 128 + signum
    assert session.invoked == [["seed"], ["build"]]
    assert session.events[-1] == ("close", 128 + signum)
    assert signal.getsignal(signal.SIGINT) == previous
    assert managed_lines(result)[1:] == [
        "Managed dbt 2/3 (dbt build) interrupted after 0s",
        f"Managed dbt run finished with exit {128 + signum}: 1 succeeded, 1 interrupted, 1 not run "
        "(interrupted)",
    ]


def test_interrupt_escaping_dbt_still_cleans_up_once(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK)
    session.on_invoke[2] = lambda: signal.raise_signal(signal.SIGINT)
    result = session.run(tmp_path, "--", "seed", "--", "build", "--", "test")
    assert result.exit_code == 130
    assert [event[0] for event in session.events] == ["invoke", "prepare", "invoke", "close"]
    assert managed_lines(result)[-1] == (
        "Managed dbt run finished with exit 130: 1 succeeded, 2 not run (interrupted)"
    )


def test_interrupt_between_commands_starts_no_later_command(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, OK)

    def interrupt_while_checking(owner):
        session.events.append(("prepare",))
        signal.raise_signal(signal.SIGTERM)

    monkeypatch.setattr(InvocationOwner, "prepare_next_command", interrupt_while_checking)
    result = session.run(tmp_path, "--", "seed", "--", "build")
    assert result.exit_code == 143
    assert session.events == [("invoke", "seed"), ("prepare",), ("close", 143)]
    assert receipt(tmp_path)["dbt_exit_code"] == 143
    assert managed_lines(result)[-1] == (
        "Managed dbt run finished with exit 143: 1 succeeded, 1 not run (interrupted)"
    )


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.mark.parametrize("keep_going", [False, True])
def test_deadline_starts_no_new_command_but_never_interrupts_one(tmp_path, monkeypatch, keep_going):
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    session = Session(monkeypatch, OK, OK)

    def slow():
        clock.now += 90
        # The running command is neither cancelled nor cut short by the deadline.
        assert not get_invocation_owner().cancelled
        return OK

    session.on_invoke[2] = slow
    options = ["--keep-going"] if keep_going else []
    result = session.run(tmp_path, *options, "--deadline", "60", "--", "seed", "--", "build", "--", "test")
    assert result.exit_code == 5
    assert session.invoked == [["seed"], ["build"]]
    assert session.events[-1] == ("close", 5)
    assert receipt(tmp_path)["dbt_exit_code"] == 5
    assert managed_lines(result)[1:] == [
        "Managed dbt 2/3 (dbt build) succeeded in 90s",
        "Managed dbt run finished with exit 5: 2 succeeded, 1 not run (the 60s deadline passed)",
    ]


def test_earlier_failure_keeps_its_code_when_the_deadline_passes(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(cli.time, "monotonic", clock)
    session = Session(monkeypatch, OK)

    def slow_failure():
        clock.now += 90
        return FAILED

    session.on_invoke[1] = slow_failure
    result = session.run(tmp_path, "--keep-going", "--deadline", "60", "--", "run", "--", "test")
    assert result.exit_code == 1
    assert session.invoked == [["run"]]
    assert managed_lines(result)[-1] == (
        "Managed dbt run finished with exit 1: 1 failed, 1 not run (the 60s deadline passed)"
    )


def test_failed_startup_is_never_retried_by_a_later_command(tmp_path, monkeypatch):
    session = Session(monkeypatch)

    def startup_failed():
        # The adapter recorded its inputs, but no endpoint was ever verified.
        get_invocation_owner()._inputs = ("connect", {})
        return FAILED

    session.on_invoke[1] = startup_failed
    result = session.run(tmp_path, "--keep-going", "--", "seed", "--", "build")
    assert result.exit_code == 1
    assert session.invoked == [["seed"]]
    assert "Managed dbt failed: Managed Spark startup failed in an earlier command" in result.stderr
    assert managed_lines(result)[-1] == (
        "Managed dbt run finished with exit 1: 1 failed, 1 not run (stopped by the error above)"
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--", "seed", "--"],
        ["--", "--", "seed"],
        ["--", "seed", "--", "--", "build"],
        ["--deadline", "0", "--", "seed"],
        ["--deadline", "inf", "--", "seed"],
        ["--deadline", "nan", "--", "seed"],
    ],
)
def test_invalid_sequences_fail_before_any_receipt_or_dbt(tmp_path, monkeypatch, arguments):
    session = Session(monkeypatch)
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), *arguments])
    assert result.exit_code == 2, result.output
    assert session.events == []
    assert not (tmp_path / "run").exists()


def test_arguments_after_the_first_dbt_word_belong_to_dbt(tmp_path, monkeypatch):
    session = Session(monkeypatch, OK, OK)
    result = session.run(tmp_path, "seed", "--keep-going", "--", "build", "--deadline", "1")
    assert result.exit_code == 0
    assert session.invoked == [["seed", "--keep-going"], ["build", "--deadline", "1"]]


def ready_owner(tmp_path, monkeypatch, observed):
    owner = InvocationOwner(tmp_path / "receipt", progress=StartupProgress(allowed=lambda: True))
    owner._inputs = ("connect", {})
    owner.endpoint = Endpoint("127.0.0.1", 1234)
    owner.config = SimpleNamespace(
        startup_timeout=180, poll_interval=0.01, server=SimpleNamespace(namespace="test")
    )
    owner.state.update(
        method="connect",
        server={"run": {"namespace": "test", "app_id": "app", "driver_pod": "app-driver", "driver_uid": "d"}},
    )
    owner.sdk = Mock()
    owner.sdk.k8s_client_manager.client.side_effect = lambda: nullcontext(object())
    api = Mock()
    api.read_namespaced_pod.side_effect = observed
    monkeypatch.setattr(lifecycle.k8s, "CoreV1Api", lambda raw: api)
    return owner, api


def driver(phase="Running", uid="d", deleting=False):
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp="now" if deleting else None),
        status=SimpleNamespace(phase=phase),
    )


def test_next_command_reuses_a_running_driver_and_its_live_tunnel(tmp_path, monkeypatch):
    owner, api = ready_owner(tmp_path, monkeypatch, lambda *args, **kwargs: driver())
    owner.forward = Mock()
    owner.forward.process.poll.return_value = None
    endpoint = owner.endpoint
    owner.prepare_next_command()
    api.read_namespaced_pod.assert_called_once()
    assert api.read_namespaced_pod.call_args.args == ("app-driver", "test")
    owner.forward.start.assert_not_called()
    assert owner.endpoint is endpoint


def test_next_command_reopens_an_exited_tunnel_to_the_same_driver(tmp_path, monkeypatch, capsys):
    owner, api = ready_owner(tmp_path, monkeypatch, lambda *args, **kwargs: driver())
    owner.forward = Mock(pod="app-driver")
    owner.forward.process.poll.return_value = 1
    owner.forward.start.return_value = 4321
    owner._verified_endpoint = Mock(return_value=Endpoint("127.0.0.1", 4321))
    owner.prepare_next_command()
    owner.forward.close.assert_called_once_with()
    owner.forward.start.assert_called_once()
    owner._verified_endpoint.assert_called_once()
    assert owner._verified_endpoint.call_args.args[:2] == ("127.0.0.1", 4321)
    assert owner.endpoint.port == 4321
    messages = [line.split(": ", 1)[1] for line in capsys.readouterr().err.splitlines()]
    assert messages == [
        "kubectl port-forward exited; reopening the tunnel to the same driver",
        "driver container running",
        "tunnel reopened; the same driver answered a query",
    ]


def test_nothing_is_checked_or_created_before_compute_is_requested(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.prepare_next_command()
    assert owner.sdk is None and owner.state["stage"] == "reserved"


@pytest.mark.parametrize(
    "observed, message",
    [
        (ApiException(status=404), "no longer exists"),
        (driver(uid="other"), "replacement is not adopted"),
        (driver(phase="Failed"), r"no longer running \(phase Failed\)"),
        (driver(deleting=True), r"no longer running \(phase Running, deleting\)"),
    ],
)
def test_a_lost_driver_ends_the_run_instead_of_creating_another(tmp_path, monkeypatch, observed, message):
    owner, api = ready_owner(tmp_path, monkeypatch, [observed])
    owner.forward = Mock()
    owner.forward.process.poll.return_value = 1
    with pytest.raises(RuntimeError, match=message):
        owner.prepare_next_command()
    owner.sdk.create_connect_server.assert_not_called()
    owner.forward.start.assert_not_called()


def test_uncertain_driver_reads_are_not_reported_as_absence(tmp_path, monkeypatch):
    error = ApiException(status=500)
    error.body = "response body"
    owner, _ = ready_owner(tmp_path, monkeypatch, [error])
    with pytest.raises(RuntimeError, match=r"Could not confirm .* \(Kubernetes API status 500\)") as raised:
        owner.prepare_next_command()
    assert "no longer exists" not in str(raised.value) and "response body" not in str(raised.value)


def test_no_later_command_after_a_failed_startup_or_an_interrupt(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner._inputs = ("connect", {})
    with pytest.raises(RuntimeError, match="startup failed in an earlier command"):
        owner.prepare_next_command()
    owner.cancel()
    with pytest.raises(RuntimeError, match="already finished"):
        owner.prepare_next_command()


def test_real_dbt_commands_keep_their_own_fail_fast_and_hooks_on_one_owner(tmp_path, monkeypatch):
    executed = []

    class ConnectSession:
        description = None

        def __init__(self, profile, parameters):
            self.rows = []

        def cursor(self):
            return self

        def execute(self, sql, bindings=None):
            executed.append(sql)
            if "fail_me" in sql and "create" in sql.lower():
                raise RuntimeError("model failed on purpose")
            self.description = [("x", "string", None, None, None, None, True)]

        def fetchall(self):
            return self.rows

        def fetchmany(self, size):
            return self.rows[:size]

        def cancel(self):
            pass

        def close(self):
            pass

        def rollback(self):
            pass

    owners, closed = [], []
    close = InvocationOwner.close

    def ensure_endpoint(owner, method, compute):
        owners.append(owner)
        return Endpoint("localhost", 1234, Mock())

    def record_close(owner, **kwargs):
        closed.append(kwargs)
        return close(owner, **kwargs)

    monkeypatch.setattr("dbt.adapters.spark_on_k8s.connections.ConnectConnectionWrapper", ConnectSession)
    monkeypatch.setattr(InvocationOwner, "ensure_endpoint", ensure_endpoint)
    monkeypatch.setattr(InvocationOwner, "close", record_close)
    (tmp_path / "models").mkdir()
    (tmp_path / "dbt_project.yml").write_text(
        "name: sequence\nversion: 1.0.0\nconfig-version: 2\nprofile: sequence\n"
        "on-run-end:\n  - \"select 'on_run_end_hook_ran'\"\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "sequence:\n  target: local\n  outputs:\n    local:\n"
        "      type: spark_on_k8s\n      method: connect\n      schema: example\n"
        "      compute: {image: fixture}\n      threads: 1\n"
    )
    (tmp_path / "models" / "fail_me.sql").write_text("{{ config(materialized='view') }}\nselect 1 as id\n")
    (tmp_path / "models" / "works.sql").write_text("{{ config(materialized='view') }}\nselect 2 as id\n")
    common = [
        "--project-dir",
        str(tmp_path),
        "--profiles-dir",
        str(tmp_path),
        "--log-path",
        str(tmp_path / "logs"),
        "--target-path",
        str(tmp_path / "target"),
        "--no-partial-parse",
        "--no-send-anonymous-usage-stats",
    ]
    result = CliRunner().invoke(
        main,
        [
            "run",
            "--keep-going",
            "--receipt-dir",
            str(tmp_path / "run"),
            "--",
            "run",
            "--select",
            "fail_me",
            "--fail-fast",
            *common,
            "--",
            "run",
            "--select",
            "works",
            *common,
        ],
    )
    assert result.exit_code == 1, result.output
    # --fail-fast applied to the first command only, whose on-run-end hook still ran.
    hooks = [index for index, sql in enumerate(executed) if "on_run_end_hook_ran" in sql]
    works = [index for index, sql in enumerate(executed) if "works" in sql and "create" in sql.lower()]
    assert len(hooks) == 2 and works and hooks[0] < works[0] < hooks[1]
    # dbt resets its adapters for each command; every connection still used the one owner.
    assert owners and len({id(owner) for owner in owners}) == 1
    assert closed == [{"dbt_exit_code": 1}]
    assert [re.sub(r"\d+s$", "Ns", line) for line in managed_lines(result)] == [
        "Managed dbt 1/2 (dbt run) failed with exit 1 after Ns",
        "Managed dbt 2/2 (dbt run) succeeded in Ns",
        "Managed dbt run finished with exit 1: 1 succeeded, 1 failed",
    ]
