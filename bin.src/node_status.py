#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["anyio", "rich", "click", "htcondor==25.0.*; sys_platform == 'linux'"]  # noqa: W505
# ///

"""Dagman node status parser.

Interesting files include
- `*.node_status`, a snapshot of the ClassAds for the dag and every node in the
  dag, updated throughout dag execution.
- `*.dag.metrics`, a JSON file with workflow metrics, written at the end of the
  workflow.
- `*.dagman.out`. The append-only debug log of the dagman workflow executor.
- `*.nodes.log`. The htcondor event log for the dag

# TODO
- if we had the dagman's own clusterid, could we use the condor api instead of
  the node status file?
- would enabling a "JOBSTATE_LOG" be useful? it is the filtered version of the
  `dagman.out` log, so should be smaller and faster to parse.
"""

from __future__ import annotations

import datetime
import os
import re
import signal
import sqlite3
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from enum import IntEnum
from math import inf
from pathlib import Path
from typing import Annotated, Literal, Self

import anyio
import click
from anyio.abc import CancelScope
from classad2 import ClassAd, parseAds
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TaskID, TimeElapsedColumn
from rich.table import Table

KNOWN_EXCEPTIONS = [
    "botocore.exceptions.EndpointConnectionError",
    "lsst::afw::detection::InvalidPsfError",
    "lsst::pex::exceptions::RuntimeError",
    "sqlalchemy.exc.OperationalError",
    "^RuntimeError",
    "^ValueError",
]
RETRYABLE_EXCEPTIONS = []

exc_pattern = re.compile(rf"({'|'.join(map(re.escape, KNOWN_EXCEPTIONS))})")
"""Regex pattern group-matching any registered known exception strings."""

console = Console(stderr=False)
stderr = Console(stderr=True)

progress = Progress(
    SpinnerColumn(), *Progress.get_default_columns(), "Elapsed:", TimeElapsedColumn(), console=stderr
)

ptask: ContextVar[TaskID] = ContextVar("ptask")


class DagNodeStatus(IntEnum):
    """Enum of potential Dag or Node states reported by dagman."""

    NOT_READY = 0
    READY = 1
    PRERUN = 2
    SUBMITTED = 3
    POSTRUN = 4
    DONE = 5
    ERROR = 6
    FUTILE = 7


@dataclass
class Status:
    """Mutable status object for tracking HTCondor DAG results."""

    timestamp: datetime.datetime | None = None
    status: int = 0
    total: int = 0
    done: int = 0
    failed: int = 0
    futile: int = 0
    event_pointer: int = 0
    failures: dict = field(default_factory=dict)
    indeterminate: set = field(default_factory=set)

    def __ior__(self, other: Status) -> Self:
        """Dunder method supporting `|=` merge operation."""
        self.total += other.total
        self.done += other.done
        self.failed += other.failed
        self.futile += other.futile
        self.failures |= other.failures
        self.indeterminate |= other.indeterminate
        return self

    def __iand__(self, other: Status) -> Self:
        """Dunder method supporting `&=` merge operation."""
        self.status = max(self.status, other.status)
        self.total = other.total
        self.done = other.done
        self.failed = other.failed
        self.futile = other.futile
        self.failures |= other.failures
        self.indeterminate |= other.indeterminate
        return self

    @classmethod
    def row_factory(cls, cur: sqlite3.Cursor, row):
        fields = [column[0] for column in cur.description]
        return cls(**dict(zip(fields, row, strict=False)))


@dataclass
class Context:
    """Runtime context."""

    submit_dir: Path
    capacity: int
    format: str
    max_failures: float
    interval: float
    cache: bool
    cache_dir: Path


async def with_callback[**P, T](fn: Callable[P, Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> T:
    """Wrap a task with a done callback that advances its progress."""
    try:
        return await fn(*args, **kwargs)
    finally:
        progress.update(ptask.get(), advance=1)


def adapt_datetime_epoch(val: datetime.datetime) -> int:
    """Adapt ``datetime.datetime`` to Unix timestamp."""
    return int(val.timestamp())


def init_db(ctx: Context) -> Generator[sqlite3.Cursor]:
    """Initialize a sqlite3 database for caching and result storage."""
    con = sqlite3.connect(ctx.cache_dir / "cache.db", autocommit=True)
    con.row_factory = sqlite3.Row
    with con:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS status(timestamp, status, total, done, failed, futile, event_pointer)"
        )
        cur.execute("CREATE TABLE IF NOT EXISTS failures(label, exception, reason)")
        cur.execute("CREATE TABLE IF NOT EXISTS indeterminate(label)")
        yield cur


def format_output(o: dict, format: Annotated[str, Literal["json", "table"]]) -> None:
    """Format the terminal output according to supplied option."""
    if format == "json":
        console.print(o)
        return None

    summary_table = Table(title="Dagman Node Status Summary")

    summary_table.add_column("Status")
    summary_table.add_column("Count")
    summary_table.add_row("Total", f"{o['total']:,d}")
    summary_table.add_row("Done", f"{o['done']:,d}")
    summary_table.add_row("Failed", f"{o['failed']:,d}")
    summary_table.add_row("Pruned", f"{o['futile']:,d}")
    console.print(summary_table)

    if not o["failures"]:
        return None

    failure_table = Table(title="Dagman Node Failures Detail")
    failure_table.add_column("Label")
    failure_table.add_column("Exception")
    failure_table.add_column("Reason")
    for node, failure in o["failures"].items():
        failure_table.add_row(node, failure["exception"], failure["reason"])
    console.print(failure_table)


def dag_status(p: Path) -> ClassAd | None:
    """Find a classad for the DagStatus node in a DAGMan status file.

    This should often or always be the first ad in the file, so should be quick
    to locate.
    """
    with p.open("r") as f:
        ad = filter(
            lambda ad_: ad_.get("Type") == "DagStatus",
            parseAds(f),
        )
        return next(ad)


def failed_nodes(p: Path) -> Generator[ClassAd]:
    """Find classads for any failed/error nodes in a DAGMan status file.

    The `finalJob` is not included in this collection irrespective of its
    `NodeStatus` since it inherits its exit code from the `DAG_STATUS` instead
    of its own payload.
    """
    with p.open("r") as f:
        yield from filter(
            lambda ad_: ad_.get("Type") == "NodeStatus" and ad_.get("NodeStatus", 7) == DagNodeStatus.ERROR,
            parseAds(f),
        )


async def locate_log_files(status: Status, p: anyio.Path) -> AsyncGenerator[tuple[str, anyio.Path]]:
    """Locate and provide a generator for any log files associated with failed
    nodes in `status`, skipping those nodes that already have a logfile or
    have already been marked indeterminate.
    """
    nodeset: set[str] = {
        node
        for node in status.failures.keys()
        if not status.failures[node]["logfile"] and node not in status.indeterminate
    }
    if not nodeset:
        return
    # check cache for nodes we already know the log files for and only go back
    # to the filesystem if we need to
    async for path in p.rglob("**/*.*.out"):
        if (label := path.stem.partition(".")[0]) in nodeset:
            nodeset.remove(label)
            progress.update(ptask.get(), advance=1)
            yield label, path
            if not nodeset:
                break


async def heuristic(
    node: str, p: anyio.Path, limiter: anyio.CapacityLimiter
) -> tuple[str, tuple[str, str]] | None:
    """Search log file for `node` for any well-known exceptions."""
    async with limiter, await anyio.open_file(p, "r") as f:
        match_result = ("", "")
        while line := await f.readline():
            if match := exc_pattern.search(line):
                match_result = (match.group(1), line[match.end() :].lstrip(": ").rstrip())
                break
    return node, match_result


async def discover_node_status(p: Path) -> Status:
    """Discover the DAG status and return a status object to merge with
    global status.
    """
    ad = dag_status(p)
    status = Status()
    if ad is None:
        stderr.log("[red]No DAG Status found[/red]")
        return status

    status.total = ad["NodesTotal"]
    status.done = ad["NodesDone"]
    status.failed = ad["NodesFailed"]
    status.futile = ad["NodesFutile"]
    status.status = ad["DagStatus"]

    return status


async def check_status(ctx: Context) -> None:
    """Asynchronous application entry point"""
    progress.start()
    limiter = anyio.CapacityLimiter(ctx.capacity)
    jobs_dir = anyio.Path(ctx.submit_dir) / "jobs"
    subdags_dir = Path(ctx.submit_dir) / "subdags"
    status_files = []
    output = Status(timestamp=datetime.datetime.now(tz=datetime.UTC))

    cur: sqlite3.Cursor | None
    try:
        cur = next(init_db(ctx))
        s = cur.execute(
            "SELECT status, total, done, failed, futile, event_pointer "
            "FROM status ORDER BY ROWID DESC LIMIT 1"
        )
        r = s.fetchone()
        if r is not None:
            output |= Status(**r)
    except sqlite3.OperationalError:
        cur = None
        stderr.log("[red]Can't create or open a database file")

    try:
        node_status_file = next(ctx.submit_dir.glob("*.node_status"))
        node_status = await discover_node_status(node_status_file)
        output &= node_status
        status_files.append(node_status_file)
    except StopIteration:
        stderr.log("[red]No node_status file found in submit dir[/red]")
        return None

    # TODO read eventlog using ctx.event_pointer

    if subdags_dir.exists():
        for subdag_status_file in subdags_dir.rglob("*.node_status"):
            subdag_status = await discover_node_status(subdag_status_file)
            output |= subdag_status
            status_files.append(subdag_status_file)

    # The finalJob is always "failed" if there are any other DAG failures
    # assuming the DAG itself is completed and the finalJob has actually run
    if output.status >= DagNodeStatus.DONE and output.failed > 1:
        output.failed -= 1

    failed_nodeset = set()
    if output.failed > 0:
        ptask0 = progress.add_task(
            "[green]Finding [red]failed[/red] nodes in [cyan]node_status[/cyan] files...[/green]",
            total=output.failed,
        )
        found_fails = 0
        for status_file in status_files:
            for failed_node in failed_nodes(status_file):
                if failed_node["Node"] == "finalJob":
                    continue
                found_fails += 1
                failed_nodeset.add(failed_node["Node"])
                progress.update(ptask0, advance=1)
                if found_fails >= output.failed or found_fails >= ctx.max_failures:
                    break
    # Reconcile failed_nodeset with current status
    known_failed_nodes = set(output.failures.keys())
    for node in known_failed_nodes.difference(failed_nodeset):
        # known nodes not in current failset are no longer failed!
        output.failures.pop(node)
        output.indeterminate.discard(node)
    for node in failed_nodeset.difference(known_failed_nodes):
        # new failures are added to status
        output.failures[node] = {"logfile": None, "exception": None, "reason": None}

    ptask0 = progress.add_task(
        "[green]Locating [cyan]log files[/cyan] for [red]failed[/red] nodes...[/green]",
        total=output.failed,
    )
    token = ptask.set(ptask0)
    async for node, log_file in locate_log_files(output, jobs_dir):
        output.failures[node]["logfile"] = str(log_file)
    ptask.reset(token)

    ptask1 = progress.add_task(
        "[green]Reading [cyan]log files[/cyan] for [red]failed[/red] nodes...[/green]", total=output.failed
    )
    heuristics: deque[anyio.TaskHandle] = deque(maxlen=output.failed)
    async with anyio.create_task_group() as tg:
        token = ptask.set(ptask1)
        for node, v in output.failures.items():
            if v["logfile"] is None:
                output.indeterminate.add(node)
                continue
            if v["exception"] is not None:
                continue
            handle = tg.start_soon(with_callback, heuristic, node, v["logfile"], limiter)
            heuristics.append(handle)
        ptask.reset(token)
    progress.stop()

    for task in heuristics:
        failed_node, match_result = task.return_value
        exc, reason = match_result
        if not exc:
            output.indeterminate.add(failed_node)
        else:
            output.failures[failed_node]["exception"] = exc
            output.failures[failed_node]["reason"] = reason

    # save status in cache file
    if cur:
        cur.execute(
            "INSERT INTO "
            "status(timestamp, status, total, done, failed, futile, event_pointer) "
            "VALUES(?,?,?,?,?,?,?)",
            (output.timestamp, output.status, output.total, output.done, output.failed, output.futile, 0),
        )

    format_output(asdict(output), ctx.format)


async def signal_handler(scope: CancelScope) -> None:
    """Cancel scope when signal received."""
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                stderr.log("[red]Ctrl+C Pressed[/red]")
            else:
                stderr.log("[red]Received Termination Signal[/red]")
            scope.cancel()
            return


async def amain(ctx: Context):
    """Async application entry point.

    Parameters
    ----------
    ctx : ``Context``
        Runtime context object.
    """
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        while True:
            await check_status(ctx)
            if ctx.interval < 0:
                stderr.log("[green]exiting...[/green]")
                os.kill(os.getpid(), signal.SIGINT)
                break
            stderr.log("[green]sleeping...[/green]")
            await anyio.sleep(ctx.interval)


@click.command()
@click.argument("submit_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--capacity", default=10, help="max number of concurrent tasks", show_default=True)
@click.option(
    "--format", type=click.Choice(["json", "table"]), default="json", show_default=True, show_envvar=True
)
@click.option("--max-failures", type=float, default=inf, help="stop reporting heuristics after N failures")
@click.option("--interval", type=float, default=600, help="wakeup interval")
@click.option("--cache/--no-cache", default=True)
@click.option(
    "--cache-dir",
    default=None,
    type=click.Path(file_okay=False, path_type=Path),
    help="directory for sqlite cache file",
)
def main(
    submit_dir: Path,
    *,
    capacity: int,
    format: str,
    max_failures: float,
    interval: float,
    cache: bool,
    cache_dir: Path | None,
) -> None:
    """Application entry point."""
    if cache_dir is None:
        cache_dir = submit_dir
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)
    ctx = Context(submit_dir, capacity, format, max_failures, interval, cache, cache_dir)
    anyio.run(amain, ctx)


if __name__ == "__main__":
    main()
