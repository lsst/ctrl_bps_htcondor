#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["anyio", "rich", "click", "htcondor==24.0.*; sys_platform == 'linux'"]  # noqa: W505
# ///
"""Dagman node status parser

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

import pickle
import re
import signal
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
    "RuntimeError",
    "lsst::pex::exceptions::RuntimeError",
    "botocore.exceptions.EndpointConnectionError",
]
RETRYABLE_EXCEPTIONS = []

exc_pattern = re.compile(rf"({'|'.join(map(re.escape, KNOWN_EXCEPTIONS))})")
"""Regex pattern group-matching any registered known exception strings"""

console = Console(stderr=False)
stderr = Console(stderr=True)

progress = Progress(
    SpinnerColumn(), *Progress.get_default_columns(), "Elapsed:", TimeElapsedColumn(), console=stderr
)

ptask: ContextVar[TaskID] = ContextVar("ptask")


class DagNodeStatus(IntEnum):
    """Enum of potential Dag or Node states reported by dagman"""

    NOT_READY = 0
    READY = 1
    PRERUN = 2
    SUBMITTED = 3
    POSTRUN = 4
    DONE = 5
    ERROR = 6
    FUTILE = 7


@dataclass
class Context:
    """Mutable context object for tracking HTCondor DAG results"""

    status: int = 0
    total: int = 0
    done: int = 0
    failed: int = 0
    futile: int = 0
    failures: dict = field(default_factory=dict)
    indeterminate: set = field(default_factory=set)

    def __ior__(self, other: Context) -> Self:
        """Dunder method supporting `|=` merge operation"""
        self.total += other.total
        self.done += other.done
        self.failed += other.failed
        self.futile += other.futile
        self.failures |= other.failures
        self.indeterminate |= other.indeterminate
        return self

    def __iand__(self, other: Context) -> Self:
        """Dunder method supporting `&=` merge operation"""
        self.status = max(self.status, other.status)
        self.total = other.total
        self.done = other.done
        self.failed = other.failed
        self.futile = other.futile
        self.failures |= other.failures
        self.indeterminate |= other.indeterminate
        return self


async def with_callback[**P, T](fn: Callable[P, Awaitable[T]], *args: P.args, **kwargs: P.kwargs) -> T:
    """Wrap a task with a done callback that advances its progress"""
    try:
        return await fn(*args, **kwargs)
    finally:
        progress.update(ptask.get(), advance=1)


def format_output(o: dict, format: Annotated[str, Literal["json", "table"]]) -> None:
    """Format the terminal output according to supplied option"""
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


async def locate_log_files(context: Context, p: anyio.Path) -> AsyncGenerator[tuple[str, anyio.Path]]:
    """Locate and provide a generator for any log files associated with failed
    nodes in `context`, skipping those nodes that already have a logfile or
    have already been marked indeterminate.
    """
    nodeset: set[str] = {
        node
        for node in context.failures.keys()
        if not context.failures[node]["logfile"] and node not in context.indeterminate
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
    """Search log file for `node` for any well-known exceptions"""
    async with limiter, await anyio.open_file(p, "r") as f:
        match_result = ("", "")
        while line := await f.readline():
            if match := exc_pattern.search(line):
                match_result = (match.group(1), line[match.end() :].lstrip(": ").rstrip())
                break
    return node, match_result


async def discover_node_status(p: Path) -> Context:
    """Discover the DAG status and return a context object to merge with
    global status.
    """
    ad = dag_status(p)
    context = Context()
    if ad is None:
        stderr.log("[red]No DAG Status found[/red]")
        return context

    context.total = ad["NodesTotal"]
    context.done = ad["NodesDone"]
    context.failed = ad["NodesFailed"]
    context.futile = ad["NodesFutile"]
    context.status = ad["DagStatus"]

    return context


async def status(
    submit_dir: Path, capacity: int, max_failures: float, format: str, *, with_cache: bool
) -> None:
    """Asynchronous application entry point"""
    progress.start()
    limiter = anyio.CapacityLimiter(capacity)
    jobs_dir = anyio.Path(submit_dir) / "jobs"
    subdags_dir = Path(submit_dir) / "subdags"
    status_cache_file = Path(submit_dir) / "cm_node_status.pickle"
    status_files = []
    if with_cache and status_cache_file.exists():
        with status_cache_file.open("rb") as f:
            output = pickle.load(f)
    else:
        output = Context()

    try:
        node_status_file = next(submit_dir.glob("*.node_status"))
        node_status = await discover_node_status(node_status_file)
        output &= node_status
        status_files.append(node_status_file)
    except StopIteration:
        stderr.log("[red]No node_status file found in submit dir[/red]")
        return None

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
                if found_fails >= output.failed or found_fails >= max_failures:
                    break
    # Reconcile failed_nodeset with current context
    known_failed_nodes = set(output.failures.keys())
    for node in known_failed_nodes.difference(failed_nodeset):
        # known nodes not in current failset are no longer failed!
        output.failures.pop(node)
        output.indeterminate.discard(node)
    for node in failed_nodeset.difference(known_failed_nodes):
        # new failures are added to context
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

    # save context in cache file
    if with_cache:
        try:
            with status_cache_file.open("wb") as f:
                pickle.dump(output, f, pickle.HIGHEST_PROTOCOL)
        except OSError:
            stderr.log("[red]Could not cache results (OSError)[/red]")

    format_output(asdict(output), format)


async def signal_handler(scope: CancelScope) -> None:
    """Cancel scope when signal received"""
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                stderr.log("[red]Ctrl+C Pressed[/red]")
            else:
                stderr.log("[red]Received Termination Signal[/red]")
            scope.cancel()
            return


async def amain(
    submit_dir: Path, capacity: int, format: str, max_failures: float, interval: float, cache: bool
):
    """Async application entry point"""
    async with anyio.create_task_group() as tg:
        tg.start_soon(signal_handler, tg.cancel_scope)
        while True:
            await status(submit_dir, capacity, max_failures, format, with_cache=cache)
            stderr.log("[green]sleeping...[/green]")
            await anyio.sleep(interval)


@click.command()
@click.argument("submit_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--capacity", default=10, help="max number of concurrent tasks", show_default=True)
@click.option(
    "--format", type=click.Choice(["json", "table"]), default="json", show_default=True, show_envvar=True
)
@click.option("--max-failures", type=float, default=inf, help="stop reporting heuristics after N failures")
@click.option("--interval", type=float, default=600, help="wakeup interval")
@click.option("--cache/--no-cache", default=True)
def main(
    submit_dir: Path, *, capacity: int, format: str, max_failures: float, interval: float, cache: bool
) -> None:
    """Application entry point"""
    anyio.run(amain, submit_dir, capacity, format, max_failures, interval, cache)


if __name__ == "__main__":
    main()
