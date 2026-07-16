# anyio and rich are in the lsst-scipipe environment
from __future__ import annotations

import asyncio
import re
import sys
from collections import deque
from collections.abc import AsyncGenerator, Generator
from contextvars import ContextVar
from math import inf
from pathlib import Path
from typing import Annotated, Literal

import anyio
import click
from classad2 import ClassAd, parseAds
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TaskID, TimeElapsedColumn
from rich.table import Table

FAILED_STATES = {6}
KNOWN_EXCEPTIONS = [
    "lsst::pex::exceptions::RuntimeError",
    "botocore.exceptions.EndpointConnectionError",
]
RETRYABLE_EXCEPTIONS = []

exc_pattern = re.compile(rf"({'|'.join(map(re.escape, KNOWN_EXCEPTIONS))})")
"""Regex pattern group-matching any registered known exception strings"""

console = Console(stderr=True)

progress = Progress(
    SpinnerColumn(), *Progress.get_default_columns(), "Elapsed:", TimeElapsedColumn(), console=console
)

ptask: ContextVar[TaskID] = ContextVar("ptask")


def task_callback(task: asyncio.Task | None = None):
    """Done callback used by tasks added to taskgroups.

    Updates the rich progress in the task's context.
    """
    progress.update(ptask.get(), advance=1)


def format_output(o: dict, format: Annotated[str, Literal["json", "table"]]):
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
            lambda ad_: ad_.get("Type") == "NodeStatus" and ad_.get("NodeStatus") in FAILED_STATES,
            parseAds(f),
        )


async def locate_log_files(nodes, p: anyio.Path) -> AsyncGenerator[tuple[str, anyio.Path]]:
    """Locate and provide a generator for any log files associated with a
    member of `nodes`.
    """
    nodeset = set(nodes)
    async for path in p.rglob("**/*.*.out"):
        if (label := path.stem.partition(".")[0]) in nodeset:
            nodeset.remove(label)
            task_callback()
            yield label, path
            if not nodeset:
                break


async def heuristic(node: str, p: anyio.Path, sem: asyncio.Semaphore) -> tuple[str, tuple[str, str]] | None:
    """Search log file for `node` for any well-known exceptions"""
    async with sem, await anyio.open_file(p, "r") as f:
        match_result = ("", "")
        while line := await f.readline():
            if match := exc_pattern.search(line):
                match_result = (match.group(1), line[match.end() :].lstrip(": ").rstrip())
                break
    return node, match_result


async def amain(submit_dir: Path, limit: int, format: str, max_failures: float) -> None:
    """Asynchronous application entry point"""
    progress.start()
    limiter = asyncio.Semaphore(limit)
    jobs_dir = anyio.Path(submit_dir) / "jobs"
    output = {}

    try:
        node_status_file = next(submit_dir.glob("*.node_status"))
    except StopIteration:
        print("No node_status file found in submit dir")
        sys.exit(1)

    ad = dag_status(node_status_file)
    if ad is None:
        console.log("[red]No DAG Status found[/red]")
        sys.exit(0)

    output["total"] = ad["NodesTotal"]
    output["done"] = ad["NodesDone"]
    output["failed"] = ad["NodesFailed"]
    output["failures"] = {}
    output["indeterminate"] = []

    # The finalJob is always "failed" if there are any other DAG failures
    if output["failed"] > 1:
        output["failed"] -= 1

    if output["failed"] > 0:
        ptask0 = progress.add_task(
            "[green]Finding [red]failed[/red] nodes in [cyan]node_status[/cyan] file...[/green]",
            total=output["failed"],
        )
        found_fails = 0
        for failed_node in failed_nodes(node_status_file):
            if failed_node["Node"] == "finalJob":
                continue
            found_fails += 1
            output["failures"][failed_node["Node"]] = {"logfile": None, "exception": None, "reason": None}
            progress.update(ptask0, advance=1)
            if found_fails >= output["failed"] or found_fails >= max_failures:
                break

    ptask0 = progress.add_task(
        "[green]Locating [cyan]log files[/cyan] for [red]failed[/red] nodes...[/green]",
        total=output["failed"],
    )
    token = ptask.set(ptask0)
    async for node, log_file in locate_log_files(output["failures"].keys(), jobs_dir):
        output["failures"][node]["logfile"] = str(log_file)
    ptask.reset(token)

    ptask1 = progress.add_task(
        "[green]Reading [cyan]log files[/cyan] for [red]failed[/red] nodes...[/green]", total=output["failed"]
    )
    heuristics = deque(maxlen=output["failed"])
    async with asyncio.TaskGroup() as tg:
        token = ptask.set(ptask1)
        for node, v in output["failures"].items():
            if v["logfile"] is None:
                output["indeterminate"].append(node)
                continue
            tg_task = tg.create_task(heuristic(node, v["logfile"], limiter))
            tg_task.add_done_callback(task_callback)
            heuristics.append(tg_task)
        ptask.reset(token)
    progress.stop()

    for task in heuristics:
        failed_node, match_result = task.result()
        exc, reason = match_result
        if not exc:
            output["indeterminate"].append(failed_node)
        else:
            output["failures"][failed_node]["exception"] = exc
            output["failures"][failed_node]["reason"] = reason

    format_output(output, format)


@click.command()
@click.argument("submit_dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--limit", default=10, help="max number of concurrent tasks", show_default=True)
@click.option(
    "--format", type=click.Choice(["json", "table"]), default="json", show_default=True, show_envvar=True
)
@click.option(
    "--max-failures", type=float, default=inf, help="stop reporting heuristics after N failures"
)
def main(submit_dir: Path, limit: int, format: str, max_failures: float) -> None:
    """Application entry point"""
    asyncio.run(amain(submit_dir, limit, format, max_failures))


if __name__ == "__main__":
    main()
