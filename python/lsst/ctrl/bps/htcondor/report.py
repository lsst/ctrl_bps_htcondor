# This file is part of ctrl_bps_htcondor.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Support functions for report."""


import logging
import re
from pathlib import Path
from typing import Any

import htcondor
from lsst.ctrl.bps import WmsJobReport, WmsRunReport, WmsSpecificInfo, WmsStates
from lsst.ctrl.bps.bps_utils import parse_count_summary

from .lssthtc import (
    MISSING_ID,
    JobStatus,
    NodeStatus,
    condor_search,
    htc_check_dagman_output,
    pegasus_name_to_label,
    read_dag_info,
    read_dag_log,
    read_dag_status,
    read_node_status,
    summary_from_dag,
)
from .utils import is_service_job

_LOG = logging.getLogger(__name__)


def _report_from_path(wms_path):
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_path : `str`
        The directory containing the submit side files (e.g., HTCondor files).

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the detailed report.  The key is the HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    wms_workflow_id, jobs, message = _get_info_from_path(wms_path)
    if wms_workflow_id == MISSING_ID:
        run_reports = {}
    else:
        run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
    return run_reports, message


def _report_from_id(wms_workflow_id, hist, schedds=None):
    """Gather run information using workflow id.

    Parameters
    ----------
    wms_workflow_id : `str`
        Limit to specific run based on id.
    hist : `float`
        Limit history search to this many days.
    schedds : `dict` [ `str`, `htcondor.Schedd` ], optional
        HTCondor schedulers which to query for job information. If None
        (default), all queries will be run against the local scheduler only.

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the detailed report.  The key is the HTCondor id
        and the value is a collection of report information for that run.
    message : `str`
        Message to be printed with the summary report.
    """
    messages = []

    # Collect information about the job by querying HTCondor schedd and
    # HTCondor history.
    schedd_dag_info = _get_info_from_schedd(wms_workflow_id, hist, schedds)
    if len(schedd_dag_info) == 1:
        # Extract the DAG info without altering the results of the query.
        schedd_name = next(iter(schedd_dag_info))
        dag_id = next(iter(schedd_dag_info[schedd_name]))
        dag_ad = schedd_dag_info[schedd_name][dag_id]

        # If the provided workflow id does not correspond to the one extracted
        # from the DAGMan log file in the submit directory, rerun the query
        # with the id found in the file.
        #
        # This is to cover the situation in which the user provided the old job
        # id of a restarted run.
        try:
            path_dag_id, _ = read_dag_log(dag_ad["Iwd"])
        except FileNotFoundError as exc:
            # At the moment missing DAGMan log is pretty much a fatal error.
            # So empty the DAG info to finish early (see the if statement
            # below).
            schedd_dag_info.clear()
            messages.append(f"Cannot create the report for '{dag_id}': {exc}")
        else:
            if path_dag_id != dag_id:
                schedd_dag_info = _get_info_from_schedd(path_dag_id, hist, schedds)
                messages.append(
                    f"WARNING: Found newer workflow executions in same submit directory as id '{dag_id}'. "
                    "This normally occurs when a run is restarted. The report shown is for the most "
                    f"recent status with run id '{path_dag_id}'"
                )

    if len(schedd_dag_info) == 0:
        run_reports = {}
    elif len(schedd_dag_info) == 1:
        _, dag_info = schedd_dag_info.popitem()
        dag_id, dag_ad = dag_info.popitem()

        # Create a mapping between jobs and their classads. The keys will
        # be of format 'ClusterId.ProcId'.
        job_info = {dag_id: dag_ad}

        # Find jobs (nodes) belonging to that DAGMan job.
        job_constraint = f"DAGManJobId == {int(float(dag_id))}"
        schedd_job_info = condor_search(constraint=job_constraint, hist=hist, schedds=schedds)
        if schedd_job_info:
            _, node_info = schedd_job_info.popitem()
            job_info.update(node_info)

        # Collect additional pieces of information about jobs using HTCondor
        # files in the submission directory.
        _, path_jobs, message = _get_info_from_path(dag_ad["Iwd"])
        _update_jobs(job_info, path_jobs)
        if message:
            messages.append(message)
        run_reports = _create_detailed_report_from_jobs(dag_id, job_info)
    else:
        ids = [ad["GlobalJobId"] for dag_info in schedd_dag_info.values() for ad in dag_info.values()]
        message = (
            f"More than one job matches id '{wms_workflow_id}', "
            f"their global ids are: {', '.join(ids)}. Rerun with one of the global ids"
        )
        messages.append(message)
        run_reports = {}

    message = "\n".join(messages)
    return run_reports, message


def _get_info_from_schedd(
    wms_workflow_id: str, hist: float, schedds: dict[str, htcondor.Schedd]
) -> dict[str, dict[str, dict[str, Any]]]:
    """Gather run information from HTCondor.

    Parameters
    ----------
    wms_workflow_id : `str`
        Limit to specific run based on id.
    hist : `int`
        Limit history search to this many days.
    schedds : `dict` [ `str`, `htcondor.Schedd` ], optional
        HTCondor schedulers which to query for job information. If None
        (default), all queries will be run against the local scheduler only.

    Returns
    -------
    schedd_dag_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    dag_constraint = 'regexp("dagman$", Cmd)'
    try:
        cluster_id = int(float(wms_workflow_id))
    except ValueError:
        dag_constraint += f' && GlobalJobId == "{wms_workflow_id}"'
    else:
        dag_constraint += f" && ClusterId == {cluster_id}"

    # With the current implementation of the condor_* functions the query
    # will always return only one match per Scheduler.
    #
    # Even in the highly unlikely situation where HTCondor history (which
    # condor_search queries too) is long enough to have jobs from before
    # the cluster ids were rolled over (and as a result there is more than
    # one job with the same cluster id) they will not show up in
    # the results.
    schedd_dag_info = condor_search(constraint=dag_constraint, hist=hist, schedds=schedds)
    return schedd_dag_info


def _get_info_from_path(wms_path: str) -> tuple[str, dict[str, dict[str, Any]], str]:
    """Gather run information from a given run directory.

    Parameters
    ----------
    wms_path : `str`
        Directory containing HTCondor files.

    Returns
    -------
    wms_workflow_id : `str`
        The run id which is a DAGman job id.
    jobs : `dict` [`str`, `dict` [`str`, `Any`]]
        Information about jobs read from files in the given directory.
        The key is the HTCondor id and the value is a dictionary of HTCondor
        keys and values.
    message : `str`
        Message to be printed with the summary report.
    """
    # Ensure path is absolute, in particular for folks helping
    # debug failures that need to dig around submit files.
    wms_path = Path(wms_path).resolve()

    messages = []
    try:
        wms_workflow_id, jobs = read_dag_log(wms_path)
        _LOG.debug("_get_info_from_path: from dag log %s = %s", wms_workflow_id, jobs)
        _update_jobs(jobs, read_node_status(wms_path))
        _LOG.debug("_get_info_from_path: after node status %s = %s", wms_workflow_id, jobs)

        # Add more info for DAGman job
        job = jobs[wms_workflow_id]
        job.update(read_dag_status(wms_path))

        job["total_jobs"], job["state_counts"] = _get_state_counts_from_jobs(wms_workflow_id, jobs)
        if "bps_run" not in job:
            _add_run_info(wms_path, job)

        message = htc_check_dagman_output(wms_path)
        if message:
            messages.append(message)
        _LOG.debug(
            "_get_info: id = %s, total_jobs = %s", wms_workflow_id, jobs[wms_workflow_id]["total_jobs"]
        )

        # Add extra pieces of information which cannot be found in HTCondor
        # generated files like 'GlobalJobId'.
        #
        # Do not treat absence of this file as a serious error. Neither runs
        # submitted with earlier versions of the plugin nor the runs submitted
        # with Pegasus plugin will have it at the moment. However, once enough
        # time passes and Pegasus plugin will have its own report() method
        # (instead of sneakily using HTCondor's one), the lack of that file
        # should be treated as seriously as lack of any other file.
        try:
            job_info = read_dag_info(wms_path)
        except FileNotFoundError as exc:
            message = f"Warn: Some information may not be available: {exc}"
            messages.append(message)
        else:
            schedd_name = next(iter(job_info))
            job_ad = next(iter(job_info[schedd_name].values()))
            job.update(job_ad)
    except FileNotFoundError:
        message = f"Could not find HTCondor files in '{wms_path}'"
        _LOG.warning(message)
        messages.append(message)
        message = htc_check_dagman_output(wms_path)
        if message:
            messages.append(message)
        wms_workflow_id = MISSING_ID
        jobs = {}

    message = "\n".join([msg for msg in messages if msg])
    _LOG.debug("wms_workflow_id = %s, jobs = %s", wms_workflow_id, jobs.keys())
    _LOG.debug("message = %s", message)
    return wms_workflow_id, jobs, message


def _create_detailed_report_from_jobs(
    wms_workflow_id: str, jobs: dict[str, dict[str, Any]]
) -> dict[str, WmsRunReport]:
    """Gather run information to be used in generating summary reports.

    Parameters
    ----------
    wms_workflow_id : `str`
        The run id to create the report for.
    jobs : `dict` [`str`, `dict` [`str`, Any]]
        Mapping HTCondor job id to job information.

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the detailed report.  The key is the given HTCondor
        id and the value is a collection of report information for that run.
    """
    _LOG.debug("_create_detailed_report: id = %s, job = %s", wms_workflow_id, jobs[wms_workflow_id])
    dag_ad = jobs[wms_workflow_id]

    # Following code assumes only payload jobs in job dictionary
    # so make new dictionary with only payload jobs.
    payload_jobs = {}
    wms_jobs = {}
    for job_id, job_ad in jobs.items():
        name = job_ad.get("DAGNodeName", job_id)
        if job_id == wms_workflow_id or name.startswith("wms_"):
            wms_jobs[job_id] = job_ad
        else:
            payload_jobs[job_id] = job_ad

    total_jobs, state_counts = _get_state_counts_from_dag_job(dag_ad)
    if "workflow_job_summary" in dag_ad:
        expected_workflow_job_counts = parse_count_summary(dag_ad["workflow_job_summary"])
        total_number_jobs = int(expected_workflow_job_counts["payload"])
    else:
        total_number_jobs = total_jobs

    report = WmsRunReport(
        wms_id=f"{dag_ad['ClusterId']}.{dag_ad['ProcId']}",
        global_wms_id=dag_ad.get("GlobalJobId", "MISS"),
        path=dag_ad["Iwd"],
        label=dag_ad.get("bps_job_label", "MISS"),
        run=dag_ad.get("bps_run", "MISS"),
        project=dag_ad.get("bps_project", "MISS"),
        campaign=dag_ad.get("bps_campaign", "MISS"),
        payload=dag_ad.get("bps_payload", "MISS"),
        operator=_get_owner(dag_ad),
        run_summary=_get_run_summary(dag_ad),
        state=_htc_status_to_wms_state(dag_ad),
        jobs=[],
        total_number_jobs=total_number_jobs,
        job_state_counts=dict.fromkeys(WmsStates, 0),
        exit_code_summary=_get_exit_code_summary(payload_jobs),
    )

    # Add reporting for payload jobs.
    for job_id, job_ad in payload_jobs.items():
        if not is_service_job(job_id):
            try:
                wms_state = _htc_status_to_wms_state(job_ad)
                job_report = WmsJobReport(
                    wms_id=job_id,
                    name=job_ad.get("DAGNodeName", job_id),
                    label=job_ad.get("bps_job_label", pegasus_name_to_label(job_ad["DAGNodeName"])),
                    state=_htc_status_to_wms_state(job_ad),
                )
                report.job_state_counts[wms_state] += 1

                if job_report.label == "init":
                    job_report.label = "pipetaskInit"
                report.jobs.append(job_report)
            except KeyError as ex:
                _LOG.error("Job missing key '%s': %s", str(ex), job_ad)
                raise

    # Add reporting for special wms jobs, e.g., service jobs
    for job_id, job_ad in wms_jobs.items():
        if is_service_job(job_id):
            job_label = job_ad.get("bps_job_label")
            if job_label is None:
                _LOG.warning("Service job with id '%s': missing label, no action taken", job_id)
            elif job_label == dag_ad.get("bps_provisioning_job", "MISS"):
                report.specific_info = WmsSpecificInfo()
                job_status = _htc_status_to_wms_state(job_ad)
                if job_status == WmsStates.DELETED:
                    if "Reason" in job_ad and "Removed by DAGMan" in job_ad["Reason"]:
                        job_status = WmsStates.SUCCEEDED
                report.specific_info.add_message(
                    template="Provisioning job status: {status}",
                    context={"status": job_status.name},
                )
            else:
                _LOG.warning(
                    "Service job with id '%s' (label '%s'): no handler, no action taken", job_id, job_label
                )

    run_reports = {report.wms_id: report}
    _LOG.debug("_create_detailed_report: run_reports = %s", run_reports)
    return run_reports


def _summary_report(user, hist, pass_thru, schedds=None):
    """Gather run information to be used in generating summary reports.

    Parameters
    ----------
    user : `str`
        Run lookup restricted to given user.
    hist : `float`
        How many previous days to search for run information.
    pass_thru : `str`
        Advanced users can define the HTCondor constraint to be used
        when searching queue and history.

    Returns
    -------
    run_reports : `dict` [`str`, `lsst.ctrl.bps.WmsRunReport`]
        Run information for the summary report.  The keys are HTCondor ids and
        the values are collections of report information for each run.
    message : `str`
        Message to be printed with the summary report.
    """
    # only doing summary report so only look for dagman jobs
    if pass_thru:
        constraint = pass_thru
    else:
        # Notes:
        # * bps_isjob == 'True' isn't getting set for DAG jobs that are
        #   manually restarted.
        # * Any job with DAGManJobID isn't a DAG job
        constraint = 'bps_isjob == "True" && JobUniverse == 7'
        if user:
            constraint += f' && (Owner == "{user}" || bps_operator == "{user}")'

    job_info = condor_search(constraint=constraint, hist=hist, schedds=schedds)

    # Have list of DAGMan jobs, need to get run_report info.
    run_reports = {}
    for jobs in job_info.values():
        for job_id, job in jobs.items():
            total_jobs, state_counts = _get_state_counts_from_dag_job(job)
            # If didn't get from queue information (e.g., Kerberos bug),
            # try reading from file.
            if total_jobs == 0:
                try:
                    job.update(read_dag_status(job["Iwd"]))
                    total_jobs, state_counts = _get_state_counts_from_dag_job(job)
                except StopIteration:
                    pass  # don't kill report can't find htcondor files

            if "bps_run" not in job:
                _add_run_info(job["Iwd"], job)
            report = WmsRunReport(
                wms_id=job_id,
                global_wms_id=job["GlobalJobId"],
                path=job["Iwd"],
                label=job.get("bps_job_label", "MISS"),
                run=job.get("bps_run", "MISS"),
                project=job.get("bps_project", "MISS"),
                campaign=job.get("bps_campaign", "MISS"),
                payload=job.get("bps_payload", "MISS"),
                operator=_get_owner(job),
                run_summary=_get_run_summary(job),
                state=_htc_status_to_wms_state(job),
                jobs=[],
                total_number_jobs=total_jobs,
                job_state_counts=state_counts,
            )
            run_reports[report.global_wms_id] = report

    return run_reports, ""


def _add_run_info(wms_path, job):
    """Find BPS run information elsewhere for runs without bps attributes.

    Parameters
    ----------
    wms_path : `str`
        Path to submit files for the run.
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Raises
    ------
    StopIteration
        If cannot find file it is looking for.  Permission errors are
        caught and job's run is marked with error.
    """
    path = Path(wms_path) / "jobs"
    try:
        subfile = next(path.glob("**/*.sub"))
    except (StopIteration, PermissionError):
        job["bps_run"] = "Unavailable"
    else:
        _LOG.debug("_add_run_info: subfile = %s", subfile)
        try:
            with open(subfile, encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("+bps_"):
                        m = re.match(r"\+(bps_\S+)\s*=\s*(.+)$", line)
                        if m:
                            _LOG.debug("Matching line: %s", line)
                            job[m.group(1)] = m.group(2).replace('"', "")
                        else:
                            _LOG.debug("Could not parse attribute: %s", line)
        except PermissionError:
            job["bps_run"] = "PermissionError"
    _LOG.debug("After adding job = %s", job)


def _get_owner(job):
    """Get the owner of a dag job.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    owner : `str`
        Owner of the dag job.
    """
    owner = job.get("bps_operator", None)
    if not owner:
        owner = job.get("Owner", None)
        if not owner:
            _LOG.warning("Could not get Owner from htcondor job: %s", job)
            owner = "MISS"
    return owner


def _get_run_summary(job):
    """Get the run summary for a job.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    summary : `str`
        Number of jobs per PipelineTask label in approximate pipeline order.
        Format: <label>:<count>[;<label>:<count>]+
    """
    summary = job.get("bps_job_summary", job.get("bps_run_summary", None))
    if not summary:
        summary, _ = summary_from_dag(job["Iwd"])
        if not summary:
            _LOG.warning("Could not get run summary for htcondor job: %s", job)
    _LOG.debug("_get_run_summary: summary=%s", summary)

    # Workaround sometimes using init vs pipetaskInit
    summary = summary.replace("init:", "pipetaskInit:")

    if "pegasus_version" in job and "pegasus" not in summary:
        summary += ";pegasus:0"

    return summary


def _get_exit_code_summary(jobs):
    """Get the exit code summary for a run.

    Parameters
    ----------
    jobs : `dict` [`str`, `dict` [`str`, Any]]
        Mapping HTCondor job id to job information.

    Returns
    -------
    summary : `dict` [`str`, `list` [`int`]]
        Jobs' exit codes per job label.
    """
    summary = {}
    for job_id, job_ad in jobs.items():
        job_label = job_ad["bps_job_label"]
        summary.setdefault(job_label, [])
        try:
            exit_code = 0
            job_status = job_ad["JobStatus"]
            match job_status:
                case JobStatus.COMPLETED | JobStatus.HELD:
                    exit_code = job_ad["ExitSignal"] if job_ad["ExitBySignal"] else job_ad["ExitCode"]
                case (
                    JobStatus.IDLE
                    | JobStatus.RUNNING
                    | JobStatus.REMOVED
                    | JobStatus.TRANSFERRING_OUTPUT
                    | JobStatus.SUSPENDED
                ):
                    pass
                case _:
                    _LOG.debug("Unknown 'JobStatus' value ('%d') in classad for job '%s'", job_status, job_id)
            if exit_code != 0:
                summary[job_label].append(exit_code)
        except KeyError as ex:
            _LOG.debug("Attribute '%s' not found in the classad for job '%s'", ex, job_id)
    return summary


def _get_state_counts_from_jobs(
    wms_workflow_id: str, jobs: dict[str, dict[str, Any]]
) -> tuple[int, dict[WmsStates, int]]:
    """Count number of jobs per WMS state.

    The workflow job and the service jobs are excluded from the count.

    Parameters
    ----------
    wms_workflow_id : `str`
        HTCondor job id.
    jobs : `dict [`dict` [`str`, `Any`]]
        HTCondor dag job information.

    Returns
    -------
    total_count : `int`
        Total number of dag nodes.
    state_counts : `dict` [`lsst.ctrl.bps.WmsStates`, `int`]
        Keys are the different WMS states and values are counts of jobs
        that are in that WMS state.
    """
    state_counts = dict.fromkeys(WmsStates, 0)
    for job_id, job_ad in jobs.items():
        if job_id != wms_workflow_id and not is_service_job(job_id):
            state_counts[_htc_status_to_wms_state(job_ad)] += 1
    total_counted = sum(state_counts.values())

    dag_ad = jobs[wms_workflow_id]
    if "workflow_job_summary" in dag_ad:
        expected_workflow_job_counts = parse_count_summary(dag_ad["workflow_job_summary"])
        total_count = expected_workflow_job_counts["payload"]
    else:
        total_count = dag_ad.get("NodesTotal", total_counted)

    state_counts[WmsStates.UNREADY] += total_count - total_counted
    return total_count, state_counts


def _get_state_counts_from_dag_job(job):
    """Count number of jobs per WMS state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor dag job information.

    Returns
    -------
    total_count : `int`
        Total number of dag nodes.
    state_counts : `dict` [`lsst.ctrl.bps.WmsStates`, `int`]
        Keys are the different WMS states and values are counts of jobs
        that are in that WMS state.
    """
    _LOG.debug("_get_state_counts_from_dag_job: job = %s %s", type(job), len(job))
    state_counts = dict.fromkeys(WmsStates, 0)
    if "DAG_NodesReady" in job:
        state_counts = {
            WmsStates.UNREADY: job.get("DAG_NodesUnready", 0),
            WmsStates.READY: job.get("DAG_NodesReady", 0),
            WmsStates.HELD: job.get("DAG_JobsHeld", 0),
            WmsStates.SUCCEEDED: job.get("DAG_NodesDone", 0),
            WmsStates.FAILED: job.get("DAG_NodesFailed", 0),
            WmsStates.PRUNED: job.get("DAG_NodesFutile", 0),
            WmsStates.MISFIT: job.get("DAG_NodesPre", 0) + job.get("DAG_NodesPost", 0),
        }
        total_jobs = job.get("DAG_NodesTotal")
        _LOG.debug("_get_state_counts_from_dag_job: from DAG_* keys, total_jobs = %s", total_jobs)
    elif "NodesFailed" in job:
        state_counts = {
            WmsStates.UNREADY: job.get("NodesUnready", 0),
            WmsStates.READY: job.get("NodesReady", 0),
            WmsStates.HELD: job.get("JobProcsHeld", 0),
            WmsStates.SUCCEEDED: job.get("NodesDone", 0),
            WmsStates.FAILED: job.get("NodesFailed", 0),
            WmsStates.PRUNED: job.get("NodesFutile", 0),
            WmsStates.MISFIT: job.get("NodesPre", 0) + job.get("NodesPost", 0),
        }
        try:
            total_jobs = job.get("NodesTotal")
        except KeyError as ex:
            _LOG.error("Job missing %s. job = %s", str(ex), job)
            raise
        _LOG.debug("_get_state_counts_from_dag_job: from NODES* keys, total_jobs = %s", total_jobs)
    else:
        # With Kerberos job auth and Kerberos bug, if warning would be printed
        # for every DAG.
        _LOG.debug("Can't get job state counts %s", job["Iwd"])
        total_jobs = 0

    _LOG.debug("total_jobs = %s, state_counts: %s", total_jobs, state_counts)
    return total_jobs, state_counts


def _htc_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `WmsStates`
        The equivalent WmsState to given job's status.
    """
    wms_state = WmsStates.MISFIT
    if "JobStatus" in job:
        wms_state = _htc_job_status_to_wms_state(job)

    if wms_state == WmsStates.MISFIT and "NodeStatus" in job:
        wms_state = _htc_node_status_to_wms_state(job)
    return wms_state


def _htc_job_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `lsst.ctrl.bps.WmsStates`
        The equivalent WmsState to given job's status.
    """
    _LOG.debug(
        "htc_job_status_to_wms_state: %s=%s, %s", job["ClusterId"], job["JobStatus"], type(job["JobStatus"])
    )
    job_status = int(job["JobStatus"])
    wms_state = WmsStates.MISFIT

    _LOG.debug("htc_job_status_to_wms_state: job_status = %s", job_status)
    if job_status == JobStatus.IDLE:
        wms_state = WmsStates.PENDING
    elif job_status == JobStatus.RUNNING:
        wms_state = WmsStates.RUNNING
    elif job_status == JobStatus.REMOVED:
        wms_state = WmsStates.DELETED
    elif job_status == JobStatus.COMPLETED:
        if (
            (job.get("ExitBySignal", False) and job.get("ExitSignal", 0))
            or job.get("ExitCode", 0)
            or job.get("DAG_Status", 0)
        ):
            wms_state = WmsStates.FAILED
        else:
            wms_state = WmsStates.SUCCEEDED
    elif job_status == JobStatus.HELD:
        wms_state = WmsStates.HELD

    return wms_state


def _htc_node_status_to_wms_state(job):
    """Convert HTCondor node status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `lsst.ctrl.bps.WmsStates`
        The equivalent WmsState to given node's status.
    """
    wms_state = WmsStates.MISFIT
    match job["NodeStatus"]:
        case NodeStatus.NOT_READY:
            wms_state = WmsStates.UNREADY
        case NodeStatus.READY:
            wms_state = WmsStates.READY
        case NodeStatus.PRERUN:
            wms_state = WmsStates.MISFIT
        case NodeStatus.SUBMITTED:
            if job["JobProcsHeld"]:
                wms_state = WmsStates.HELD
            elif job["StatusDetails"] == "not_idle":
                wms_state = WmsStates.RUNNING
            elif job["JobProcsQueued"]:
                wms_state = WmsStates.PENDING
        case NodeStatus.POSTRUN:
            wms_state = WmsStates.MISFIT
        case NodeStatus.DONE:
            wms_state = WmsStates.SUCCEEDED
        case NodeStatus.ERROR:
            # Use job exit status instead of post script exit status.
            if "DAGMAN error 0" in job["StatusDetails"]:
                wms_state = WmsStates.SUCCEEDED
            elif "ULOG_JOB_ABORTED" in job["StatusDetails"]:
                wms_state = WmsStates.DELETED
            else:
                wms_state = WmsStates.FAILED
        case NodeStatus.FUTILE:
            wms_state = WmsStates.PRUNED
    return wms_state


def _update_jobs(jobs1, jobs2):
    """Update jobs1 with info in jobs2.

    (Basically an update for nested dictionaries.)

    Parameters
    ----------
    jobs1 : `dict` [`str`, `dict` [`str`, `Any`]]
        HTCondor job information to be updated.
    jobs2 : `dict` [`str`, `dict` [`str`, `Any`]]
        Additional HTCondor job information.
    """
    for job_id, job_ad in jobs2.items():
        if job_id in jobs1:
            jobs1[job_id].update(job_ad)
        else:
            jobs1[job_id] = job_ad
