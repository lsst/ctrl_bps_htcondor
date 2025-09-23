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

"""Utility functions for preparing the HTCondor workflow."""

import logging
import os
import re
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

from lsst.ctrl.bps import (
    BpsConfig,
    GenericWorkflow,
    GenericWorkflowGroup,
    GenericWorkflowJob,
    GenericWorkflowNodeType,
    GenericWorkflowNoopJob,
)
from lsst.ctrl.bps.bps_utils import create_count_summary

from .lssthtc import (
    HTCDag,
    HTCJob,
    condor_status,
    htc_escape,
)

_LOG = logging.getLogger(__name__)

DEFAULT_HTC_EXEC_PATT = ".*worker.*"
"""Default pattern for searching execute machines in an HTCondor pool.
"""


def _create_job(subdir_template, cached_values, generic_workflow, gwjob, out_prefix):
    """Convert GenericWorkflow job nodes to DAG jobs.

    Parameters
    ----------
    subdir_template : `str`
        Template for making subdirs.
    cached_values : `dict`
        Site and label specific values.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that is being converted.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        The generic job to convert to a HTCondor job.
    out_prefix : `str`
        Directory prefix for HTCondor files.

    Returns
    -------
    htc_job : `lsst.ctrl.bps.wms.htcondor.HTCJob`
        The HTCondor job equivalent to the given generic job.
    """
    htc_job = HTCJob(gwjob.name, label=gwjob.label)

    curvals = defaultdict(str)
    curvals["label"] = gwjob.label
    if gwjob.tags:
        curvals.update(gwjob.tags)

    subdir = Path("jobs") / subdir_template.format_map(curvals)
    htc_job.subdir = subdir
    htc_job.subfile = f"{gwjob.name}.sub"
    htc_job.add_dag_cmds({"dir": subdir})

    htc_job_cmds = {
        "universe": "vanilla",
        "should_transfer_files": "YES",
        "when_to_transfer_output": "ON_EXIT_OR_EVICT",
        "transfer_output_files": '""',  # Set to empty string to disable
        "transfer_executable": "False",
        "getenv": "True",
        # Exceeding memory sometimes triggering SIGBUS or SIGSEGV error. Tell
        # htcondor to put on hold any jobs which exited by a signal.
        "on_exit_hold": "ExitBySignal == true",
        "on_exit_hold_reason": (
            'strcat("Job raised a signal ", string(ExitSignal), ". ", '
            '"Handling signal as if job has gone over memory limit.")'
        ),
        "on_exit_hold_subcode": "34",
    }

    htc_job_cmds.update(_translate_job_cmds(cached_values, generic_workflow, gwjob))

    # Combine stdout and stderr to reduce the number of files.
    for key in ("output", "error"):
        if cached_values["overwriteJobFiles"]:
            htc_job_cmds[key] = f"{gwjob.name}.$(Cluster).out"
        else:
            htc_job_cmds[key] = f"{gwjob.name}.$(Cluster).$$([NumJobStarts ?: 0]).out"
        _LOG.debug("HTCondor %s = %s", key, htc_job_cmds[key])

    key = "log"
    htc_job_cmds[key] = f"{gwjob.name}.$(Cluster).{key}"
    _LOG.debug("HTCondor %s = %s", key, htc_job_cmds[key])

    htc_job_cmds.update(
        _handle_job_inputs(generic_workflow, gwjob.name, cached_values["bpsUseShared"], out_prefix)
    )

    htc_job_cmds.update(
        _handle_job_outputs(generic_workflow, gwjob.name, cached_values["bpsUseShared"], out_prefix)
    )

    # Add the job cmds dict to the job object.
    htc_job.add_job_cmds(htc_job_cmds)

    htc_job.add_dag_cmds(_translate_dag_cmds(gwjob))

    # Add job attributes to job.
    _LOG.debug("gwjob.attrs = %s", gwjob.attrs)
    htc_job.add_job_attrs(gwjob.attrs)
    htc_job.add_job_attrs(cached_values["attrs"])
    htc_job.add_job_attrs({"bps_job_quanta": create_count_summary(gwjob.quanta_counts)})
    htc_job.add_job_attrs({"bps_job_name": gwjob.name, "bps_job_label": gwjob.label})

    return htc_job


def _translate_job_cmds(cached_vals, generic_workflow, gwjob):
    """Translate the job data that are one to one mapping

    Parameters
    ----------
    cached_vals : `dict` [`str`, `~typing.Any`]
        Config values common to jobs with same site or label.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains job to being converted.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Generic workflow job to be converted.

    Returns
    -------
    htc_job_commands : `dict` [`str`, `~typing.Any`]
        Contains commands which can appear in the HTCondor submit description
        file.
    """
    # Values in the job script that just are name mappings.
    job_translation = {
        "mail_to": "notify_user",
        "when_to_mail": "notification",
        "request_cpus": "request_cpus",
        "priority": "priority",
        "category": "category",
        "accounting_group": "accounting_group",
        "accounting_user": "accounting_group_user",
    }

    jobcmds = {}
    for gwkey, htckey in job_translation.items():
        jobcmds[htckey] = getattr(gwjob, gwkey, None)

    # If accounting info was not set explicitly, use site settings if any.
    if not gwjob.accounting_group:
        jobcmds["accounting_group"] = cached_vals.get("accountingGroup")
    if not gwjob.accounting_user:
        jobcmds["accounting_group_user"] = cached_vals.get("accountingUser")

    # job commands that need modification
    if gwjob.retry_unless_exit:
        if isinstance(gwjob.retry_unless_exit, int):
            jobcmds["retry_until"] = f"{gwjob.retry_unless_exit}"
        elif isinstance(gwjob.retry_unless_exit, list):
            jobcmds["retry_until"] = (
                f"member(ExitCode, {{{','.join([str(x) for x in gwjob.retry_unless_exit])}}})"
            )
        else:
            raise ValueError("retryUnlessExit must be an integer or a list of integers.")

    if gwjob.request_disk:
        jobcmds["request_disk"] = f"{gwjob.request_disk}MB"

    if gwjob.request_memory:
        jobcmds["request_memory"] = f"{gwjob.request_memory}"

    memory_max = 0
    if gwjob.memory_multiplier:
        # Do not use try-except! At the moment, BpsConfig returns an empty
        # string if it does not contain the key.
        memory_limit = cached_vals["memoryLimit"]
        if not memory_limit:
            raise RuntimeError(
                "Memory autoscaling enabled, but automatic detection of the memory limit "
                "failed; setting it explicitly with 'memoryLimit' or changing worker node "
                "search pattern 'executeMachinesPattern' might help."
            )

        # Set maximal amount of memory job can ask for.
        #
        # The check below assumes that 'memory_limit' was set to a value which
        # realistically reflects actual physical limitations of a given compute
        # resource.
        memory_max = memory_limit
        if gwjob.request_memory_max and gwjob.request_memory_max < memory_limit:
            memory_max = gwjob.request_memory_max

        # Make job ask for more memory each time it failed due to insufficient
        # memory requirements.
        jobcmds["request_memory"] = _create_request_memory_expr(
            gwjob.request_memory, gwjob.memory_multiplier, memory_max
        )

    user_release_expr = cached_vals.get("releaseExpr", "")
    if gwjob.number_of_retries is not None and gwjob.number_of_retries >= 0:
        jobcmds["max_retries"] = gwjob.number_of_retries

        # No point in adding periodic_release if 0 retries
        if gwjob.number_of_retries > 0:
            periodic_release = _create_periodic_release_expr(
                gwjob.request_memory, gwjob.memory_multiplier, memory_max, user_release_expr
            )
            if periodic_release:
                jobcmds["periodic_release"] = periodic_release

        jobcmds["periodic_remove"] = _create_periodic_remove_expr(
            gwjob.request_memory, gwjob.memory_multiplier, memory_max
        )

    # Assume concurrency_limit implemented using HTCondor concurrency limits.
    # May need to move to special site-specific implementation if sites use
    # other mechanisms.
    if gwjob.concurrency_limit:
        jobcmds["concurrency_limit"] = gwjob.concurrency_limit

    # Handle command line
    if gwjob.executable.transfer_executable:
        jobcmds["transfer_executable"] = "True"
        jobcmds["executable"] = gwjob.executable.src_uri
    else:
        jobcmds["executable"] = _fix_env_var_syntax(gwjob.executable.src_uri)

    if gwjob.arguments:
        arguments = gwjob.arguments
        arguments = _replace_cmd_vars(arguments, gwjob)
        arguments = _replace_file_vars(cached_vals["bpsUseShared"], arguments, generic_workflow, gwjob)
        arguments = _fix_env_var_syntax(arguments)
        jobcmds["arguments"] = arguments

    if gwjob.environment:
        env_str = ""
        for name, value in gwjob.environment.items():
            if isinstance(value, str):
                value2 = _replace_cmd_vars(value, gwjob)
                value2 = _fix_env_var_syntax(value2)
                value2 = htc_escape(value2)
                env_str += f"{name}='{value2}' "  # Add single quotes to allow internal spaces
            else:
                env_str += f"{name}={value} "

        # Process above added one trailing space
        jobcmds["environment"] = env_str.rstrip()

    # Add extra "pass-thru" job commands
    if gwjob.profile:
        for key, val in gwjob.profile.items():
            jobcmds[key] = htc_escape(val)
    for key, val in cached_vals["profile"].items():
        jobcmds[key] = htc_escape(val)

    return jobcmds


def _translate_dag_cmds(gwjob):
    """Translate job values into DAGMan commands.

    Parameters
    ----------
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job containing values to be translated.

    Returns
    -------
    dagcmds : `dict` [`str`, `~typing.Any`]
        DAGMan commands for the job.
    """
    # Values in the dag script that just are name mappings.
    dag_translation = {"abort_on_value": "abort_dag_on", "abort_return_value": "abort_exit"}

    dagcmds = {}
    for gwkey, htckey in dag_translation.items():
        dagcmds[htckey] = getattr(gwjob, gwkey, None)

    # Still to be coded: vars "pre_cmdline", "post_cmdline"
    return dagcmds


def _fix_env_var_syntax(oldstr):
    """Change ENV place holders to HTCondor Env var syntax.

    Parameters
    ----------
    oldstr : `str`
        String in which environment variable syntax is to be fixed.

    Returns
    -------
    newstr : `str`
        Given string with environment variable syntax fixed.
    """
    newstr = oldstr
    for key in re.findall(r"<ENV:([^>]+)>", oldstr):
        newstr = newstr.replace(rf"<ENV:{key}>", f"$ENV({key})")
    return newstr


def _replace_file_vars(use_shared, arguments, workflow, gwjob):
    """Replace file placeholders in command line arguments with correct
    physical file names.

    Parameters
    ----------
    use_shared : `bool`
        Whether HTCondor can assume shared filesystem.
    arguments : `str`
        Arguments string in which to replace file placeholders.
    workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow that contains file information.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        The job corresponding to the arguments.

    Returns
    -------
    arguments : `str`
        Given arguments string with file placeholders replaced.
    """
    # Replace input file placeholders with paths.
    for gwfile in workflow.get_job_inputs(gwjob.name, data=True, transfer_only=False):
        if not gwfile.wms_transfer:
            # Must assume full URI if in command line and told WMS is not
            # responsible for transferring file.
            uri = gwfile.src_uri
        elif use_shared:
            if gwfile.job_shared:
                # Have shared filesystems and jobs can share file.
                uri = gwfile.src_uri
            else:
                uri = os.path.basename(gwfile.src_uri)
        else:  # Using push transfer
            uri = os.path.basename(gwfile.src_uri)
        arguments = arguments.replace(f"<FILE:{gwfile.name}>", uri)

    # Replace output file placeholders with paths.
    for gwfile in workflow.get_job_outputs(gwjob.name, data=True, transfer_only=False):
        if not gwfile.wms_transfer:
            # Must assume full URI if in command line and told WMS is not
            # responsible for transferring file.
            uri = gwfile.src_uri
        elif use_shared:
            if gwfile.job_shared:
                # Have shared filesystems and jobs can share file.
                uri = gwfile.src_uri
            else:
                uri = os.path.basename(gwfile.src_uri)
        else:  # Using push transfer
            uri = os.path.basename(gwfile.src_uri)
        arguments = arguments.replace(f"<FILE:{gwfile.name}>", uri)
    return arguments


def _replace_cmd_vars(arguments, gwjob):
    """Replace format-style placeholders in arguments.

    Parameters
    ----------
    arguments : `str`
        Arguments string in which to replace placeholders.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job containing values to be used to replace placeholders
        (in particular gwjob.cmdvals).

    Returns
    -------
    arguments : `str`
        Given arguments string with placeholders replaced.
    """
    replacements = gwjob.cmdvals if gwjob.cmdvals is not None else {}
    try:
        arguments = arguments.format(**replacements)
    except (KeyError, TypeError) as exc:  # TypeError in case None instead of {}
        _LOG.error("Could not replace command variables: replacement for %s not provided", str(exc))
        _LOG.debug("arguments: %s\ncmdvals: %s", arguments, replacements)
        raise
    return arguments


def _handle_job_inputs(
    generic_workflow: GenericWorkflow, job_name: str, use_shared: bool, out_prefix: str
) -> dict[str, str]:
    """Add job input files from generic workflow to job.

    Parameters
    ----------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow (e.g., has executable name and arguments).
    job_name : `str`
        Unique name for the job.
    use_shared : `bool`
        Whether job has access to files via shared filesystem.
    out_prefix : `str`
        The root directory into which all WMS-specific files are written.

    Returns
    -------
    htc_commands : `dict` [`str`, `str`]
        HTCondor commands for the job submission script.
    """
    inputs = []
    for gwf_file in generic_workflow.get_job_inputs(job_name, data=True, transfer_only=True):
        _LOG.debug("src_uri=%s", gwf_file.src_uri)

        uri = Path(gwf_file.src_uri)

        # Note if use_shared and job_shared, don't need to transfer file.

        if not use_shared:  # Copy file using push to job
            inputs.append(str(uri))
        elif not gwf_file.job_shared:  # Jobs require own copy
            # if using shared filesystem, but still need copy in job. Use
            # HTCondor's curl plugin for a local copy.
            if uri.is_dir():
                raise RuntimeError(
                    f"HTCondor plugin cannot transfer directories locally within job {gwf_file.src_uri}"
                )
            inputs.append(f"file://{uri}")

    htc_commands = {}
    if inputs:
        htc_commands["transfer_input_files"] = ",".join(inputs)
        _LOG.debug("transfer_input_files=%s", htc_commands["transfer_input_files"])
    return htc_commands


def _handle_job_outputs(
    generic_workflow: GenericWorkflow, job_name: str, use_shared: bool, out_prefix: str
) -> dict[str, str]:
    """Add job output files from generic workflow to the job if any.

    Parameters
    ----------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The generic workflow (e.g., has executable name and arguments).
    job_name : `str`
        Unique name for the job.
    use_shared : `bool`
        Whether job has access to files via shared filesystem.
    out_prefix : `str`
        The root directory into which all WMS-specific files are written.

    Returns
    -------
    htc_commands : `dict` [`str`, `str`]
        HTCondor commands for the job submission script.
    """
    outputs = []
    output_remaps = []
    for gwf_file in generic_workflow.get_job_outputs(job_name, data=True, transfer_only=True):
        _LOG.debug("src_uri=%s", gwf_file.src_uri)

        uri = Path(gwf_file.src_uri)
        if not use_shared:
            outputs.append(uri.name)
            output_remaps.append(f"{uri.name}={str(uri)}")

    # Set to an empty string to disable and only update if there are output
    # files to transfer. Otherwise, HTCondor will transfer back all files in
    # the job’s temporary working directory that have been modified or created
    # by the job.
    htc_commands = {"transfer_output_files": '""'}
    if outputs:
        htc_commands["transfer_output_files"] = ",".join(outputs)
        _LOG.debug("transfer_output_files=%s", htc_commands["transfer_output_files"])

        htc_commands["transfer_output_remaps"] = f'"{";".join(output_remaps)}"'
        _LOG.debug("transfer_output_remaps=%s", htc_commands["transfer_output_remaps"])
    return htc_commands


def _create_periodic_release_expr(
    memory: int, multiplier: float | None, limit: int, additional_expr: str = ""
) -> str:
    """Construct an HTCondorAd expression for releasing held jobs.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float` or None
        Memory growth rate between retries.
    limit : `int`
        Memory limit.
    additional_expr : `str`, optional
        Expression to add to periodic_release.  Defaults to empty string.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression for releasing job.
    """
    _LOG.debug(
        "periodic_release: memory: %s, multiplier: %s, limit: %s, additional_expr: %s",
        memory,
        multiplier,
        limit,
        additional_expr,
    )

    # ctrl_bps sets multiplier to None in the GenericWorkflow if
    # memoryMultiplier <= 1, but checking value just in case.
    if (not multiplier or multiplier <= 1) and not additional_expr:
        return ""

    # Job ClassAds attributes 'HoldReasonCode' and 'HoldReasonSubCode' are
    # UNDEFINED if job is not HELD (i.e. when 'JobStatus' is not 5).
    # The special comparison operators ensure that all comparisons below will
    # evaluate to FALSE in this case.
    #
    # Note:
    # May not be strictly necessary. Operators '&&' and '||' are not strict so
    # the entire expression should evaluate to FALSE when the job is not HELD.
    # According to ClassAd evaluation semantics FALSE && UNDEFINED is FALSE,
    # but better safe than sorry.
    is_held = "JobStatus == 5"
    is_retry_allowed = "NumJobStarts <= JobMaxRetries"

    mem_expr = ""
    if memory and multiplier and multiplier > 1 and limit:
        was_mem_exceeded = (
            "(HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 "
            "|| HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34)"
        )
        was_below_limit = f"min({{int({memory} * pow({multiplier}, NumJobStarts - 1)), {limit}}}) < {limit}"
        mem_expr = f"{was_mem_exceeded} && {was_below_limit}"

    user_expr = ""
    if additional_expr:
        # Never auto release a job held by user.
        user_expr = f"HoldReasonCode =!= 1 && {additional_expr}"

    expr = f"{is_held} && {is_retry_allowed}"
    if user_expr and mem_expr:
        expr += f" && ({mem_expr} || {user_expr})"
    elif user_expr:
        expr += f" && {user_expr}"
    elif mem_expr:
        expr += f" && {mem_expr}"

    return expr


def _create_periodic_remove_expr(memory, multiplier, limit):
    """Construct an HTCondorAd expression for removing jobs from the queue.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float`
        Memory growth rate between retries.
    limit : `int`
        Memory limit.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression for removing jobs.
    """
    # Job ClassAds attributes 'HoldReasonCode' and 'HoldReasonSubCode'
    # are UNDEFINED if job is not HELD (i.e. when 'JobStatus' is not 5).
    # The special comparison operators ensure that all comparisons below
    # will evaluate to FALSE in this case.
    #
    # Note:
    # May not be strictly necessary. Operators '&&' and '||' are not
    # strict so the entire expression should evaluate to FALSE when the
    # job is not HELD. According to ClassAd evaluation semantics
    # FALSE && UNDEFINED is FALSE, but better safe than sorry.
    is_held = "JobStatus == 5"
    is_retry_disallowed = "NumJobStarts > JobMaxRetries"

    mem_expr = ""
    if memory and multiplier and multiplier > 1 and limit:
        mem_limit_expr = f"min({{int({memory} * pow({multiplier}, NumJobStarts - 1)), {limit}}}) == {limit}"

        mem_expr = (  # Add || here so only added if adding memory expr
            " || ((HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 "
            f"|| HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34) && {mem_limit_expr})"
        )

    expr = f"{is_held} && ({is_retry_disallowed}{mem_expr})"
    return expr


def _create_request_memory_expr(memory, multiplier, limit):
    """Construct an HTCondor ClassAd expression for safe memory scaling.

    Parameters
    ----------
    memory : `int`
        Requested memory in MB.
    multiplier : `float`
        Memory growth rate between retries.
    limit : `int`
        Memory limit.

    Returns
    -------
    expr : `str`
        A string representing an HTCondor ClassAd expression enabling safe
        memory scaling between job retries.
    """
    # The check if the job was held due to exceeding memory requirements
    # will be made *after* job was released back to the job queue (is in
    # the IDLE state), hence the need to use `Last*` job ClassAds instead of
    # the ones describing job's current state.
    #
    # Also, 'Last*' job ClassAds attributes are UNDEFINED when a job is
    # initially put in the job queue. The special comparison operators ensure
    # that all comparisons below will evaluate to FALSE in this case.
    was_mem_exceeded = (
        "LastJobStatus =?= 5 "
        "&& (LastHoldReasonCode =?= 34 && LastHoldReasonSubCode =?= 0 "
        "|| LastHoldReasonCode =?= 3 && LastHoldReasonSubCode =?= 34)"
    )

    # If job runs the first time or was held for reasons other than exceeding
    # the memory, set the required memory to the requested value or use
    # the memory value measured by HTCondor (MemoryUsage) depending on
    # whichever is greater.
    expr = (
        f"({was_mem_exceeded}) "
        f"? min({{int({memory} * pow({multiplier}, NumJobStarts)), {limit}}}) "
        f": max({{{memory}, MemoryUsage ?: 0}})"
    )
    return expr


def _gather_site_values(config, compute_site):
    """Gather values specific to given site.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime
        information.
    compute_site : `str`
        Compute site name.

    Returns
    -------
    site_values : `dict` [`str`, `~typing.Any`]
        Values specific to the given site.
    """
    site_values = {"attrs": {}, "profile": {}}
    search_opts = {}
    if compute_site:
        search_opts["curvals"] = {"curr_site": compute_site}

    # Determine the hard limit for the memory requirement.
    found, limit = config.search("memoryLimit", opt=search_opts)
    if not found:
        search_opts["default"] = DEFAULT_HTC_EXEC_PATT
        _, patt = config.search("executeMachinesPattern", opt=search_opts)
        del search_opts["default"]

        # To reduce the amount of data, ignore dynamic slots (if any) as,
        # by definition, they cannot have more memory than
        # the partitionable slot they are the part of.
        constraint = f'SlotType != "Dynamic" && regexp("{patt}", Machine)'
        pool_info = condor_status(constraint=constraint)
        try:
            limit = max(int(info["TotalSlotMemory"]) for info in pool_info.values())
        except ValueError:
            _LOG.debug("No execute machine in the pool matches %s", patt)
    if limit:
        config[".bps_defined.memory_limit"] = limit

    _, site_values["bpsUseShared"] = config.search("bpsUseShared", opt={"default": False})
    site_values["memoryLimit"] = limit

    found, value = config.search("accountingGroup", opt=search_opts)
    if found:
        site_values["accountingGroup"] = value
    found, value = config.search("accountingUser", opt=search_opts)
    if found:
        site_values["accountingUser"] = value

    key = f".site.{compute_site}.profile.condor"
    if key in config:
        for subkey, val in config[key].items():
            if subkey.startswith("+"):
                site_values["attrs"][subkey[1:]] = val
            else:
                site_values["profile"][subkey] = val

    return site_values


def _gather_label_values(config: BpsConfig, label: str) -> dict[str, Any]:
    """Gather values specific to given job label.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime
        information.
    label : `str`
        GenericWorkflowJob label.

    Returns
    -------
    values : `dict` [`str`, `~typing.Any`]
        Values specific to the given job label.
    """
    values: dict[str, Any] = {"attrs": {}, "profile": {}}

    search_opts = {}
    profile_key = ""
    if label == "finalJob":
        search_opts["searchobj"] = config["finalJob"]
        profile_key = ".finalJob.profile.condor"
    elif label in config["cluster"]:
        search_opts["curvals"] = {"curr_cluster": label}
        profile_key = f".cluster.{label}.profile.condor"
    elif label in config["pipetask"]:
        search_opts["curvals"] = {"curr_pipetask": label}
        profile_key = f".pipetask.{label}.profile.condor"

    found, value = config.search("releaseExpr", opt=search_opts)
    if found:
        values["releaseExpr"] = value

    found, value = config.search("overwriteJobFiles", opt=search_opts)
    if found:
        values["overwriteJobFiles"] = value
    else:
        values["overwriteJobFiles"] = True

    if profile_key and profile_key in config:
        for subkey, val in config[profile_key].items():
            if subkey.startswith("+"):
                values["attrs"][subkey[1:]] = val
            else:
                values["profile"][subkey] = val

    return values


def _group_to_subdag(
    config: BpsConfig, generic_workflow_group: GenericWorkflowGroup, out_prefix: str
) -> HTCJob:
    """Convert a generic workflow group to an HTCondor dag.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Workflow configuration.
    generic_workflow_group : `lsst.ctrl.bps.GenericWorkflowGroup`
        The generic workflow group to convert.
    out_prefix : `str`
        Location prefix to be used when creating jobs.

    Returns
    -------
    htc_job : `lsst.ctrl.bps.htcondor.HTCJob`
        Job for running the HTCondor dag.
    """
    jobname = f"wms_{generic_workflow_group.name}"
    htc_job = HTCJob(name=jobname, label=generic_workflow_group.label)
    htc_job.add_dag_cmds({"dir": f"subdags/{jobname}"})
    htc_job.subdag = _generic_workflow_to_htcondor_dag(config, generic_workflow_group, out_prefix)
    if not generic_workflow_group.blocking:
        htc_job.dagcmds["post"] = {
            "defer": "",
            "executable": f"{os.path.dirname(__file__)}/subdag_post.sh",
            "arguments": f"{jobname} $RETURN",
        }
    return htc_job


def _create_check_job(group_job_name: str, job_label: str) -> HTCJob:
    """Create a job to check status of a group job.

    Parameters
    ----------
    group_job_name : `str`
        Name of the group job.
    job_label : `str`
        Label to use for the check status job.

    Returns
    -------
    htc_job : `lsst.ctrl.bps.htcondor.HTCJob`
        Job description for the job to check group job status.
    """
    htc_job = HTCJob(name=f"wms_check_status_{group_job_name}", label=job_label)
    htc_job.subfile = "${CTRL_BPS_HTCONDOR_DIR}/python/lsst/ctrl/bps/htcondor/check_group_status.sub"
    htc_job.add_dag_cmds({"dir": f"subdags/{group_job_name}", "vars": {"group_job_name": group_job_name}})

    return htc_job


def _generic_workflow_to_htcondor_dag(
    config: BpsConfig, generic_workflow: GenericWorkflow, out_prefix: str
) -> HTCDag:
    """Convert a GenericWorkflow to a HTCDag.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Workflow configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        The GenericWorkflow to convert.
    out_prefix : `str`
        Location prefix where the HTCondor files will be written.

    Returns
    -------
    dag : `lsst.ctrl.bps.htcondor.HTCDag`
        The HTCDag representation of the given GenericWorkflow.
    """
    dag = HTCDag(name=generic_workflow.name)

    _LOG.debug("htcondor dag attribs %s", generic_workflow.run_attrs)
    dag.add_attribs(generic_workflow.run_attrs)
    dag.add_attribs(
        {
            "bps_run_quanta": create_count_summary(generic_workflow.quanta_counts),
            "bps_job_summary": create_count_summary(generic_workflow.job_counts),
        }
    )

    _, tmp_template = config.search("subDirTemplate", opt={"replaceVars": False, "default": ""})
    if isinstance(tmp_template, str):
        subdir_template = defaultdict(lambda: tmp_template)
    else:
        subdir_template = tmp_template

    # Create all DAG jobs
    site_values = {}  # Cache compute site specific values to reduce config lookups.
    cached_values = {}  # Cache label-specific values to reduce config lookups.
    # Note: Can't use get_job_by_label because those only include payload jobs.
    for job_name in generic_workflow:
        gwjob = generic_workflow.get_job(job_name)
        if gwjob.node_type == GenericWorkflowNodeType.PAYLOAD:
            gwjob = cast(GenericWorkflowJob, gwjob)
            if gwjob.compute_site not in site_values:
                site_values[gwjob.compute_site] = _gather_site_values(config, gwjob.compute_site)
            if gwjob.label not in cached_values:
                cached_values[gwjob.label] = deepcopy(site_values[gwjob.compute_site])
                cached_values[gwjob.label].update(_gather_label_values(config, gwjob.label))
                _LOG.debug("cached: %s= %s", gwjob.label, cached_values[gwjob.label])
            htc_job = _create_job(
                subdir_template[gwjob.label],
                cached_values[gwjob.label],
                generic_workflow,
                gwjob,
                out_prefix,
            )
        elif gwjob.node_type == GenericWorkflowNodeType.NOOP:
            gwjob = cast(GenericWorkflowNoopJob, gwjob)
            htc_job = HTCJob(f"wms_{gwjob.name}", label=gwjob.label)
            htc_job.subfile = "${CTRL_BPS_HTCONDOR_DIR}/python/lsst/ctrl/bps/htcondor/noop.sub"
            htc_job.add_job_attrs({"bps_job_name": gwjob.name, "bps_job_label": gwjob.label})
            htc_job.add_dag_cmds({"noop": True})
        elif gwjob.node_type == GenericWorkflowNodeType.GROUP:
            gwjob = cast(GenericWorkflowGroup, gwjob)
            htc_job = _group_to_subdag(config, gwjob, out_prefix)
            # In case DAGMAN_GENERATE_SUBDAG_SUBMITS is False,
            dag.graph["submit_options"]["do_recurse"] = True
        else:
            raise RuntimeError(f"Unsupported generic workflow node type {gwjob.node_type} ({gwjob.name})")
        _LOG.debug("Calling adding job %s %s", htc_job.name, htc_job.label)
        dag.add_job(htc_job)

    # Add job dependencies to the DAG (be careful with wms_ jobs)
    for job_name in generic_workflow:
        gwjob = generic_workflow.get_job(job_name)
        parent_name = (
            gwjob.name if gwjob.node_type == GenericWorkflowNodeType.PAYLOAD else f"wms_{gwjob.name}"
        )
        successor_jobs = [generic_workflow.get_job(j) for j in generic_workflow.successors(job_name)]
        children_names = []
        if gwjob.node_type == GenericWorkflowNodeType.GROUP:
            gwjob = cast(GenericWorkflowGroup, gwjob)
            group_children = []  # Dependencies between same group jobs
            for sjob in successor_jobs:
                if sjob.node_type == GenericWorkflowNodeType.GROUP and sjob.label == gwjob.label:
                    group_children.append(f"wms_{sjob.name}")
                elif sjob.node_type == GenericWorkflowNodeType.PAYLOAD:
                    children_names.append(sjob.name)
                else:
                    children_names.append(f"wms_{sjob.name}")
            if group_children:
                dag.add_job_relationships([parent_name], group_children)
            if not gwjob.blocking:
                # Since subdag will always succeed, need to add a special
                # job that fails if group failed to block payload children.
                check_job = _create_check_job(f"wms_{gwjob.name}", gwjob.label)
                dag.add_job(check_job)
                dag.add_job_relationships([f"wms_{gwjob.name}"], [check_job.name])
                parent_name = check_job.name
        else:
            for sjob in successor_jobs:
                if sjob.node_type == GenericWorkflowNodeType.PAYLOAD:
                    children_names.append(sjob.name)
                else:
                    children_names.append(f"wms_{sjob.name}")

        dag.add_job_relationships([parent_name], children_names)

    # If final job exists in generic workflow, create DAG final job
    final = generic_workflow.get_final()
    if final and isinstance(final, GenericWorkflowJob):
        if final.compute_site and final.compute_site not in site_values:
            site_values[final.compute_site] = _gather_site_values(config, final.compute_site)
        if final.label not in cached_values:
            cached_values[final.label] = deepcopy(site_values[final.compute_site])
            cached_values[final.label].update(_gather_label_values(config, final.label))
        final_htjob = _create_job(
            subdir_template[final.label],
            cached_values[final.label],
            generic_workflow,
            final,
            out_prefix,
        )
        if "post" not in final_htjob.dagcmds:
            final_htjob.dagcmds["post"] = {
                "defer": "",
                "executable": f"{os.path.dirname(__file__)}/final_post.sh",
                "arguments": f"{final.name} $DAG_STATUS $RETURN",
            }
        dag.add_final_job(final_htjob)
    elif final and isinstance(final, GenericWorkflow):
        raise NotImplementedError("HTCondor plugin does not support a workflow as the final job")
    elif final:
        raise TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final)})")

    return dag
