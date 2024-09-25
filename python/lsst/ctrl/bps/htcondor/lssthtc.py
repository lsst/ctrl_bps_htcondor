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

"""Placeholder HTCondor DAGMan API.

There is new work on a python DAGMan API from HTCondor.  However, at this
time, it tries to make things easier by assuming DAG is easily broken into
levels where there are 1-1 or all-to-all relationships to nodes in next
level.  LSST workflows are more complicated.
"""

__all__ = [
    "DagStatus",
    "JobStatus",
    "NodeStatus",
    "RestrictedDict",
    "HTCJob",
    "HTCDag",
    "htc_backup_files",
    "htc_check_dagman_output",
    "htc_create_submit_from_cmd",
    "htc_create_submit_from_dag",
    "htc_create_submit_from_file",
    "htc_escape",
    "htc_write_attribs",
    "htc_write_condor_file",
    "htc_query_history",
    "htc_query_present",
    "htc_version",
    "htc_submit_dag",
    "condor_history",
    "condor_q",
    "condor_search",
    "condor_status",
    "update_job_info",
    "MISSING_ID",
    "summary_from_dag",
    "read_dag_info",
    "read_dag_log",
    "read_dag_nodes_log",
    "read_dag_status",
    "read_node_status",
    "write_dag_info",
    "pegasus_name_to_label",
]


import itertools
import json
import logging
import os
import pprint
import re
import subprocess
from collections import defaultdict
from collections.abc import MutableMapping
from datetime import datetime, timedelta
from enum import IntEnum
from pathlib import Path
from typing import Any

import classad
import htcondor
import networkx
from packaging import version

from .handlers import HTC_JOB_AD_HANDLERS

_LOG = logging.getLogger(__name__)

MISSING_ID = -99999


class DagStatus(IntEnum):
    """HTCondor DAGMan's statuses for a DAG."""

    OK = 0
    ERROR = 1  # an error condition different than those listed here
    FAILED = 2  # one or more nodes in the DAG have failed
    ABORTED = 3  # the DAG has been aborted by an ABORT-DAG-ON specification
    REMOVED = 4  # the DAG has been removed by condor_rm
    CYCLE = 5  # a cycle was found in the DAG
    SUSPENDED = 6  # the DAG has been suspended (see section 2.10.8)


class JobStatus(IntEnum):
    """HTCondor's statuses for jobs."""

    UNEXPANDED = 0  # Unexpanded
    IDLE = 1  # Idle
    RUNNING = 2  # Running
    REMOVED = 3  # Removed
    COMPLETED = 4  # Completed
    HELD = 5  # Held
    TRANSFERRING_OUTPUT = 6  # Transferring_Output
    SUSPENDED = 7  # Suspended


class NodeStatus(IntEnum):
    """HTCondor's statuses for DAGman nodes."""

    # (STATUS_NOT_READY): At least one parent has not yet finished or the node
    # is a FINAL node.
    NOT_READY = 0

    # (STATUS_READY): All parents have finished, but the node is not yet
    # running.
    READY = 1

    # (STATUS_PRERUN): The node’s PRE script is running.
    PRERUN = 2

    # (STATUS_SUBMITTED): The node’s HTCondor job(s) are in the queue.
    #                     StatusDetails = "not_idle" -> running.
    #                     JobProcsHeld = 1-> hold.
    #                     JobProcsQueued = 1 -> idle.
    SUBMITTED = 3

    # (STATUS_POSTRUN): The node’s POST script is running.
    POSTRUN = 4

    # (STATUS_DONE): The node has completed successfully.
    DONE = 5

    # (STATUS_ERROR): The node has failed. StatusDetails has info (e.g.,
    # ULOG_JOB_ABORTED for deleted job).
    ERROR = 6

    # (STATUS_FUTILE): The node will never run because ancestor node failed.
    FUTILE = 7


HTC_QUOTE_KEYS = {"environment"}
HTC_VALID_JOB_KEYS = {
    "universe",
    "executable",
    "arguments",
    "environment",
    "log",
    "error",
    "output",
    "should_transfer_files",
    "when_to_transfer_output",
    "getenv",
    "notification",
    "notify_user",
    "concurrency_limit",
    "transfer_executable",
    "transfer_input_files",
    "transfer_output_files",
    "request_cpus",
    "request_memory",
    "request_disk",
    "priority",
    "category",
    "requirements",
    "on_exit_hold",
    "on_exit_hold_reason",
    "on_exit_hold_subcode",
    "max_retries",
    "retry_until",
    "periodic_release",
    "periodic_remove",
    "accounting_group",
    "accounting_group_user",
}
HTC_VALID_JOB_DAG_KEYS = {"vars", "pre", "post", "retry", "retry_unless_exit", "abort_dag_on", "abort_exit"}
HTC_VERSION = version.parse(htcondor.__version__)


class RestrictedDict(MutableMapping):
    """A dictionary that only allows certain keys.

    Parameters
    ----------
    valid_keys : `Container`
        Strings that are valid keys.
    init_data : `dict` or `RestrictedDict`, optional
        Initial data.

    Raises
    ------
    KeyError
        If invalid key(s) in init_data.
    """

    def __init__(self, valid_keys, init_data=()):
        self.valid_keys = valid_keys
        self.data = {}
        self.update(init_data)

    def __getitem__(self, key):
        """Return value for given key if exists.

        Parameters
        ----------
        key : `str`
            Identifier for value to return.

        Returns
        -------
        value : `~collections.abc.Any`
            Value associated with given key.

        Raises
        ------
        KeyError
            If key doesn't exist.
        """
        return self.data[key]

    def __delitem__(self, key):
        """Delete value for given key if exists.

        Parameters
        ----------
        key : `str`
            Identifier for value to delete.

        Raises
        ------
        KeyError
            If key doesn't exist.
        """
        del self.data[key]

    def __setitem__(self, key, value):
        """Store key,value in internal dict only if key is valid.

        Parameters
        ----------
        key : `str`
            Identifier to associate with given value.
        value : `~collections.abc.Any`
            Value to store.

        Raises
        ------
        KeyError
            If key is invalid.
        """
        if key not in self.valid_keys:
            raise KeyError(f"Invalid key {key}")
        self.data[key] = value

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return len(self.data)

    def __str__(self):
        return str(self.data)


def htc_backup_files(wms_path, subdir=None, limit=100):
    """Backup select HTCondor files in the submit directory.

    Files will be saved in separate subdirectories which will be created in
    the submit directory where the files are located. These subdirectories
    will be consecutive, zero-padded integers. Their values will correspond to
    the number of HTCondor rescue DAGs in the submit directory.

    Hence, with the default settings, copies after the initial failed run will
    be placed in '001' subdirectory, '002' after the first restart, and so on
    until the limit of backups is reached. If there's no rescue DAG yet, files
    will be copied to '000' subdirectory.

    Parameters
    ----------
    wms_path : `str` or `pathlib.Path`
        Path to the submit directory either absolute or relative.
    subdir : `str` or `pathlib.Path`, optional
        A path, relative to the submit directory, where all subdirectories with
        backup files will be kept. Defaults to None which means that the backup
        subdirectories will be placed directly in the submit directory.
    limit : `int`, optional
        Maximal number of backups. If the number of backups reaches the limit,
        the last backup files will be overwritten. The default value is 100
        to match the default value of HTCondor's DAGMAN_MAX_RESCUE_NUM in
        version 8.8+.

    Raises
    ------
    FileNotFoundError
        If the submit directory or the file that needs to be backed up does not
        exist.
    OSError
        If the submit directory cannot be accessed or backing up a file failed
        either due to permission or filesystem related issues.

    Notes
    -----
    This is not a generic function for making backups. It is intended to be
    used once, just before a restart, to make snapshots of files which will be
    overwritten by HTCondor after during the next run.
    """
    width = len(str(limit))

    path = Path(wms_path).resolve()
    if not path.is_dir():
        raise FileNotFoundError(f"Directory {path} not found")

    # Initialize the backup counter.
    rescue_dags = list(Path(wms_path).glob("*.rescue*"))
    counter = min(len(rescue_dags), limit)

    # Create the backup directory and move select files there.
    dest = Path(wms_path)
    if subdir:
        # PurePath.is_relative_to() is not available before Python 3.9. Hence
        # we need to check is 'subdir' is in the submit directory in some other
        # way if it is an absolute path.
        subdir = Path(subdir)
        if subdir.is_absolute():
            if dest not in subdir.parents:
                _LOG.warning(
                    "Invalid backup location: '%s' not in the submit directory, will use '%s' instead.",
                    subdir,
                    wms_path,
                )
            else:
                dest /= subdir
        else:
            dest /= subdir
    dest /= f"{counter:0{width}}"
    try:
        dest.mkdir(parents=True, exist_ok=False if counter < limit else True)
    except FileExistsError:
        _LOG.warning("Refusing to do backups: target directory '%s' already exists", dest)
    else:
        for patt in ["*.info.*", "*.dag.metrics", "*.dag.nodes.log", "*.node_status"]:
            for source in path.glob(patt):
                if source.is_file():
                    target = dest / source.relative_to(path)
                    try:
                        source.rename(target)
                    except OSError as exc:
                        raise type(exc)(f"Backing up '{source}' failed: {exc.strerror}") from None
                else:
                    raise FileNotFoundError(f"Backing up '{source}' failed: not a file")


def htc_escape(value):
    """Escape characters in given value based upon HTCondor syntax.

    Parameters
    ----------
    value : `~collections.abc.Any`
        Value that needs to have characters escaped if string.

    Returns
    -------
    new_value : `~collections.abc.Any`
        Given value with characters escaped appropriate for HTCondor if string.
    """
    if isinstance(value, str):
        newval = value.replace('"', '""').replace("'", "''").replace("&quot;", '"')
    else:
        newval = value

    return newval


def htc_write_attribs(stream, attrs):
    """Write job attributes in HTCondor format to writeable stream.

    Parameters
    ----------
    stream : `~io.TextIOBase`
        Output text stream (typically an open file).
    attrs : `dict`
        HTCondor job attributes (dictionary of attribute key, value).
    """
    for key, value in attrs.items():
        # Make sure strings are syntactically correct for HTCondor.
        if isinstance(value, str):
            pval = f'"{htc_escape(value)}"'
        else:
            pval = value

        print(f"+{key} = {pval}", file=stream)


def htc_write_condor_file(filename, job_name, job, job_attrs):
    """Write an HTCondor submit file.

    Parameters
    ----------
    filename : `str`
        Filename for the HTCondor submit file.
    job_name : `str`
        Job name to use in submit file.
    job : `RestrictedDict`
        Submit script information.
    job_attrs : `dict`
        Job attributes.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as fh:
        for key, value in job.items():
            if value is not None:
                if key in HTC_QUOTE_KEYS:
                    print(f'{key}="{htc_escape(value)}"', file=fh)
                else:
                    print(f"{key}={value}", file=fh)
        for key in ["output", "error", "log"]:
            if key not in job:
                filename = f"{job_name}.$(Cluster).${key[:3]}"
                print(f"{key}={filename}", file=fh)

        if job_attrs is not None:
            htc_write_attribs(fh, job_attrs)
        print("queue", file=fh)


# To avoid doing the version check during every function call select
# appropriate conversion function at the import time.
#
# Make sure that *each* version specific variant of the conversion function(s)
# has the same signature after applying any changes!
if HTC_VERSION < version.parse("8.9.8"):

    def htc_tune_schedd_args(**kwargs):
        """Ensure that arguments for Schedd are version appropriate.

        The old arguments: 'requirements' and 'attr_list' of
        'Schedd.history()', 'Schedd.query()', and 'Schedd.xquery()' were
        deprecated in favor of 'constraint' and 'projection', respectively,
        starting from version 8.9.8.  The function will convert "new" keyword
        arguments to "old" ones.

        Parameters
        ----------
        **kwargs
            Any keyword arguments that Schedd.history(), Schedd.query(), and
            Schedd.xquery() accepts.

        Returns
        -------
        kwargs : `dict` [`str`, Any]
            Keywords arguments that are guaranteed to work with the Python
            HTCondor API.

        Notes
        -----
        Function doesn't validate provided keyword arguments beyond converting
        selected arguments to their version specific form. For example,
        it won't remove keywords that are not supported by the methods
        mentioned earlier.
        """
        translation_table = {
            "constraint": "requirements",
            "projection": "attr_list",
        }
        for new, old in translation_table.items():
            try:
                kwargs[old] = kwargs.pop(new)
            except KeyError:
                pass
        return kwargs

else:

    def htc_tune_schedd_args(**kwargs):
        """Ensure that arguments for Schedd are version appropriate.

        This is the fallback function if no version specific alteration are
        necessary. Effectively, a no-op.

        Parameters
        ----------
        **kwargs
            Any keyword arguments that Schedd.history(), Schedd.query(), and
            Schedd.xquery() accepts.

        Returns
        -------
        kwargs : `dict` [`str`, Any]
            Keywords arguments that were passed to the function.
        """
        return kwargs


def htc_query_history(schedds, **kwargs):
    """Fetch history records from the condor_schedd daemon.

    Parameters
    ----------
    schedds : `htcondor.Schedd`
        HTCondor schedulers which to query for job information.
    **kwargs
        Any keyword arguments that Schedd.history() accepts.

    Yields
    ------
    schedd_name : `str`
        Name of the HTCondor scheduler managing the job queue.
    job_ad : `dict` [`str`, Any]
        A dictionary representing HTCondor ClassAd describing a job. It maps
        job attributes names to values of the ClassAd expressions they
        represent.
    """
    # If not set, provide defaults for positional arguments.
    kwargs.setdefault("constraint", None)
    kwargs.setdefault("projection", [])
    kwargs = htc_tune_schedd_args(**kwargs)
    for schedd_name, schedd in schedds.items():
        for job_ad in schedd.history(**kwargs):
            yield schedd_name, dict(job_ad)


def htc_query_present(schedds, **kwargs):
    """Query the condor_schedd daemon for job ads.

    Parameters
    ----------
    schedds : `htcondor.Schedd`
        HTCondor schedulers which to query for job information.
    **kwargs
        Any keyword arguments that Schedd.xquery() accepts.

    Yields
    ------
    schedd_name : `str`
        Name of the HTCondor scheduler managing the job queue.
    job_ad : `dict` [`str`, Any]
        A dictionary representing HTCondor ClassAd describing a job. It maps
        job attributes names to values of the ClassAd expressions they
        represent.
    """
    kwargs = htc_tune_schedd_args(**kwargs)
    for schedd_name, schedd in schedds.items():
        for job_ad in schedd.query(**kwargs):
            yield schedd_name, dict(job_ad)


def htc_version():
    """Return the version given by the HTCondor API.

    Returns
    -------
    version : `str`
        HTCondor version as easily comparable string.
    """
    return str(HTC_VERSION)


def htc_submit_dag(sub):
    """Submit job for execution.

    Parameters
    ----------
    sub : `htcondor.Submit`
        An object representing a job submit description.

    Returns
    -------
    schedd_job_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    coll = htcondor.Collector()
    schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
    schedd = htcondor.Schedd(schedd_ad)

    # If Schedd.submit() fails, the method will raise an exception. Usually,
    # that implies issues with the HTCondor pool which BPS can't address.
    # Hence, no effort is made to handle the exception.
    submit_result = schedd.submit(sub)

    # Sadly, the ClassAd from Schedd.submit() (see above) does not have
    # 'GlobalJobId' so we need to run a regular query to get it anyway.
    schedd_name = schedd_ad["Name"]
    schedd_dag_info = condor_q(
        constraint=f"ClusterId == {submit_result.cluster()}", schedds={schedd_name: schedd}
    )
    return schedd_dag_info


def htc_create_submit_from_dag(dag_filename, submit_options=None):
    """Create a DAGMan job submit description.

    Parameters
    ----------
    dag_filename : `str`
        Name of file containing HTCondor DAG commands.
    submit_options : `dict` [`str`, Any], optional
        Contains extra options for command line (Value of None means flag).

    Returns
    -------
    sub : `htcondor.Submit`
        An object representing a job submit description.

    Notes
    -----
    Use with HTCondor versions which support htcondor.Submit.from_dag(),
    i.e., 8.9.3 or newer.
    """
    return htcondor.Submit.from_dag(dag_filename, submit_options)


def htc_create_submit_from_cmd(dag_filename, submit_options=None):
    """Create a DAGMan job submit description.

    Create a DAGMan job submit description by calling ``condor_submit_dag``
    on given DAG description file.

    Parameters
    ----------
    dag_filename : `str`
        Name of file containing HTCondor DAG commands.
    submit_options : `dict` [`str`, Any], optional
        Contains extra options for command line (Value of None means flag).

    Returns
    -------
    sub : `htcondor.Submit`
        An object representing a job submit description.

    Notes
    -----
    Use with HTCondor versions which do not support htcondor.Submit.from_dag(),
    i.e., older than 8.9.3.
    """
    # Run command line condor_submit_dag command.
    cmd = "condor_submit_dag -f -no_submit -notification never -autorescue 1 -UseDagDir -no_recurse "

    if submit_options is not None:
        for opt, val in submit_options.items():
            cmd += f" -{opt} {val or ''}"
    cmd += f"{dag_filename}"

    process = subprocess.Popen(
        cmd.split(), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8"
    )
    process.wait()

    if process.returncode != 0:
        print(f"Exit code: {process.returncode}")
        print(process.communicate()[0])
        raise RuntimeError("Problems running condor_submit_dag")

    return htc_create_submit_from_file(f"{dag_filename}.condor.sub")


def htc_create_submit_from_file(submit_file):
    """Parse a submission file.

    Parameters
    ----------
    submit_file : `str`
        Name of the HTCondor submit file.

    Returns
    -------
    sub : `htcondor.Submit`
        An object representing a job submit description.
    """
    descriptors = {}
    with open(submit_file) as fh:
        for line in fh:
            line = line.strip()
            if not line.startswith("#") and not line == "queue":
                (key, val) = re.split(r"\s*=\s*", line, 1)
                descriptors[key] = val

    # Avoid UserWarning: the line 'copy_to_spool = False' was
    #       unused by Submit object. Is it a typo?
    try:
        del descriptors["copy_to_spool"]
    except KeyError:
        pass

    return htcondor.Submit(descriptors)


def _htc_write_job_commands(stream, name, jobs):
    """Output the DAGMan job lines for single job in DAG.

    Parameters
    ----------
    stream : `~io.TextIOBase`
        Writeable text stream (typically an opened file).
    name : `str`
        Job name.
    jobs : `RestrictedDict`
        DAG job keys and values.
    """
    if "pre" in jobs:
        print(
            f"SCRIPT {jobs['pre'].get('defer', '')} PRE {name}"
            f"{jobs['pre']['executable']} {jobs['pre'].get('arguments', '')}",
            file=stream,
        )

    if "post" in jobs:
        print(
            f"SCRIPT {jobs['post'].get('defer', '')} PRE {name}"
            f"{jobs['post']['executable']} {jobs['post'].get('arguments', '')}",
            file=stream,
        )

    if "vars" in jobs:
        for key, value in jobs["vars"]:
            print(f'VARS {name} {key}="{htc_escape(value)}"', file=stream)

    if "pre_skip" in jobs:
        print(f"PRE_SKIP {name} {jobs['pre_skip']}", file=stream)

    if "retry" in jobs and jobs["retry"]:
        print(f"RETRY {name} {jobs['retry']} ", end="", file=stream)
        if "retry_unless_exit" in jobs:
            print(f"UNLESS-EXIT {jobs['retry_unless_exit']}", end="", file=stream)
        print("\n", file=stream)

    if "abort_dag_on" in jobs and jobs["abort_dag_on"]:
        print(
            f"ABORT-DAG-ON {name} {jobs['abort_dag_on']['node_exit']}"
            f" RETURN {jobs['abort_dag_on']['abort_exit']}",
            file=stream,
        )


class HTCJob:
    """HTCondor job for use in building DAG.

    Parameters
    ----------
    name : `str`
        Name of the job.
    label : `str`
        Label that can used for grouping or lookup.
    initcmds : `RestrictedDict`
        Initial job commands for submit file.
    initdagcmds : `RestrictedDict`
        Initial commands for job inside DAG.
    initattrs : `dict`
        Initial dictionary of job attributes.
    """

    def __init__(self, name, label=None, initcmds=(), initdagcmds=(), initattrs=None):
        self.name = name
        self.label = label
        self.cmds = RestrictedDict(HTC_VALID_JOB_KEYS, initcmds)
        self.dagcmds = RestrictedDict(HTC_VALID_JOB_DAG_KEYS, initdagcmds)
        self.attrs = initattrs
        self.subfile = None

    def __str__(self):
        return self.name

    def add_job_cmds(self, new_commands):
        """Add commands to Job (overwrite existing).

        Parameters
        ----------
        new_commands : `dict`
            Submit file commands to be added to Job.
        """
        self.cmds.update(new_commands)

    def add_dag_cmds(self, new_commands):
        """Add DAG commands to Job (overwrite existing).

        Parameters
        ----------
        new_commands : `dict`
            DAG file commands to be added to Job.
        """
        self.dagcmds.update(new_commands)

    def add_job_attrs(self, new_attrs):
        """Add attributes to Job (overwrite existing).

        Parameters
        ----------
        new_attrs : `dict`
            Attributes to be added to Job.
        """
        if self.attrs is None:
            self.attrs = {}
        if new_attrs:
            self.attrs.update(new_attrs)

    def write_submit_file(self, submit_path, job_subdir=""):
        """Write job description to submit file.

        Parameters
        ----------
        submit_path : `str`
            Prefix path for the submit file.
        job_subdir : `str`, optional
            Template for job subdir.
        """
        if not self.subfile:
            self.subfile = f"{self.name}.sub"
            job_subdir = job_subdir.format(self=self)
            if job_subdir:
                self.subfile = os.path.join(job_subdir, self.subfile)
        htc_write_condor_file(os.path.join(submit_path, self.subfile), self.name, self.cmds, self.attrs)

    def write_dag_commands(self, stream):
        """Write DAG commands for single job to output stream.

        Parameters
        ----------
        stream : `IO` or `str`
            Output Stream.
        """
        print(f'JOB {self.name} "{self.subfile}"', file=stream)
        _htc_write_job_commands(stream, self.name, self.dagcmds)

    def dump(self, fh):
        """Dump job information to output stream.

        Parameters
        ----------
        fh : `~io.TextIOBase`
            Output stream.
        """
        printer = pprint.PrettyPrinter(indent=4, stream=fh)
        printer.pprint(self.name)
        printer.pprint(self.cmds)
        printer.pprint(self.attrs)


class HTCDag(networkx.DiGraph):
    """HTCondor DAG.

    Parameters
    ----------
    data : networkx.DiGraph.data
        Initial graph.
    name : `str`
        Name for DAG.
    """

    def __init__(self, data=None, name=""):
        super().__init__(data=data, name=name)

        self.graph["attr"] = {}
        self.graph["run_id"] = None
        self.graph["submit_path"] = None
        self.graph["final_job"] = None
        self.graph["service_job"] = None

    def __str__(self):
        """Represent basic DAG info as string.

        Returns
        -------
        info : `str`
            String containing basic DAG info.
        """
        return f"{self.graph['name']} {len(self)}"

    def add_attribs(self, attribs=None):
        """Add attributes to the DAG.

        Parameters
        ----------
        attribs : `dict`
            DAG attributes.
        """
        if attribs is not None:
            self.graph["attr"].update(attribs)

    def add_job(self, job, parent_names=None, child_names=None):
        """Add an HTCJob to the HTCDag.

        Parameters
        ----------
        job : `HTCJob`
            HTCJob to add to the HTCDag.
        parent_names : `~collections.abc.Iterable` [`str`], optional
            Names of parent jobs.
        child_names : `~collections.abc.Iterable` [`str`], optional
            Names of child jobs.
        """
        assert isinstance(job, HTCJob)

        # Add dag level attributes to each job
        job.add_job_attrs(self.graph["attr"])

        self.add_node(job.name, data=job)

        if parent_names is not None:
            self.add_job_relationships(parent_names, job.name)

        if child_names is not None:
            self.add_job_relationships(child_names, job.name)

    def add_job_relationships(self, parents, children):
        """Add DAG edge between parents and children jobs.

        Parameters
        ----------
        parents : `list` [`str`]
            Contains parent job name(s).
        children : `list` [`str`]
            Contains children job name(s).
        """
        self.add_edges_from(itertools.product(parents, children))

    def add_final_job(self, job):
        """Add an HTCJob for the FINAL job in HTCDag.

        Parameters
        ----------
        job : `HTCJob`
            HTCJob to add to the HTCDag as a FINAL job.
        """
        # Add dag level attributes to each job
        job.add_job_attrs(self.graph["attr"])

        self.graph["final_job"] = job

    def add_service_job(self, job):
        """Add an HTCJob for the SERVICE job in HTCDag.

        Parameters
        ----------
        job : `HTCJob`
            HTCJob to add to the HTCDag as a FINAL job.
        """
        # Add dag level attributes to each job
        job.add_job_attrs(self.graph["attr"])

        self.graph["service_job"] = job

    def del_job(self, job_name):
        """Delete the job from the DAG.

        Parameters
        ----------
        job_name : `str`
            Name of job in DAG to delete.
        """
        # Reconnect edges around node to delete
        parents = self.predecessors(job_name)
        children = self.successors(job_name)
        self.add_edges_from(itertools.product(parents, children))

        # Delete job node (which deletes its edges).
        self.remove_node(job_name)

    def write(self, submit_path, job_subdir=""):
        """Write DAG to a file.

        Parameters
        ----------
        submit_path : `str`
            Prefix path for dag filename to be combined with DAG name.
        job_subdir : `str`, optional
            Template for job subdir.
        """
        self.graph["submit_path"] = submit_path
        self.graph["dag_filename"] = os.path.join(submit_path, f"{self.graph['name']}.dag")
        os.makedirs(submit_path, exist_ok=True)
        with open(self.graph["dag_filename"], "w") as fh:
            for _, nodeval in self.nodes().items():
                job = nodeval["data"]
                job.write_submit_file(submit_path, job_subdir)
                job.write_dag_commands(fh)
            for edge in self.edges():
                print(f"PARENT {edge[0]} CHILD {edge[1]}", file=fh)
            print(f"DOT {self.name}.dot", file=fh)
            print(f"NODE_STATUS_FILE {self.name}.node_status", file=fh)

            # Add bps attributes to dag submission
            for key, value in self.graph["attr"].items():
                print(f'SET_JOB_ATTR {key}= "{htc_escape(value)}"', file=fh)

            # Add special nodes if any.
            special_jobs = {
                "FINAL": self.graph["final_job"],
                "SERVICE": self.graph["service_job"],
            }
            for dagcmd, job in special_jobs.items():
                if job is not None:
                    job.write_submit_file(submit_path, job_subdir)
                    print(f"{dagcmd} {job.name} {job.subfile}", file=fh)
                    if "pre" in job.dagcmds:
                        print(f"SCRIPT PRE {job.name} {job.dagcmds['pre']}", file=fh)
                    if "post" in job.dagcmds:
                        print(f"SCRIPT POST {job.name} {job.dagcmds['post']}", file=fh)

    def dump(self, fh):
        """Dump DAG info to output stream.

        Parameters
        ----------
        fh : `io.IO` or `str`
            Where to dump DAG info as text.
        """
        for key, value in self.graph:
            print(f"{key}={value}", file=fh)
        for name, data in self.nodes().items():
            print(f"{name}:", file=fh)
            data.dump(fh)
        for edge in self.edges():
            print(f"PARENT {edge[0]} CHILD {edge[1]}", file=fh)
        if self.graph["final_job"]:
            print(f'FINAL {self.graph["final_job"].name}:', file=fh)
            self.graph["final_job"].dump(fh)

    def write_dot(self, filename):
        """Write a dot version of the DAG.

        Parameters
        ----------
        filename : `str`
            Name of the dot file.
        """
        pos = networkx.nx_agraph.graphviz_layout(self)
        networkx.draw(self, pos=pos)
        networkx.drawing.nx_pydot.write_dot(self, filename)


def condor_q(constraint=None, schedds=None, **kwargs):
    """Get information about the jobs in the HTCondor job queue(s).

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    schedds : `dict` [`str`, `htcondor.Schedd`], optional
        HTCondor schedulers which to query for job information. If None
        (default), the query will be run against local scheduler only.
    **kwargs : `~typing.Any`
        Additional keyword arguments that need to be passed to the internal
        query method.

    Returns
    -------
    job_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    return condor_query(constraint, schedds, htc_query_present, **kwargs)


def condor_history(constraint=None, schedds=None, **kwargs):
    """Get information about the jobs from HTCondor history records.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    schedds : `dict` [`str`, `htcondor.Schedd`], optional
        HTCondor schedulers which to query for job information. If None
        (default), the query will be run against the history file of
        the local scheduler only.
    **kwargs : `~typing.Any`
        Additional keyword arguments that need to be passed to the internal
        query method.

    Returns
    -------
    job_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    return condor_query(constraint, schedds, htc_query_history, **kwargs)


def condor_query(constraint=None, schedds=None, query_func=htc_query_present, **kwargs):
    """Get information about HTCondor jobs.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    schedds : `dict` [`str`, `htcondor.Schedd`], optional
        HTCondor schedulers which to query for job information. If None
        (default), the query will be run against the history file of
        the local scheduler only.
    query_func : callable
        An query function which takes following arguments:

        - ``schedds``: Schedulers to query (`list` [`htcondor.Schedd`]).
        - ``**kwargs``: Keyword arguments that will be passed to the query
          function.
    **kwargs : `~typing.Any`
        Additional keyword arguments that need to be passed to the query
        method.

    Returns
    -------
    job_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    if not schedds:
        coll = htcondor.Collector()
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        schedds = {schedd_ad["Name"]: htcondor.Schedd(schedd_ad)}

    # Make sure that 'ClusterId' and 'ProcId' attributes are always included
    # in the job classad. They are needed to construct the job id.
    added_attrs = set()
    if "projection" in kwargs and kwargs["projection"]:
        requested_attrs = set(kwargs["projection"])
        required_attrs = {"ClusterId", "ProcId"}
        added_attrs = required_attrs - requested_attrs
        for attr in added_attrs:
            kwargs["projection"].append(attr)

    unwanted_attrs = {"Env", "Environment"} | added_attrs
    job_info = defaultdict(dict)
    for schedd_name, job_ad in query_func(schedds, constraint=constraint, **kwargs):
        id_ = f"{job_ad['ClusterId']}.{job_ad['ProcId']}"
        for attr in set(job_ad) & unwanted_attrs:
            del job_ad[attr]
        job_info[schedd_name][id_] = job_ad
    _LOG.debug("query returned %d jobs", sum(len(val) for val in job_info.values()))

    # Restore the list of the requested attributes to its original value
    # if needed.
    if added_attrs:
        for attr in added_attrs:
            kwargs["projection"].remove(attr)

    # When returning the results filter out entries for schedulers with no jobs
    # matching the search criteria.
    return {key: val for key, val in job_info.items() if val}


def condor_search(constraint=None, hist=None, schedds=None):
    """Search for running and finished jobs satisfying given criteria.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to job query.
    hist : `float`
        Limit history search to this many days.
    schedds : `dict` [`str`, `htcondor.Schedd`], optional
        The list of the HTCondor schedulers which to query for job information.
        If None (default), only the local scheduler will be queried.

    Returns
    -------
    job_info : `dict` [`str`, `dict` [`str`, `dict` [`str` Any]]]
        Information about jobs satisfying the search criteria where for each
        Scheduler, local HTCondor job ids are mapped to their respective
        classads.
    """
    if not schedds:
        coll = htcondor.Collector()
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        schedds = {schedd_ad["Name"]: htcondor.Schedd(locate_ad=schedd_ad)}

    job_info = condor_q(constraint=constraint, schedds=schedds)
    if hist is not None:
        epoch = (datetime.now() - timedelta(days=hist)).timestamp()
        constraint += f" && (CompletionDate >= {epoch} || JobFinishedHookDone >= {epoch})"
        hist_info = condor_history(constraint, schedds=schedds)
        update_job_info(job_info, hist_info)
    return job_info


def condor_status(constraint=None, coll=None):
    """Get information about HTCondor pool.

    Parameters
    ----------
    constraint : `str`, optional
        Constraints to be passed to the query.
    coll : `htcondor.Collector`, optional
        Object representing HTCondor collector daemon.

    Returns
    -------
    pool_info : `dict` [`str`, `dict` [`str`, Any]]
        Mapping between HTCondor slot names and slot information (classAds).
    """
    if coll is None:
        coll = htcondor.Collector()
    try:
        pool_ads = coll.query(constraint=constraint)
    except OSError as ex:
        raise RuntimeError(f"Problem querying the Collector.  (Constraint='{constraint}')") from ex

    pool_info = {}
    for slot in pool_ads:
        pool_info[slot["name"]] = dict(slot)
    _LOG.debug("condor_status returned %d ads", len(pool_info))
    return pool_info


def update_job_info(job_info, other_info):
    """Update results of a job query with results from another query.

    Parameters
    ----------
    job_info : `dict` [`str`, `dict` [`str`, Any]]
        Results of the job query that needs to be updated.
    other_info : `dict` [`str`, `dict` [`str`, Any]]
        Results of the other job query.

    Returns
    -------
    job_info : `dict` [`str`, `dict` [`str`, Any]]
        The updated results.
    """
    for schedd_name, others in other_info.items():
        try:
            jobs = job_info[schedd_name]
        except KeyError:
            job_info[schedd_name] = others
        else:
            for id_, ad in others.items():
                jobs.setdefault(id_, {}).update(ad)
    return job_info


def summary_from_dag(dir_name):
    """Build bps_run_summary string from dag file.

    Parameters
    ----------
    dir_name : `str`
        Path that includes dag file for a run.

    Returns
    -------
    summary : `str`
        Semi-colon separated list of job labels and counts.
        (Same format as saved in dag classad).
    job_name_to_label : `dict` [`str`, `str`]
        Mapping of job names to job labels.
    """
    # Later code depends upon insertion order
    counts = defaultdict(int)
    job_name_to_label = {}
    try:
        dag = next(Path(dir_name).glob("*.dag"))
        with open(dag) as fh:
            for line in fh:
                if line.startswith("JOB"):
                    m = re.match(r'JOB (\S+) "jobs/([^/]+)/', line)
                    if m:
                        label = m.group(2)
                        if label == "init":
                            label = "pipetaskInit"
                        job_name_to_label[m.group(1)] = label
                        counts[label] += 1
                    else:  # Check if Pegasus submission
                        m = re.match(r"JOB (\S+) (\S+)", line)
                        if m:
                            label = pegasus_name_to_label(m.group(1))
                            job_name_to_label[m.group(1)] = label
                            counts[label] += 1
                        else:
                            _LOG.warning("Parse DAG: unmatched job line: %s", line)
                elif line.startswith("FINAL"):
                    m = re.match(r"FINAL (\S+) jobs/([^/]+)/", line)
                    if m:
                        label = m.group(2)
                        job_name_to_label[m.group(1)] = label
                        counts[label] += 1
                elif line.startswith("SERVICE"):
                    m = re.match(r"SERVICE (\S+) jobs/([^/]+)/", line)
                    if m:
                        label = m.group(2)
                        job_name_to_label[m.group(1)] = label
    except (OSError, PermissionError, StopIteration):
        pass

    summary = ";".join([f"{name}:{counts[name]}" for name in counts])
    _LOG.debug("summary_from_dag: %s %s", summary, job_name_to_label)
    return summary, job_name_to_label


def pegasus_name_to_label(name):
    """Convert pegasus job name to a label for the report.

    Parameters
    ----------
    name : `str`
        Name of job.

    Returns
    -------
    label : `str`
        Label for job.
    """
    label = "UNK"
    if name.startswith("create_dir") or name.startswith("stage_in") or name.startswith("stage_out"):
        label = "pegasus"
    else:
        m = re.match(r"pipetask_(\d+_)?([^_]+)", name)
        if m:
            label = m.group(2)
            if label == "init":
                label = "pipetaskInit"

    return label


def read_dag_status(wms_path):
    """Read the node status file for DAG summary information.

    Parameters
    ----------
    wms_path : `str`
        Path that includes node status file for a run.

    Returns
    -------
    dag_ad : `dict` [`str`, Any]
        DAG summary information.
    """
    dag_ad = {}

    # While this is probably more up to date than dag classad, only read from
    # file if need to.
    try:
        try:
            node_stat_file = next(Path(wms_path).glob("*.node_status"))
            _LOG.debug("Reading Node Status File %s", node_stat_file)
            with open(node_stat_file) as infh:
                dag_ad = classad.parseNext(infh)  # pylint: disable=E1101
        except StopIteration:
            pass

        if not dag_ad:
            # Pegasus check here
            try:
                metrics_file = next(Path(wms_path).glob("*.dag.metrics"))
                with open(metrics_file) as infh:
                    metrics = json.load(infh)
                dag_ad["NodesTotal"] = metrics.get("jobs", 0)
                dag_ad["NodesFailed"] = metrics.get("jobs_failed", 0)
                dag_ad["NodesDone"] = metrics.get("jobs_succeeded", 0)
                dag_ad["pegasus_version"] = metrics.get("planner_version", "")
            except StopIteration:
                try:
                    metrics_file = next(Path(wms_path).glob("*.metrics"))
                    with open(metrics_file) as infh:
                        metrics = json.load(infh)
                    dag_ad["NodesTotal"] = metrics["wf_metrics"]["total_jobs"]
                    dag_ad["pegasus_version"] = metrics.get("version", "")
                except StopIteration:
                    pass
    except (OSError, PermissionError):
        pass

    _LOG.debug("read_dag_status: %s", dag_ad)
    return dict(dag_ad)


def read_node_status(wms_path):
    """Read entire node status file.

    Parameters
    ----------
    wms_path : `str`
        Path that includes node status file for a run.

    Returns
    -------
    jobs : `dict` [`str`, Any]
        DAG summary information compiled from the node status file combined
        with the information found in the node event log.

        Currently, if the same job attribute is found in both files, its value
        from the event log takes precedence over the value from the node status
        file.
    """
    # Get jobid info from other places to fill in gaps in info from node_status
    _, job_name_to_label = summary_from_dag(wms_path)
    wms_workflow_id, loginfo = read_dag_log(wms_path)
    loginfo = read_dag_nodes_log(wms_path)
    _LOG.debug("loginfo = %s", loginfo)
    job_name_to_id = {}
    for job_id, job_info in loginfo.items():
        if "LogNotes" in job_info:
            m = re.match(r"DAG Node: (\S+)", job_info["LogNotes"])
            if m:
                job_name_to_id[m.group(1)] = job_id
                job_info["DAGNodeName"] = m.group(1)

    try:
        node_status = next(Path(wms_path).glob("*.node_status"))
    except StopIteration:
        return loginfo

    jobs = {}
    fake_id = -1.0  # For nodes that do not yet have a job id, give fake one
    try:
        with open(node_status) as fh:
            for ad in classad.parseAds(fh):
                match ad["Type"]:
                    case "DagStatus":
                        # Skip DAG summary.
                        pass
                    case "NodeStatus":
                        job_name = ad["Node"]
                        if job_name in job_name_to_label:
                            job_label = job_name_to_label[job_name]
                        elif "_" in job_name:
                            job_label = job_name.split("_")[1]
                        else:
                            job_label = job_name

                        # Make job info as if came from condor_q.
                        if job_name in job_name_to_id:
                            job_id = str(job_name_to_id[job_name])
                        else:
                            job_id = str(fake_id)
                            fake_id -= 1
                        job = dict(ad)
                        job["ClusterId"] = int(float(job_id))
                        job["DAGManJobID"] = wms_workflow_id
                        job["DAGNodeName"] = job_name
                        job["bps_job_label"] = job_label

                        # Include information retrieved from the event log
                        # if available.
                        jobs[job_id] = job
                        try:
                            jobs[job_id] |= loginfo[job_id]
                        except KeyError:
                            pass
                    case "StatusEnd":
                        # Skip node status file "epilog".
                        pass
                    case _:
                        _LOG.debug(
                            "Ignoring unknown classad type '%s' in the node status file '%s'",
                            ad["Type"],
                            wms_path,
                        )
    except (OSError, PermissionError):
        pass
    else:
        # Assume that the jobs found in the event log, but *not* in the node
        # status file are the service jobs as HTCondor does not include
        # information about these jobs in the node status file at the moment.
        #
        # Note: To be able to easily identify the service jobs downstream,
        # we reverse the ClusterId and ProcId in their HTCondor ids in internal
        # use. For example, if HTCondor id of a service job is '1.0', we will
        # use '0.1' instead.
        service_jobs = {job_id: loginfo[job_id] for job_id in set(loginfo) - set(jobs)}
        job_id_to_name = {
            job_id: job_name for job_name, job_id in job_name_to_id.items() if job_id in service_jobs
        }
        for job_id, job_info in service_jobs.items():
            job_info["bps_job_label"] = job_name_to_label[job_id_to_name[job_id]]
            jobs[f"{job_info['ProcId']}.{job_info['ClusterId']}"] = job_info

    return jobs


def read_dag_log(wms_path: str) -> tuple[str, dict[str, Any]]:
    """Read job information from the DAGMan log file.

    Parameters
    ----------
    wms_path : `str`
        Path containing the DAGMan log file.

    Returns
    -------
    wms_workflow_id : `str`
        HTCondor job id (i.e., <ClusterId>.<ProcId>) of the DAGMan job.
    dag_info : `dict` [`str`, `~collections.abc.Any`]
        HTCondor job information read from the log file mapped to HTCondor
        job id.

    Raises
    ------
    FileNotFoundError
        If cannot find DAGMan log in given wms_path.
    """
    wms_workflow_id = 0
    dag_info = {}

    path = Path(wms_path)
    if path.exists():
        try:
            filename = next(path.glob("*.dag.dagman.log"))
        except StopIteration as exc:
            raise FileNotFoundError(f"DAGMan log not found in {wms_path}") from exc
        _LOG.debug("dag node log filename: %s", filename)

        info = {}
        job_event_log = htcondor.JobEventLog(str(filename))
        for event in job_event_log.events(stop_after=0):
            id_ = f"{event['Cluster']}.{event['Proc']}"
            if id_ not in info:
                info[id_] = {}
                wms_workflow_id = id_  # taking last job id in case of restarts
            info[id_].update(event)
            info[id_][f"{event.type.name.lower()}_time"] = event["EventTime"]

        # only save latest DAG job
        dag_info = {wms_workflow_id: info[wms_workflow_id]}
        for job in dag_info.values():
            _tweak_log_info(filename, job)

    return wms_workflow_id, dag_info


def read_dag_nodes_log(wms_path):
    """Read job information from the DAGMan nodes log file.

    Parameters
    ----------
    wms_path : `str`
        Path containing the DAGMan nodes log file.

    Returns
    -------
    info : `dict` [`str`, Any]
        HTCondor job information read from the log file mapped to HTCondor
        job id.

    Raises
    ------
    FileNotFoundError
        If cannot find DAGMan node log in given wms_path.
    """
    try:
        filename = next(Path(wms_path).glob("*.dag.nodes.log"))
    except StopIteration as exc:
        raise FileNotFoundError(f"DAGMan node log not found in {wms_path}") from exc
    _LOG.debug("dag node log filename: %s", filename)

    info = {}
    job_event_log = htcondor.JobEventLog(str(filename))
    for event in job_event_log.events(stop_after=0):
        id_ = f"{event['Cluster']}.{event['Proc']}"
        if id_ not in info:
            info[id_] = {}
        info[id_].update(event)
        info[id_][f"{event.type.name.lower()}_time"] = event["EventTime"]

    # Add more condor_q-like info to info parsed from log file.
    for job in info.values():
        _tweak_log_info(filename, job)

    return info


def read_dag_info(wms_path):
    """Read custom DAGMan job information from the file.

    Parameters
    ----------
    wms_path : `str`
        Path containing the file with the DAGMan job info.

    Returns
    -------
    dag_info : `dict` [`str`, `dict` [`str`, Any]]
        HTCondor job information.

    Raises
    ------
    FileNotFoundError
        If cannot find DAGMan job info file in the given location.
    """
    try:
        filename = next(Path(wms_path).glob("*.info.json"))
    except StopIteration as exc:
        raise FileNotFoundError(f"File with DAGMan job information not found in {wms_path}") from exc
    _LOG.debug("DAGMan job information filename: %s", filename)
    try:
        with open(filename) as fh:
            dag_info = json.load(fh)
    except (OSError, PermissionError) as exc:
        _LOG.debug("Retrieving DAGMan job information failed: %s", exc)
        dag_info = {}
    return dag_info


def write_dag_info(filename, dag_info):
    """Write custom job information about DAGMan job.

    Parameters
    ----------
    filename : `str`
        Name of the file where the information will be stored.
    dag_info : `dict` [`str` `dict` [`str`, Any]]
        Information about the DAGMan job.
    """
    schedd_name = next(iter(dag_info))
    dag_id = next(iter(dag_info[schedd_name]))
    dag_ad = dag_info[schedd_name][dag_id]
    ad = {"ClusterId": dag_ad["ClusterId"], "GlobalJobId": dag_ad["GlobalJobId"]}
    ad.update({key: val for key, val in dag_ad.items() if key.startswith("bps")})
    try:
        with open(filename, "w") as fh:
            info = {schedd_name: {dag_id: ad}}
            json.dump(info, fh)
    except (KeyError, OSError, PermissionError) as exc:
        _LOG.debug("Persisting DAGMan job information failed: %s", exc)


def _tweak_log_info(filename, job):
    """Massage the given job info has same structure as if came from condor_q.

    Parameters
    ----------
    filename : `pathlib.Path`
        Name of the DAGMan log.
    job : `dict` [ `str`, Any ]
        A mapping between HTCondor job id and job information read from
        the log.
    """
    _LOG.debug("_tweak_log_info: %s %s", filename, job)

    try:
        job["ClusterId"] = job["Cluster"]
        job["ProcId"] = job["Proc"]
        job["Iwd"] = str(filename.parent)
        job["Owner"] = filename.owner()

        match job["MyType"]:
            case "ExecuteEvent":
                job["JobStatus"] = JobStatus.RUNNING
            case "JobTerminatedEvent" | "PostScriptTerminatedEvent":
                job["JobStatus"] = JobStatus.COMPLETED
            case "SubmitEvent":
                job["JobStatus"] = JobStatus.IDLE
            case "JobAbortedEvent":
                job["JobStatus"] = JobStatus.REMOVED
            case "JobHeldEvent":
                job["JobStatus"] = JobStatus.HELD
            case _:
                _LOG.debug("Unknown log event type: %s", job["MyType"])
                job["JobStatus"] = JobStatus.UNEXPANDED

        if job["JobStatus"] in {JobStatus.COMPLETED, JobStatus.HELD}:
            new_job = HTC_JOB_AD_HANDLERS.handle(job)
            if new_job is not None:
                job = new_job
            else:
                _LOG.error("Could not determine exit status for job '%s.%s'", job["ClusterId"], job["ProcId"])

    except KeyError as e:
        _LOG.error("Missing key %s in job: %s", str(e), job)
        raise


def htc_check_dagman_output(wms_path):
    """Check the DAGMan output for error messages.

    Parameters
    ----------
    wms_path : `str`
        Directory containing the DAGman output file.

    Returns
    -------
    message : `str`
        Message containing error messages from the DAGMan output.  Empty
        string if no messages.

    Raises
    ------
    FileNotFoundError
        If cannot find DAGMan standard output file in given wms_path.
    """
    try:
        filename = next(Path(wms_path).glob("*.dag.dagman.out"))
    except StopIteration as exc:
        raise FileNotFoundError(f"DAGMan standard output file not found in {wms_path}") from exc
    _LOG.debug("dag output filename: %s", filename)

    message = ""
    try:
        with open(filename) as fh:
            last_submit_failed = ""
            for line in fh:
                m = re.match(r"(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) Job submit try \d+/\d+ failed", line)
                if m:
                    last_submit_failed = m.group(1)
                else:
                    m = re.search(r"Warning: (.+)", line)
                    if m:
                        if ".dag.nodes.log is in /tmp" in m.group(1):
                            last_warning = "Cannot submit from /tmp."
                        else:
                            last_warning = m.group(1)
                    else:
                        m = re.search(r"(ERROR: .+)", line)
                        if m:
                            if (
                                m.group(1)
                                == "ERROR: Warning is fatal error because of DAGMAN_USE_STRICT setting"
                            ):
                                message += f"ERROR: {last_warning}"
        if last_submit_failed:
            message += f"Warn: Job submission issues (last: {last_submit_failed})"
    except (OSError, PermissionError):
        message = f"Warn: Could not read dagman output file from {wms_path}."
    _LOG.debug("dag output file message: %s", message)
    return message
