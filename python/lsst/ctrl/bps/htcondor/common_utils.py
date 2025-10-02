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

"""Utility functions used by multiple functions in ctrl_bps_htcondor."""

import logging
from enum import IntEnum, auto
from pathlib import Path

import htcondor

from lsst.ctrl.bps import (
    WmsStates,
)

from .lssthtc import (
    NodeStatus,
    condor_history,
    condor_q,
    read_dag_info,
)

_LOG = logging.getLogger(__name__)


class WmsIdType(IntEnum):
    """Type of valid WMS ids."""

    UNKNOWN = auto()
    """The type of id cannot be determined.
    """

    LOCAL = auto()
    """The id is HTCondor job's ClusterId (with optional '.ProcId').
    """

    GLOBAL = auto()
    """Id is a HTCondor's global job id.
    """

    PATH = auto()
    """Id is a submission path.
    """


def _htc_status_to_wms_state(job):
    """Convert HTCondor job status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `~typing.Any`]
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
    job : `dict` [`str`, `~typing.Any`]
        HTCondor job information.

    Returns
    -------
    wms_state : `lsst.ctrl.bps.WmsStates`
        The equivalent WmsState to given job's status.
    """
    _LOG.debug(
        "htc_job_status_to_wms_state: %s=%s, %s", job["ClusterId"], job["JobStatus"], type(job["JobStatus"])
    )
    wms_state = WmsStates.MISFIT
    if "JobStatus" in job and job["JobStatus"]:
        job_status = int(job["JobStatus"])

        _LOG.debug("htc_job_status_to_wms_state: job_status = %s", job_status)
        if job_status == htcondor.JobStatus.IDLE:
            wms_state = WmsStates.PENDING
        elif job_status == htcondor.JobStatus.RUNNING:
            wms_state = WmsStates.RUNNING
        elif job_status == htcondor.JobStatus.REMOVED:
            if (job.get("ExitBySignal", False) and job.get("ExitSignal", 0)) or job.get("ExitCode", 0):
                wms_state = WmsStates.FAILED
            else:
                wms_state = WmsStates.DELETED
        elif job_status == htcondor.JobStatus.COMPLETED:
            if (
                (job.get("ExitBySignal", False) and job.get("ExitSignal", 0))
                or job.get("ExitCode", 0)
                or job.get("DAG_Status", 0)
            ):
                wms_state = WmsStates.FAILED
            else:
                wms_state = WmsStates.SUCCEEDED
        elif job_status == htcondor.JobStatus.HELD:
            wms_state = WmsStates.HELD

    return wms_state


def _htc_node_status_to_wms_state(job):
    """Convert HTCondor node status to generic wms state.

    Parameters
    ----------
    job : `dict` [`str`, `~typing.Any`]
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


def _wms_id_type(wms_id):
    """Determine the type of the WMS id.

    Parameters
    ----------
    wms_id : `str`
        WMS id identifying a job.

    Returns
    -------
    id_type : `lsst.ctrl.bps.htcondor.WmsIdType`
        Type of WMS id.
    """
    try:
        int(float(wms_id))
    except ValueError:
        wms_path = Path(wms_id)
        if wms_path.is_dir():
            id_type = WmsIdType.PATH
        else:
            id_type = WmsIdType.GLOBAL
    except TypeError:
        id_type = WmsIdType.UNKNOWN
    else:
        id_type = WmsIdType.LOCAL
    return id_type


def _wms_id_to_cluster(wms_id):
    """Convert WMS id to cluster id.

    Parameters
    ----------
    wms_id : `int` or `float` or `str`
        HTCondor job id or path.

    Returns
    -------
    schedd_ad : `classad.ClassAd`
        ClassAd describing the scheduler managing the job with the given id.
    cluster_id : `int`
        HTCondor cluster id.
    id_type : `lsst.ctrl.bps.wms.htcondor.IdType`
        The type of the provided id.
    """
    coll = htcondor.Collector()

    schedd_ad = None
    cluster_id = None
    id_type = _wms_id_type(wms_id)
    if id_type == WmsIdType.LOCAL:
        schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
        cluster_id = int(float(wms_id))
    elif id_type == WmsIdType.GLOBAL:
        constraint = f'GlobalJobId == "{wms_id}"'
        schedd_ads = {ad["Name"]: ad for ad in coll.locateAll(htcondor.DaemonTypes.Schedd)}
        schedds = {name: htcondor.Schedd(ad) for name, ad in schedd_ads.items()}
        job_info = condor_q(constraint=constraint, schedds=schedds)
        if job_info:
            schedd_name, job_rec = job_info.popitem()
            job_id, _ = job_rec.popitem()
            schedd_ad = schedd_ads[schedd_name]
            cluster_id = int(float(job_id))
    elif id_type == WmsIdType.PATH:
        try:
            job_info = read_dag_info(wms_id)
        except (FileNotFoundError, PermissionError, OSError):
            pass
        else:
            schedd_name, job_rec = job_info.popitem()
            job_id, _ = job_rec.popitem()
            schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd, schedd_name)
            cluster_id = int(float(job_id))
    else:
        pass
    return schedd_ad, cluster_id, id_type


def _wms_id_to_dir(wms_id):
    """Convert WMS id to a submit directory candidate.

    The function does not check if the directory exists or if it is a valid
    BPS submit directory.

    Parameters
    ----------
    wms_id : `int` or `float` or `str`
        HTCondor job id or path.

    Returns
    -------
    wms_path : `pathlib.Path` or None
        Submit directory candidate for the run with the given job id. If no
        directory can be associated with the provided WMS id, it will be set
        to None.
    id_type : `lsst.ctrl.bps.wms.htcondor.IdType`
        The type of the provided id.

    Raises
    ------
    TypeError
        Raised if provided WMS id has invalid type.
    """
    coll = htcondor.Collector()
    schedd_ads = []

    constraint = None
    wms_path = None
    id_type = _wms_id_type(wms_id)
    match id_type:
        case WmsIdType.LOCAL:
            constraint = f"ClusterId == {int(float(wms_id))}"
            schedd_ads.append(coll.locate(htcondor.DaemonTypes.Schedd))
        case WmsIdType.GLOBAL:
            constraint = f'GlobalJobId == "{wms_id}"'
            schedd_ads.extend(coll.locateAll(htcondor.DaemonTypes.Schedd))
        case WmsIdType.PATH:
            wms_path = Path(wms_id).resolve()
        case WmsIdType.UNKNOWN:
            raise TypeError(f"Invalid job id type: {wms_id}")
    if constraint is not None:
        schedds = {ad["name"]: htcondor.Schedd(ad) for ad in schedd_ads}
        job_info = condor_history(constraint=constraint, schedds=schedds, projection=["Iwd"])
        if job_info:
            _, job_rec = job_info.popitem()
            _, job_ad = job_rec.popitem()
            wms_path = Path(job_ad["Iwd"])
    return wms_path, id_type
