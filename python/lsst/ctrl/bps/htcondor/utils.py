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

"""Miscellaneous support classes and functions."""

__all__ = [
    "WmsIdType",
    "_wms_id_type",
    "_wms_id_to_cluster",
    "_wms_id_to_dir",
    "_locate_schedds",
    "is_service_job",
    "_update_dicts",
]


import logging
from enum import IntEnum, auto
from pathlib import Path

import htcondor

from .lssthtc import condor_history, condor_q, read_dag_info


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


_LOG = logging.getLogger(__name__)


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


def _locate_schedds(locate_all=False):
    """Find out Scheduler daemons in an HTCondor pool.

    Parameters
    ----------
    locate_all : `bool`, optional
        If True, all available schedulers in the HTCondor pool will be located.
        False by default which means that the search will be limited to looking
        for the Scheduler running on a local host.

    Returns
    -------
    schedds : `dict` [`str`, `htcondor.Schedd`]
        A mapping between Scheduler names and Python objects allowing for
        interacting with them.
    """
    coll = htcondor.Collector()

    schedd_ads = []
    if locate_all:
        schedd_ads.extend(coll.locateAll(htcondor.DaemonTypes.Schedd))
    else:
        schedd_ads.append(coll.locate(htcondor.DaemonTypes.Schedd))
    return {ad["Name"]: htcondor.Schedd(ad) for ad in schedd_ads}


def is_service_job(job_id: str) -> bool:
    """Determine if a job is a service one.

    Parameters
    ----------
    job_id : str
        HTCondor job id.

    Returns
    -------
    is_service_job : `bool`
        True if the job is a service one, false otherwise.

    Notes
    -----
    At the moment, HTCondor does not provide a native way to distinguish
    between payload and service jobs in the workflow. As a result, the current
    implementation depends entirely on the logic that is used in
    :py:func:`read_node_status()` (service jobs are given ids with ClusterId=0
    and ProcId=some integer). If it changes, this function needs to be
    updated too.
    """
    return int(float(job_id)) == 0


def _update_dicts(dict1, dict2):
    """Update dict1 with info in dict2.

    (Basically an update for nested dictionaries.)

    Parameters
    ----------
    dict1 : `dict` [`str`, `dict` [`str`, `Any`]]
        HTCondor job information to be updated.
    dict2 : `dict` [`str`, `dict` [`str`, `Any`]]
        Additional HTCondor job information.
    """
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict, dict1[key]) and isinstance(dict, value):
            _update_dicts(dict1[key], value)
        else:
            dict1[key] = value
