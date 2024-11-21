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

"""Support functions for restart."""

__all__ = ["restart"]


import logging
from pathlib import Path

from lsst.ctrl.bps.bps_utils import chdir

from .lssthtc import (
    condor_q,
    htc_backup_files,
    htc_create_submit_from_file,
    htc_submit_dag,
    read_dag_status,
    write_dag_info,
)
from .utils import WmsIdType, _wms_id_to_dir

_LOG = logging.getLogger(__name__)


def restart(wms_workflow_id):
    """Restart a failed DAGMan workflow.

    Parameters
    ----------
    wms_workflow_id : `str`
        The directory with HTCondor files.

    Returns
    -------
    run_id : `str`
        HTCondor id of the restarted DAGMan job. If restart failed, it will
        be set to None.
    run_name : `str`
        Name of the restarted workflow. If restart failed, it will be set
        to None.
    message : `str`
        A message describing any issues encountered during the restart.
        If there were no issues, an empty string is returned.
    """
    wms_path, id_type = _wms_id_to_dir(wms_workflow_id)
    if wms_path is None:
        return (
            None,
            None,
            (
                f"workflow with run id '{wms_workflow_id}' not found. "
                f"Hint: use run's submit directory as the id instead"
            ),
        )

    if id_type in {WmsIdType.GLOBAL, WmsIdType.LOCAL}:
        if not wms_path.is_dir():
            return None, None, f"submit directory '{wms_path}' for run id '{wms_workflow_id}' not found."

    _LOG.info("Restarting workflow from directory '%s'", wms_path)
    rescue_dags = list(wms_path.glob("*.dag.rescue*"))
    if not rescue_dags:
        return None, None, f"HTCondor rescue DAG(s) not found in '{wms_path}'"

    _LOG.info("Verifying that the workflow is not already in the job queue")
    schedd_dag_info = condor_q(constraint=f'regexp("dagman$", Cmd) && Iwd == "{wms_path}"')
    if schedd_dag_info:
        _, dag_info = schedd_dag_info.popitem()
        _, dag_ad = dag_info.popitem()
        id_ = dag_ad["GlobalJobId"]
        return None, None, f"Workflow already in the job queue (global job id: '{id_}')"

    _LOG.info("Checking execution status of the workflow")
    warn = False
    dag_ad = read_dag_status(str(wms_path))
    if dag_ad:
        nodes_total = dag_ad.get("NodesTotal", 0)
        if nodes_total != 0:
            nodes_done = dag_ad.get("NodesDone", 0)
            if nodes_total == nodes_done:
                return None, None, "All jobs in the workflow finished successfully"
        else:
            warn = True
    else:
        warn = True
    if warn:
        _LOG.warning(
            "Cannot determine the execution status of the workflow, continuing with restart regardless"
        )

    _LOG.info("Backing up select HTCondor files from previous run attempt")
    htc_backup_files(wms_path, subdir="backups")

    # For workflow portability, internal paths are all relative. Hence
    # the DAG needs to be resubmitted to HTCondor from inside the submit
    # directory.
    _LOG.info("Adding workflow to the job queue")
    run_id, run_name, message = None, None, ""
    with chdir(wms_path):
        try:
            dag_path = next(Path.cwd().glob("*.dag.condor.sub"))
        except StopIteration:
            message = f"DAGMan submit description file not found in '{wms_path}'"
        else:
            sub = htc_create_submit_from_file(dag_path.name)
            schedd_dag_info = htc_submit_dag(sub)

            # Save select information about the DAGMan job to a file. Use
            # the run name (available in the ClassAd) as the filename.
            if schedd_dag_info:
                dag_info = next(iter(schedd_dag_info.values()))
                dag_ad = next(iter(dag_info.values()))
                write_dag_info(f"{dag_ad['bps_run']}.info.json", schedd_dag_info)
                run_id = f"{dag_ad['ClusterId']}.{dag_ad['ProcId']}"
                run_name = dag_ad["bps_run"]
            else:
                message = "DAGMan job information unavailable"

    return run_id, run_name, message
