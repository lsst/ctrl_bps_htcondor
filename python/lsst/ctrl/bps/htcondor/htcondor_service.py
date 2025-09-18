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

"""Interface between generic workflow to HTCondor workflow system."""

__all__ = ["HTCondorService"]


import logging
import os
from pathlib import Path

import htcondor
from packaging import version

from lsst.ctrl.bps import (
    BaseWmsService,
    WmsStates,
)
from lsst.ctrl.bps.bps_utils import chdir
from lsst.daf.butler import Config
from lsst.utils.timer import time_this

from .common_utils import WmsIdType, _wms_id_to_cluster, _wms_id_to_dir, _wms_id_type
from .htcondor_config import HTC_DEFAULTS_URI
from .htcondor_workflow import HTCondorWorkflow
from .lssthtc import (
    _locate_schedds,
    _update_rescue_file,
    condor_q,
    htc_backup_files,
    htc_create_submit_from_cmd,
    htc_create_submit_from_dag,
    htc_create_submit_from_file,
    htc_submit_dag,
    htc_version,
    read_dag_status,
    write_dag_info,
)
from .provisioner import Provisioner
from .report_utils import (
    _get_status_from_id,
    _get_status_from_path,
    _report_from_id,
    _report_from_path,
    _summary_report,
)

_LOG = logging.getLogger(__name__)


class HTCondorService(BaseWmsService):
    """HTCondor version of WMS service."""

    @property
    def defaults(self):
        return Config(HTC_DEFAULTS_URI)

    @property
    def defaults_uri(self):
        return HTC_DEFAULTS_URI

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an HTCondor DAG ready for submission.

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration that includes necessary submit/runtime
            information.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
            The generic workflow (e.g., has executable name and arguments).
        out_prefix : `str`
            The root directory into which all WMS-specific files are written.

        Returns
        -------
        workflow : `lsst.ctrl.bps.wms.htcondor.HTCondorWorkflow`
            HTCondor workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        with time_this(log=_LOG, level=logging.INFO, prefix=None, msg="Completed HTCondor workflow creation"):
            workflow = HTCondorWorkflow.from_generic_workflow(
                config,
                generic_workflow,
                out_prefix,
                f"{self.__class__.__module__}.{self.__class__.__name__}",
            )

            _, enable_provisioning = config.search("provisionResources")
            if enable_provisioning:
                provisioner = Provisioner(config)
                provisioner.configure()
                provisioner.prepare("provisioningJob.bash", prefix=out_prefix)
                provisioner.provision(workflow.dag)

        with time_this(
            log=_LOG, level=logging.INFO, prefix=None, msg="Completed writing out HTCondor workflow"
        ):
            workflow.write(out_prefix)
        return workflow

    def submit(self, workflow, **kwargs):
        """Submit a single HTCondor workflow.

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWorkflow`
            A single HTCondor workflow to submit.  run_id is updated after
            successful submission to WMS.
        **kwargs : `~typing.Any`
            Keyword arguments for the options.
        """
        dag = workflow.dag
        ver = version.parse(htc_version())

        # For workflow portability, internal paths are all relative. Hence
        # the DAG needs to be submitted to HTCondor from inside the submit
        # directory.
        with chdir(workflow.submit_path):
            try:
                if ver >= version.parse("8.9.3"):
                    sub = htc_create_submit_from_dag(dag.graph["dag_filename"], dag.graph["submit_options"])
                else:
                    sub = htc_create_submit_from_cmd(dag.graph["dag_filename"], dag.graph["submit_options"])
            except Exception:
                _LOG.error(
                    "Problems creating HTCondor submit object from filename: %s", dag.graph["dag_filename"]
                )
                raise

            _LOG.info("Submitting from directory: %s", os.getcwd())
            schedd_dag_info = htc_submit_dag(sub)
            if schedd_dag_info:
                write_dag_info(f"{dag.name}.info.json", schedd_dag_info)

                _, dag_info = schedd_dag_info.popitem()
                _, dag_ad = dag_info.popitem()

                dag.run_id = f"{dag_ad['ClusterId']}.{dag_ad['ProcId']}"
                workflow.run_id = dag.run_id
            else:
                raise RuntimeError("Submission failed: unable to retrieve DAGMan job information")

    def restart(self, wms_workflow_id):
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
                    "Hint: use run's submit directory as the id instead"
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
        rescue_file = htc_backup_files(wms_path, subdir="backups")
        if (wms_path / "subdags").exists():
            _update_rescue_file(rescue_file)

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

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False):
        """Query WMS for list of submitted WMS workflows/jobs.

        This should be a quick lookup function to create list of jobs for
        other functions.

        Parameters
        ----------
        wms_id : `int` or `str`, optional
            Id or path that can be used by WMS service to look up job.
        user : `str`, optional
            User whose submitted jobs should be listed.
        require_bps : `bool`, optional
            Whether to require jobs returned in list to be bps-submitted jobs.
        pass_thru : `str`, optional
            Information to pass through to WMS.
        is_global : `bool`, optional
            If set, all job queues (and their histories) will be queried for
            job information. Defaults to False which means that only the local
            job queue will be queried.

        Returns
        -------
        job_ids : `list` [`~typing.Any`]
            Only job ids to be used by cancel and other functions.  Typically
            this means top-level jobs (i.e., not children jobs).
        """
        _LOG.debug(
            "list_submitted_jobs params: wms_id=%s, user=%s, require_bps=%s, pass_thru=%s, is_global=%s",
            wms_id,
            user,
            require_bps,
            pass_thru,
            is_global,
        )

        # Determine which Schedds will be queried for job information.
        coll = htcondor.Collector()

        schedd_ads = []
        if is_global:
            schedd_ads.extend(coll.locateAll(htcondor.DaemonTypes.Schedd))
        else:
            schedd_ads.append(coll.locate(htcondor.DaemonTypes.Schedd))

        # Construct appropriate constraint expression using provided arguments.
        constraint = "False"
        if wms_id is None:
            if user is not None:
                constraint = f'(Owner == "{user}")'
        else:
            schedd_ad, cluster_id, id_type = _wms_id_to_cluster(wms_id)
            if cluster_id is not None:
                constraint = f"(DAGManJobId == {cluster_id} || ClusterId == {cluster_id})"

                # If provided id is either a submission path or a global id,
                # make sure the right Schedd will be queried regardless of
                # 'is_global' value.
                if id_type in {WmsIdType.GLOBAL, WmsIdType.PATH}:
                    schedd_ads = [schedd_ad]
        if require_bps:
            constraint += ' && (bps_isjob == "True")'
        if pass_thru:
            if "-forcex" in pass_thru:
                pass_thru_2 = pass_thru.replace("-forcex", "")
                if pass_thru_2 and not pass_thru_2.isspace():
                    constraint += f" && ({pass_thru_2})"
            else:
                constraint += f" && ({pass_thru})"

        # Create a list of scheduler daemons which need to be queried.
        schedds = {ad["Name"]: htcondor.Schedd(ad) for ad in schedd_ads}

        _LOG.debug("constraint = %s, schedds = %s", constraint, ", ".join(schedds))
        results = condor_q(constraint=constraint, schedds=schedds)

        # Prune child jobs where DAG job is in queue (i.e., aren't orphans).
        job_ids = []
        for job_info in results.values():
            for job_id, job_ad in job_info.items():
                _LOG.debug("job_id=%s DAGManJobId=%s", job_id, job_ad.get("DAGManJobId", "None"))
                if "DAGManJobId" not in job_ad:
                    job_ids.append(job_ad.get("GlobalJobId", job_id))
                else:
                    _LOG.debug("Looking for %s", f"{job_ad['DAGManJobId']}.0")
                    _LOG.debug("\tin jobs.keys() = %s", job_info.keys())
                    if f"{job_ad['DAGManJobId']}.0" not in job_info:  # orphaned job
                        job_ids.append(job_ad.get("GlobalJobId", job_id))

        _LOG.debug("job_ids = %s", job_ids)
        return job_ids

    def get_status(
        self,
        wms_workflow_id: str,
        hist: float = 1,
        is_global: bool = False,
    ) -> tuple[WmsStates, str]:
        """Return status of run based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `str`
            Limit to specific run based on id (queue id or path).
        hist : `float`, optional
            Limit history search to this many days. Defaults to 1.
        is_global : `bool`, optional
            If set, all job queues (and their histories) will be queried for
            job information. Defaults to False which means that only the local
            job queue will be queried.

        Returns
        -------
        state : `lsst.ctrl.bps.WmsStates`
            Status of single run from given information.
        message : `str`
            Extra message for status command to print.  This could be pointers
            to documentation or to WMS specific commands.
        """
        _LOG.debug("get_status: id=%s, hist=%s, is_global=%s", wms_workflow_id, hist, is_global)

        id_type = _wms_id_type(wms_workflow_id)
        _LOG.debug("id_type = %s", id_type.name)

        if id_type == WmsIdType.LOCAL:
            schedulers = _locate_schedds(locate_all=is_global)
            _LOG.debug("schedulers = %s", schedulers)
            state, message = _get_status_from_id(wms_workflow_id, hist, schedds=schedulers)
        elif id_type == WmsIdType.GLOBAL:
            schedulers = _locate_schedds(locate_all=True)
            _LOG.debug("schedulers = %s", schedulers)
            state, message = _get_status_from_id(wms_workflow_id, hist, schedds=schedulers)
        elif id_type == WmsIdType.PATH:
            state, message = _get_status_from_path(wms_workflow_id)
        else:
            state, message = WmsStates.UNKNOWN, "Invalid job id"
        _LOG.debug("state: %s, %s", state, message)

        return state, message

    def report(
        self,
        wms_workflow_id=None,
        user=None,
        hist=0,
        pass_thru=None,
        is_global=False,
        return_exit_codes=False,
    ):
        """Return run information based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `str`, optional
            Limit to specific run based on id.
        user : `str`, optional
            Limit results to runs for this user.
        hist : `float`, optional
            Limit history search to this many days. Defaults to 0.
        pass_thru : `str`, optional
            Constraints to pass through to HTCondor.
        is_global : `bool`, optional
            If set, all job queues (and their histories) will be queried for
            job information. Defaults to False which means that only the local
            job queue will be queried.
        return_exit_codes : `bool`, optional
            If set, return exit codes related to jobs with a
            non-success status. Defaults to False, which means that only
            the summary state is returned.

            Only applicable in the context of a WMS with associated
            handlers to return exit codes from jobs.

        Returns
        -------
        runs : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be pointers
            to documentation or to WMS specific commands.
        """
        if wms_workflow_id:
            id_type = _wms_id_type(wms_workflow_id)
            if id_type == WmsIdType.LOCAL:
                schedulers = _locate_schedds(locate_all=is_global)
                run_reports, message = _report_from_id(wms_workflow_id, hist, schedds=schedulers)
            elif id_type == WmsIdType.GLOBAL:
                schedulers = _locate_schedds(locate_all=True)
                run_reports, message = _report_from_id(wms_workflow_id, hist, schedds=schedulers)
            elif id_type == WmsIdType.PATH:
                run_reports, message = _report_from_path(wms_workflow_id)
            else:
                run_reports, message = {}, "Invalid job id"
        else:
            schedulers = _locate_schedds(locate_all=is_global)
            run_reports, message = _summary_report(user, hist, pass_thru, schedds=schedulers)
        _LOG.debug("report: %s, %s", run_reports, message)

        return list(run_reports.values()), message

    def cancel(self, wms_id, pass_thru=None):
        """Cancel submitted workflows/jobs.

        Parameters
        ----------
        wms_id : `str`
            Id or path of job that should be canceled.
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        -------
        deleted : `bool`
            Whether successful deletion or not.  Currently, if any doubt or any
            individual jobs not deleted, return False.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        _LOG.debug("Canceling wms_id = %s", wms_id)

        schedd_ad, cluster_id, _ = _wms_id_to_cluster(wms_id)

        if cluster_id is None:
            deleted = False
            message = "invalid id"
        else:
            _LOG.debug(
                "Canceling job managed by schedd_name = %s with cluster_id = %s",
                cluster_id,
                schedd_ad["Name"],
            )
            schedd = htcondor.Schedd(schedd_ad)

            constraint = f"ClusterId == {cluster_id}"
            if pass_thru is not None and "-forcex" in pass_thru:
                pass_thru_2 = pass_thru.replace("-forcex", "")
                if pass_thru_2 and not pass_thru_2.isspace():
                    constraint += f"&& ({pass_thru_2})"
                _LOG.debug("JobAction.RemoveX constraint = %s", constraint)
                results = schedd.act(htcondor.JobAction.RemoveX, constraint)
            else:
                if pass_thru:
                    constraint += f"&& ({pass_thru})"
                _LOG.debug("JobAction.Remove constraint = %s", constraint)
                results = schedd.act(htcondor.JobAction.Remove, constraint)
            _LOG.debug("Remove results: %s", results)

            if results["TotalSuccess"] > 0 and results["TotalError"] == 0:
                deleted = True
                message = ""
            else:
                deleted = False
                if results["TotalSuccess"] == 0 and results["TotalError"] == 0:
                    message = "no such bps job in batch queue"
                else:
                    message = f"unknown problems deleting: {results}"

        _LOG.debug("deleted: %s; message = %s", deleted, message)
        return deleted, message

    def ping(self, pass_thru):
        """Check whether WMS services are up, reachable, and can authenticate
        if authentication is required.

        The services to be checked are those needed for submit, report, cancel,
        restart, but ping cannot guarantee whether jobs would actually run
        successfully.

        Parameters
        ----------
        pass_thru : `str`, optional
            Information to pass through to WMS.

        Returns
        -------
        status : `int`
            0 for success, non-zero for failure.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        coll = htcondor.Collector()
        secman = htcondor.SecMan()
        status = 0
        message = ""
        _LOG.info("Not verifying that compute resources exist.")
        try:
            for daemon_type in [htcondor.DaemonTypes.Schedd, htcondor.DaemonTypes.Collector]:
                _ = secman.ping(coll.locate(daemon_type))
        except htcondor.HTCondorLocateError:
            status = 1
            message = f"Could not locate {daemon_type} service."
        except htcondor.HTCondorIOError:
            status = 1
            message = f"Permission problem with {daemon_type} service."
        return status, message
