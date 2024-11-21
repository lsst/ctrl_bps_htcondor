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

"""Representation for a HTCondor workflow."""

__all__ = ["HTCondorWorkflow"]


import logging
import os
from collections import Counter, defaultdict

from lsst.ctrl.bps import BaseWmsWorkflow, GenericWorkflow, GenericWorkflowJob, GenericWorkflowNodeType
from lsst.ctrl.bps.bps_utils import create_count_summary

from .lssthtc import HTCDag, HTCJob
from .prepare import _create_job, _gather_site_values

_LOG = logging.getLogger(__name__)


class HTCondorWorkflow(BaseWmsWorkflow):
    """Single HTCondor workflow.

    Parameters
    ----------
    name : `str`
        Unique name for Workflow used when naming files.
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information.
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.dag = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited
        htc_workflow = cls(generic_workflow.name, config)
        htc_workflow.dag = HTCDag(name=generic_workflow.name)

        _LOG.debug("htcondor dag attribs %s", generic_workflow.run_attrs)
        htc_workflow.dag.add_attribs(generic_workflow.run_attrs)
        htc_workflow.dag.add_attribs(
            {
                "bps_wms_service": service_class,
                "bps_wms_workflow": f"{cls.__module__}.{cls.__name__}",
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
        site_values = {}  # cache compute site specific values to reduce config lookups
        workflow_job_counts = Counter()
        for job_name in generic_workflow:
            gwjob = generic_workflow.get_job(job_name)
            if gwjob.node_type != GenericWorkflowNodeType.NOOP:
                if gwjob.compute_site not in site_values:
                    site_values[gwjob.compute_site] = _gather_site_values(config, gwjob.compute_site)
                htc_job = _create_job(
                    subdir_template[gwjob.label],
                    site_values[gwjob.compute_site],
                    generic_workflow,
                    gwjob,
                    out_prefix,
                )
                workflow_job_counts["payload"] += 1
            else:
                htc_job = HTCJob(f"wms_{gwjob.name}", label=gwjob.label)
                htc_job.add_job_attrs({"bps_job_name": gwjob.name, "bps_job_label": gwjob.label})
                workflow_job_counts["noop"] += 1
            _LOG.debug("adding job %s %s", htc_job.name, htc_job.label)
            htc_workflow.dag.add_job(htc_job)

        # Add job dependencies to the DAG (be careful with wms_ jobs)
        for job_name in generic_workflow:
            gwjob = generic_workflow.get_job(job_name)
            successor_jobs = [generic_workflow.get_job(j) for j in generic_workflow.successors(job_name)]
            parent_name = (
                f"wms_{gwjob.name}" if gwjob.node_type != GenericWorkflowNodeType.PAYLOAD else gwjob.name
            )
            children_names = [
                f"wms_{sjob.name}" if sjob.node_type != GenericWorkflowNodeType.PAYLOAD else sjob.name
                for sjob in successor_jobs
            ]
            htc_workflow.dag.add_job_relationships([parent_name], children_names)

        # If final job exists in generic workflow, create DAG final job
        final = generic_workflow.get_final()
        if final and isinstance(final, GenericWorkflowJob):
            if final.compute_site and final.compute_site not in site_values:
                site_values[final.compute_site] = _gather_site_values(config, final.compute_site)
            final_htjob = _create_job(
                subdir_template[final.label],
                site_values[final.compute_site],
                generic_workflow,
                final,
                out_prefix,
            )
            if "post" not in final_htjob.dagcmds:
                final_htjob.dagcmds["post"] = (
                    f"{os.path.dirname(__file__)}/final_post.sh {final.name} $DAG_STATUS $RETURN"
                )
            htc_workflow.dag.add_final_job(final_htjob)
            workflow_job_counts["payload"] += 1
        elif final and isinstance(final, GenericWorkflow):
            raise NotImplementedError("HTCondor plugin does not support a workflow as the final job")
        elif final:
            return TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final)})")

        htc_workflow.dag.add_attribs({"workflow_job_summary": create_count_summary(workflow_job_counts)})

        return htc_workflow

    def write(self, out_prefix):
        """Output HTCondor DAGMan files needed for workflow submission.

        Parameters
        ----------
        out_prefix : `str`
            Directory prefix for HTCondor files.
        """
        self.submit_path = out_prefix
        os.makedirs(out_prefix, exist_ok=True)

        # Write down the workflow in HTCondor format.
        self.dag.write(out_prefix, "jobs/{self.label}")
