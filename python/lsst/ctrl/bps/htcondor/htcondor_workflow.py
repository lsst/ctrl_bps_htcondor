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

"""Class and utility functions at the workflow level."""

__all__ = ["HTCondorWorkflow"]


import logging
import os

from lsst.ctrl.bps import (
    BaseWmsWorkflow,
)

from .prepare_utils import _generic_workflow_to_htcondor_dag

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
        htc_workflow.dag = _generic_workflow_to_htcondor_dag(config, generic_workflow, out_prefix)

        _LOG.debug("htcondor dag attribs %s", generic_workflow.run_attrs)
        # Add extra attributes to top most DAG.
        htc_workflow.dag.add_attribs(
            {
                "bps_wms_service": service_class,
                "bps_wms_workflow": f"{cls.__module__}.{cls.__name__}",
            }
        )

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
        self.dag.write(out_prefix, job_subdir="jobs/{self.label}")
