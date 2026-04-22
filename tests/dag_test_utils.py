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

"""Utility functions for ctrl_bps_htcondor tests."""

import logging
from pathlib import Path

from lsst.ctrl.bps.htcondor.lssthtc import HTC_VALID_JOB_KEYS, HTCDag, HTCJob, RestrictedDict

_LOG = logging.getLogger(__name__)


def make_lazy_dag(name: str, final: bool = True) -> tuple[HTCDag, list[str]]:
    """Manually make a simple HTCDag with a lazy subdag for unit testing.

    Parameters
    ----------
    name : `str`
        Name to use for HTCDag.
    final : `bool`, optional
        Whether to include a FINAL job.  Defaults to True.

    Returns
    -------
    dag : `lsst.ctrl.bps.htcondor.lssthtc.HTCDag`
        A simple HTCDag with a lazy subdag for testing.
    filelist : `list` [`str`]
        A list of files with relative paths that should be created
        when DAG is written.
    """
    dag = HTCDag(name=name)

    job_cmds = RestrictedDict(HTC_VALID_JOB_KEYS, {"executable": "/usr/bin/uptime"})
    job_attrs = {"bps_run": "test_run"}

    job = HTCJob("job1", "label1", job_cmds, initattrs=job_attrs)
    subdir = Path(f"jobs/{job.label}")
    job.subdir = subdir
    job.subfile = f"{job.name}.sub"
    job.add_dag_cmds({"dir": subdir})
    dag.add_node("job1", data=job)

    job = HTCJob("make_subdag", "label2", job_cmds, initattrs=job_attrs)
    subdir = Path(f"jobs/{job.label}")
    job.subdir = subdir
    job.subfile = f"{job.name}.sub"
    job.add_dag_cmds({"dir": subdir})
    dag.add_node(job.name, data=job)
    dag.add_edge("job1", job.name)

    job = HTCJob("lazy_subdag", "lazy_label", job_cmds, initattrs=job_attrs)
    job.subfile = "lazy_subdag.dag.condor.sub"
    job.dag = HTCDag(name="lazy_subdag")
    dag.add_node(job.name, data=job)
    dag.add_edge("make_subdag", job.name)

    job = HTCJob("job4", "label4", job_cmds, initattrs=job_attrs)
    subdir = Path(f"jobs/{job.label}")
    job.subdir = subdir
    job.subfile = f"{job.name}.sub"
    job.add_dag_cmds({"dir": subdir})
    dag.add_node(job.name, data=job)
    dag.add_edge("lazy_subdag", job.name)

    filelist = [
        f"{name}.dag",
        "lazy_subdag.dag.condor.sub",
        "jobs/label1/job1.sub",
        "jobs/label2/make_subdag.sub",
        "jobs/label4/job4.sub",
    ]

    if final:
        job = HTCJob("finalJob", "finalJob", job_cmds, initattrs=job_attrs)
        subdir = Path(f"jobs/{job.label}")
        job.subdir = subdir
        job.subfile = f"{job.name}.sub"
        job.add_dag_cmds({"dir": subdir})
        dag.graph["final_job"] = job
        filelist.append("jobs/finalJob/finalJob.sub")

    return dag, filelist
