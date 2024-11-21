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

"""Unit tests for the report-related functions."""

import logging
import os
import unittest
from pathlib import Path
from shutil import copy2

import htcondor
from lsst.ctrl.bps import WmsStates
from lsst.ctrl.bps.htcondor.lssthtc import MISSING_ID
from lsst.ctrl.bps.htcondor.report import (
    JobStatus,
    NodeStatus,
    _get_exit_code_summary,
    _get_info_from_path,
    _get_state_counts_from_dag_job,
    _htc_node_status_to_wms_state,
    _htc_status_to_wms_state,
)
from lsst.utils.tests import temporaryDirectory

logger = logging.getLogger("lsst.ctrl.bps.htcondor")

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class GetExitCodeSummaryTestCase(unittest.TestCase):
    """Test the function responsible for creating exit code summary."""

    def setUp(self):
        self.jobs = {
            "1.0": {
                "JobStatus": htcondor.JobStatus.IDLE,
                "bps_job_label": "foo",
            },
            "2.0": {
                "JobStatus": htcondor.JobStatus.RUNNING,
                "bps_job_label": "foo",
            },
            "3.0": {
                "JobStatus": htcondor.JobStatus.REMOVED,
                "bps_job_label": "foo",
            },
            "4.0": {
                "ExitCode": 0,
                "ExitBySignal": False,
                "JobStatus": htcondor.JobStatus.COMPLETED,
                "bps_job_label": "bar",
            },
            "5.0": {
                "ExitCode": 1,
                "ExitBySignal": False,
                "JobStatus": htcondor.JobStatus.COMPLETED,
                "bps_job_label": "bar",
            },
            "6.0": {
                "ExitBySignal": True,
                "ExitSignal": 11,
                "JobStatus": htcondor.JobStatus.HELD,
                "bps_job_label": "baz",
            },
            "7.0": {
                "ExitBySignal": False,
                "ExitCode": 42,
                "JobStatus": htcondor.JobStatus.HELD,
                "bps_job_label": "baz",
            },
            "8.0": {
                "JobStatus": htcondor.JobStatus.TRANSFERRING_OUTPUT,
                "bps_job_label": "qux",
            },
            "9.0": {
                "JobStatus": htcondor.JobStatus.SUSPENDED,
                "bps_job_label": "qux",
            },
        }

    def tearDown(self):
        pass

    def testMainScenario(self):
        actual = _get_exit_code_summary(self.jobs)
        expected = {"foo": [], "bar": [1], "baz": [11, 42], "qux": []}
        self.assertEqual(actual, expected)

    def testUnknownStatus(self):
        jobs = {
            "1.0": {
                "JobStatus": -1,
                "bps_job_label": "foo",
            }
        }
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            _get_exit_code_summary(jobs)
        self.assertIn("lsst.ctrl.bps.htcondor", cm.records[0].name)
        self.assertIn("Unknown", cm.output[0])
        self.assertIn("JobStatus", cm.output[0])

    def testUnknownKey(self):
        jobs = {
            "1.0": {
                "JobStatus": htcondor.JobStatus.COMPLETED,
                "UnknownKey": None,
                "bps_job_label": "foo",
            }
        }
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            _get_exit_code_summary(jobs)
        self.assertIn("lsst.ctrl.bps.htcondor", cm.records[0].name)
        self.assertIn("Attribute", cm.output[0])
        self.assertIn("not found", cm.output[0])


class HtcNodeStatusToWmsStateTestCase(unittest.TestCase):
    """Test assigning WMS state base on HTCondor node status."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNotReady(self):
        job = {"NodeStatus": NodeStatus.NOT_READY}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.UNREADY)

    def testReady(self):
        job = {"NodeStatus": NodeStatus.READY}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.READY)

    def testPrerun(self):
        job = {"NodeStatus": NodeStatus.PRERUN}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.MISFIT)

    def testSubmittedHeld(self):
        job = {
            "NodeStatus": NodeStatus.SUBMITTED,
            "JobProcsHeld": 1,
            "StatusDetails": "",
            "JobProcsQueued": 0,
        }
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.HELD)

    def testSubmittedRunning(self):
        job = {
            "NodeStatus": NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "not_idle",
            "JobProcsQueued": 0,
        }
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.RUNNING)

    def testSubmittedPending(self):
        job = {
            "NodeStatus": NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "",
            "JobProcsQueued": 1,
        }
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testPostrun(self):
        job = {"NodeStatus": NodeStatus.POSTRUN}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.MISFIT)

    def testDone(self):
        job = {"NodeStatus": NodeStatus.DONE}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)

    def testErrorDagmanSuccess(self):
        job = {"NodeStatus": NodeStatus.ERROR, "StatusDetails": "DAGMAN error 0"}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)

    def testErrorDagmanFailure(self):
        job = {"NodeStatus": NodeStatus.ERROR, "StatusDetails": "DAGMAN error 1"}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.FAILED)

    def testFutile(self):
        job = {"NodeStatus": NodeStatus.FUTILE}
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PRUNED)

    def testDeletedJob(self):
        job = {
            "NodeStatus": NodeStatus.ERROR,
            "StatusDetails": "HTCondor reported ULOG_JOB_ABORTED event for job proc (1.0.0)",
            "JobProcsQueued": 0,
        }
        result = _htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.DELETED)


class HtcStatusToWmsStateTestCase(unittest.TestCase):
    """Test assigning WMS state base on HTCondor status."""

    def testJobStatus(self):
        job = {
            "ClusterId": 1,
            "JobStatus": htcondor.JobStatus.IDLE,
            "bps_job_label": "foo",
        }
        result = _htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testNodeStatus(self):
        # Hold/Release test case
        job = {
            "ClusterId": 1,
            "JobStatus": 0,
            "NodeStatus": NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "",
            "JobProcsQueued": 1,
        }
        result = _htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testNeitherStatus(self):
        job = {"ClusterId": 1}
        result = _htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.MISFIT)

    def testRetrySuccess(self):
        job = {
            "NodeStatus": 5,
            "Node": "8e62c569-ae2e-44e8-be36-d1aee333a129_isr_903342_10",
            "RetryCount": 0,
            "ClusterId": 851,
            "ProcId": 0,
            "MyType": "JobTerminatedEvent",
            "EventTypeNumber": 5,
            "HoldReasonCode": 3,
            "HoldReason": "Job raised a signal 9. Handling signal as if job has gone over memory limit.",
            "HoldReasonSubCode": 34,
            "ToE": {
                "ExitBySignal": False,
                "ExitCode": 0,
            },
            "JobStatus": JobStatus.COMPLETED,
            "ExitBySignal": False,
            "ExitCode": 0,
        }
        result = _htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)


class GetStateCountsFromDagJobTestCase(unittest.TestCase):
    """Test counting number of jobs per WMS state."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testCounts(self):
        job = {
            "DAG_NodesDone": 1,
            "DAG_JobsHeld": 2,
            "DAG_NodesFailed": 3,
            "DAG_NodesFutile": 4,
            "DAG_NodesQueued": 5,
            "DAG_NodesReady": 0,
            "DAG_NodesUnready": 7,
            "DAG_NodesTotal": 22,
        }

        truth = {
            WmsStates.SUCCEEDED: 1,
            WmsStates.HELD: 2,
            WmsStates.UNREADY: 7,
            WmsStates.READY: 0,
            WmsStates.FAILED: 3,
            WmsStates.PRUNED: 4,
            WmsStates.MISFIT: 0,
        }

        total, result = _get_state_counts_from_dag_job(job)
        self.assertEqual(total, 22)
        self.assertEqual(result, truth)


class GetInfoFromPathTestCase(unittest.TestCase):
    """Test _get_info_from_path function."""

    def test_tmpdir_abort(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_tmpdir_abort.dag.dagman.out", tmp_dir)
            wms_workflow_id, jobs, message = _get_info_from_path(tmp_dir)
            self.assertEqual(wms_workflow_id, MISSING_ID)
            self.assertEqual(jobs, {})
            self.assertIn("Cannot submit from /tmp", message)

    def test_no_dagman_messages(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_no_messages.dag.dagman.out", tmp_dir)
            wms_workflow_id, jobs, message = _get_info_from_path(tmp_dir)
            self.assertEqual(wms_workflow_id, MISSING_ID)
            self.assertEqual(jobs, {})
            self.assertIn("Could not find HTCondor files", message)

    def test_successful_run(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag", tmp_dir)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.dagman.log", tmp_dir)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.dagman.out", tmp_dir)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.nodes.log", tmp_dir)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.node_status", tmp_dir)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.info.json", tmp_dir)
            wms_workflow_id, jobs, message = _get_info_from_path(tmp_dir)
            self.assertEqual(wms_workflow_id, "1163.0")
            self.assertEqual(len(jobs), 6)  # dag, pipetaskInit, 3 science, finalJob
            self.assertEqual(message, "")

    def test_relative_path(self):
        orig_dir = Path.cwd()
        with temporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            abs_path = Path(tmp_dir).resolve() / "subdir"
            abs_path.mkdir()
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag", abs_path)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.dagman.log", abs_path)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.dagman.out", abs_path)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.dag.nodes.log", abs_path)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.node_status", abs_path)
            copy2(f"{TESTDIR}/data/test_pipelines_check_20240727T003507Z.info.json", abs_path)
            wms_workflow_id, jobs, message = _get_info_from_path("subdir")
            self.assertEqual(wms_workflow_id, "1163.0")
            self.assertEqual(len(jobs), 6)  # dag, pipetaskInit, 3 science, finalJob
            self.assertEqual(message, "")
            self.assertEqual(jobs["1163.0"]["Iwd"], str(abs_path))
            os.chdir(orig_dir)
