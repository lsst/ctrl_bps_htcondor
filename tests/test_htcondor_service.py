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

"""Unit tests for the HTCondor WMS service class and related functions."""

import logging
import pathlib
import tempfile
import unittest

import htcondor
from lsst.ctrl.bps import WmsStates
from lsst.ctrl.bps.htcondor.htcondor_service import (
    NodeStatus,
    _get_exit_code_summary,
    _htc_node_status_to_wms_state,
)
from lsst.ctrl.bps.htcondor.lssthtc import _tweak_log_info

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


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


class TweakJobInfoTestCase(unittest.TestCase):
    """Test the function responsible for massaging job information."""

    def setUp(self):
        self.log_file = tempfile.NamedTemporaryFile(prefix="test_", suffix=".log")
        self.log_name = pathlib.Path(self.log_file.name)
        self.job = {
            "Cluster": 1,
            "Proc": 0,
            "Iwd": str(self.log_name.parent),
            "Owner": self.log_name.owner(),
            "MyType": None,
            "TerminatedNormally": True,
        }

    def tearDown(self):
        self.log_file.close()

    def testDirectAssignments(self):
        _tweak_log_info(self.log_name, self.job)
        self.assertEqual(self.job["ClusterId"], self.job["Cluster"])
        self.assertEqual(self.job["ProcId"], self.job["Proc"])
        self.assertEqual(self.job["Iwd"], str(self.log_name.parent))
        self.assertEqual(self.job["Owner"], self.log_name.owner())

    def testJobStatusAssignmentJobAbortedEvent(self):
        job = self.job | {"MyType": "JobAbortedEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.REMOVED)

    def testJobStatusAssignmentExecuteEvent(self):
        job = self.job | {"MyType": "ExecuteEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.RUNNING)

    def testJobStatusAssignmentSubmitEvent(self):
        job = self.job | {"MyType": "SubmitEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.IDLE)

    def testJobStatusAssignmentJobHeldEvent(self):
        job = self.job | {"MyType": "JobHeldEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.HELD)

    def testJobStatusAssignmentJobTerminatedEvent(self):
        job = self.job | {"MyType": "JobTerminatedEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.COMPLETED)

    def testJobStatusAssignmentPostScriptTerminatedEvent(self):
        job = self.job | {"MyType": "PostScriptTerminatedEvent"}
        _tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.COMPLETED)

    def testAddingExitStatusSuccess(self):
        job = self.job | {
            "MyType": "JobTerminatedEvent",
            "ToE": {"ExitBySignal": False, "ExitCode": 1},
        }
        _tweak_log_info(self.log_name, job)
        self.assertIn("ExitBySignal", job)
        self.assertIs(job["ExitBySignal"], False)
        self.assertIn("ExitCode", job)
        self.assertEqual(job["ExitCode"], 1)

    def testAddingExitStatusFailure(self):
        job = self.job | {
            "MyType": "JobHeldEvent",
        }
        with self.assertLogs(logger=logger, level="ERROR") as cm:
            _tweak_log_info(self.log_name, job)
        self.assertIn("Could not determine exit status", cm.output[0])

    def testLoggingUnknownLogEvent(self):
        job = self.job | {"MyType": "Foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            _tweak_log_info(self.log_name, job)
        self.assertIn("Unknown log event", cm.output[1])

    def testMissingKey(self):
        job = self.job
        del job["Cluster"]
        with self.assertRaises(KeyError) as cm:
            _tweak_log_info(self.log_name, job)
        self.assertEqual(str(cm.exception), "'Cluster'")
