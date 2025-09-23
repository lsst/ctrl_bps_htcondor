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

"""Unit tests for the common utility functions."""

import logging
import os
import unittest
from pathlib import Path

import htcondor

from lsst.ctrl.bps import (
    WmsStates,
)
from lsst.ctrl.bps.htcondor import common_utils
from lsst.utils.tests import temporaryDirectory

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class HtcNodeStatusToWmsStateTestCase(unittest.TestCase):
    """Test assigning WMS state base on HTCondor node status."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNotReady(self):
        job = {"NodeStatus": common_utils.NodeStatus.NOT_READY}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.UNREADY)

    def testReady(self):
        job = {"NodeStatus": common_utils.NodeStatus.READY}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.READY)

    def testPrerun(self):
        job = {"NodeStatus": common_utils.NodeStatus.PRERUN}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.MISFIT)

    def testSubmittedHeld(self):
        job = {
            "NodeStatus": common_utils.NodeStatus.SUBMITTED,
            "JobProcsHeld": 1,
            "StatusDetails": "",
            "JobProcsQueued": 0,
        }
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.HELD)

    def testSubmittedRunning(self):
        job = {
            "NodeStatus": common_utils.NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "not_idle",
            "JobProcsQueued": 0,
        }
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.RUNNING)

    def testSubmittedPending(self):
        job = {
            "NodeStatus": common_utils.NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "",
            "JobProcsQueued": 1,
        }
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testPostrun(self):
        job = {"NodeStatus": common_utils.NodeStatus.POSTRUN}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.MISFIT)

    def testDone(self):
        job = {"NodeStatus": common_utils.NodeStatus.DONE}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)

    def testErrorDagmanSuccess(self):
        job = {"NodeStatus": common_utils.NodeStatus.ERROR, "StatusDetails": "DAGMAN error 0"}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)

    def testErrorDagmanFailure(self):
        job = {"NodeStatus": common_utils.NodeStatus.ERROR, "StatusDetails": "DAGMAN error 1"}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.FAILED)

    def testFutile(self):
        job = {"NodeStatus": common_utils.NodeStatus.FUTILE}
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PRUNED)

    def testDeletedJob(self):
        job = {
            "NodeStatus": common_utils.NodeStatus.ERROR,
            "StatusDetails": "HTCondor reported ULOG_JOB_ABORTED event for job proc (1.0.0)",
            "JobProcsQueued": 0,
        }
        result = common_utils._htc_node_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.DELETED)


class HtcStatusToWmsStateTestCase(unittest.TestCase):
    """Test assigning WMS state base on HTCondor status."""

    def testJobStatus(self):
        job = {
            "ClusterId": 1,
            "JobStatus": htcondor.JobStatus.IDLE,
            "bps_job_label": "foo",
        }
        result = common_utils._htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testNodeStatus(self):
        # Hold/Release test case
        job = {
            "ClusterId": 1,
            "JobStatus": None,
            "NodeStatus": common_utils.NodeStatus.SUBMITTED,
            "JobProcsHeld": 0,
            "StatusDetails": "",
            "JobProcsQueued": 1,
        }
        result = common_utils._htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.PENDING)

    def testNeitherStatus(self):
        job = {"ClusterId": 1}
        result = common_utils._htc_status_to_wms_state(job)
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
            "JobStatus": htcondor.JobStatus.COMPLETED,
            "ExitBySignal": False,
            "ExitCode": 0,
        }
        result = common_utils._htc_status_to_wms_state(job)
        self.assertEqual(result, WmsStates.SUCCEEDED)


class WmsIdToDirTestCase(unittest.TestCase):
    """Test _wms_id_to_dir function."""

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.common_utils._wms_id_type")
    def testInvalidIdType(self, _wms_id_type_mock):
        _wms_id_type_mock.return_value = common_utils.WmsIdType.UNKNOWN
        with self.assertRaises(TypeError) as cm:
            _, _ = common_utils._wms_id_to_dir("not_used")
        self.assertIn("Invalid job id type", str(cm.exception))

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.common_utils._wms_id_type")
    def testAbsPathId(self, mock_wms_id_type):
        mock_wms_id_type.return_value = common_utils.WmsIdType.PATH
        with temporaryDirectory() as tmp_dir:
            wms_path, id_type = common_utils._wms_id_to_dir(tmp_dir)
            self.assertEqual(id_type, common_utils.WmsIdType.PATH)
            self.assertEqual(Path(tmp_dir).resolve(), wms_path)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.common_utils._wms_id_type")
    def testRelPathId(self, _wms_id_type_mock):
        _wms_id_type_mock.return_value = common_utils.WmsIdType.PATH
        orig_dir = Path.cwd()
        with temporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            abs_path = Path(tmp_dir) / "newdir"
            abs_path.mkdir()
            wms_path, id_type = common_utils._wms_id_to_dir("newdir")
            self.assertEqual(id_type, common_utils.WmsIdType.PATH)
            self.assertEqual(abs_path.resolve(), wms_path)
            os.chdir(orig_dir)


class WmsIdTypeTestCase(unittest.TestCase):
    """Test _wms_id_type function."""

    def testIntId(self):
        id_type = common_utils._wms_id_type("4")
        self.assertEqual(id_type, common_utils.WmsIdType.LOCAL)

    def testPathId(self):
        with temporaryDirectory() as tmp_dir:
            id_type = common_utils._wms_id_type(str(tmp_dir))
            self.assertEqual(id_type, common_utils.WmsIdType.PATH)

    def testGlobalId(self):
        id_type = common_utils._wms_id_type("testmachine#5044.0#1757720957")
        self.assertEqual(id_type, common_utils.WmsIdType.GLOBAL)

    def testUnknownType(self):
        id_type = common_utils._wms_id_type(["bad param"])
        self.assertEqual(id_type, common_utils.WmsIdType.UNKNOWN)


if __name__ == "__main__":
    unittest.main()
