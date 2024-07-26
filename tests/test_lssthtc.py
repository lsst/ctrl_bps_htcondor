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
"""Unit tests for classes and functions in lssthtc.py"""

import logging
import pathlib
import tempfile
import unittest

import htcondor
from lsst.ctrl.bps.htcondor import lssthtc

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class TestLsstHtc(unittest.TestCase):
    """Test basic usage."""

    def testHtcEscapeInt(self):
        self.assertEqual(lssthtc.htc_escape(100), 100)

    def testHtcEscapeDouble(self):
        self.assertEqual(lssthtc.htc_escape('"double"'), '""double""')

    def testHtcEscapeSingle(self):
        self.assertEqual(lssthtc.htc_escape("'single'"), "''single''")

    def testHtcEscapeNoSideEffect(self):
        val = "'val'"
        self.assertEqual(lssthtc.htc_escape(val), "''val''")
        self.assertEqual(val, "'val'")

    def testHtcEscapeQuot(self):
        self.assertEqual(lssthtc.htc_escape("&quot;val&quot;"), '"val"')

    def testHtcVersion(self):
        ver = lssthtc.htc_version()
        self.assertRegex(ver, r"^\d+\.\d+\.\d+$")


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
        lssthtc._tweak_log_info(self.log_name, self.job)
        self.assertEqual(self.job["ClusterId"], self.job["Cluster"])
        self.assertEqual(self.job["ProcId"], self.job["Proc"])
        self.assertEqual(self.job["Iwd"], str(self.log_name.parent))
        self.assertEqual(self.job["Owner"], self.log_name.owner())

    def testJobStatusAssignmentJobAbortedEvent(self):
        job = self.job | {"MyType": "JobAbortedEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.REMOVED)

    def testJobStatusAssignmentExecuteEvent(self):
        job = self.job | {"MyType": "ExecuteEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.RUNNING)

    def testJobStatusAssignmentSubmitEvent(self):
        job = self.job | {"MyType": "SubmitEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.IDLE)

    def testJobStatusAssignmentJobHeldEvent(self):
        job = self.job | {"MyType": "JobHeldEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.HELD)

    def testJobStatusAssignmentJobTerminatedEvent(self):
        job = self.job | {"MyType": "JobTerminatedEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.COMPLETED)

    def testJobStatusAssignmentPostScriptTerminatedEvent(self):
        job = self.job | {"MyType": "PostScriptTerminatedEvent"}
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertTrue("JobStatus" in job)
        self.assertEqual(job["JobStatus"], htcondor.JobStatus.COMPLETED)

    def testAddingExitStatusSuccess(self):
        job = self.job | {
            "MyType": "JobTerminatedEvent",
            "ToE": {"ExitBySignal": False, "ExitCode": 1},
        }
        lssthtc._tweak_log_info(self.log_name, job)
        self.assertIn("ExitBySignal", job)
        self.assertIs(job["ExitBySignal"], False)
        self.assertIn("ExitCode", job)
        self.assertEqual(job["ExitCode"], 1)

    def testAddingExitStatusFailure(self):
        job = self.job | {
            "MyType": "JobHeldEvent",
        }
        with self.assertLogs(logger=logger, level="ERROR") as cm:
            lssthtc._tweak_log_info(self.log_name, job)
        self.assertIn("Could not determine exit status", cm.output[0])

    def testLoggingUnknownLogEvent(self):
        job = self.job | {"MyType": "Foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            lssthtc._tweak_log_info(self.log_name, job)
        self.assertIn("Unknown log event", cm.output[1])

    def testMissingKey(self):
        job = self.job
        del job["Cluster"]
        with self.assertRaises(KeyError) as cm:
            lssthtc._tweak_log_info(self.log_name, job)
        self.assertEqual(str(cm.exception), "'Cluster'")


if __name__ == "__main__":
    unittest.main()
