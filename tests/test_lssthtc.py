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
"""Unit tests for classes and functions in lssthtc.py."""

import io
import logging
import os
import pathlib
import stat
import sys
import tempfile
import unittest
from shutil import copy2, copytree, ignore_patterns, rmtree, which

import htcondor

from lsst.ctrl.bps.htcondor import lssthtc
from lsst.utils.tests import temporaryDirectory

logger = logging.getLogger("lsst.ctrl.bps.htcondor")
TESTDIR = os.path.abspath(os.path.dirname(__file__))


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


class HtcCheckDagmanOutputTestCase(unittest.TestCase):
    """Test htc_check_dagman_output function."""

    def test_missing_output_file(self):
        with temporaryDirectory() as tmp_dir:
            with self.assertRaises(FileNotFoundError):
                _ = lssthtc.htc_check_dagman_output(tmp_dir)

    def test_permissions_output_file(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_tmpdir_abort.dag.dagman.out", tmp_dir)
            os.chmod(f"{tmp_dir}/test_tmpdir_abort.dag.dagman.out", 0o200)
            print(os.stat(f"{tmp_dir}/test_tmpdir_abort.dag.dagman.out"))
            results = lssthtc.htc_check_dagman_output(tmp_dir)
            os.chmod(f"{tmp_dir}/test_tmpdir_abort.dag.dagman.out", 0o600)
            self.assertIn("Could not read dagman output file", results)

    def test_submit_failure(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/bad_submit.dag.dagman.out", tmp_dir)
            results = lssthtc.htc_check_dagman_output(tmp_dir)
            self.assertIn("Warn: Job submission issues (last: ", results)

    def test_tmpdir_abort(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_tmpdir_abort.dag.dagman.out", tmp_dir)
            results = lssthtc.htc_check_dagman_output(tmp_dir)
            self.assertIn("Cannot submit from /tmp", results)

    def test_no_messages(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_no_messages.dag.dagman.out", tmp_dir)
            results = lssthtc.htc_check_dagman_output(tmp_dir)
            self.assertEqual("", results)


class SummarizeDagTestCase(unittest.TestCase):
    """Test summarize_dag function."""

    def test_no_dag_file(self):
        with temporaryDirectory() as tmp_dir:
            summary, job_name_to_pipetask, job_name_to_type = lssthtc.summarize_dag(tmp_dir)
            self.assertFalse(len(job_name_to_pipetask))
            self.assertFalse(len(job_name_to_type))
            self.assertFalse(summary)

    def test_success(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/good.dag", tmp_dir)
            summary, job_name_to_label, job_name_to_type = lssthtc.summarize_dag(tmp_dir)
            self.assertEqual(summary, "pipetaskInit:1;label1:1;label2:1;label3:1;finalJob:1")
            self.assertEqual(
                job_name_to_label,
                {
                    "pipetaskInit": "pipetaskInit",
                    "0682f8f9-12f0-40a5-971e-8b30c7231e5c_label1_val1_val2": "label1",
                    "d0305e2d-f164-4a85-bd24-06afe6c84ed9_label2_val1_val2": "label2",
                    "2806ecc9-1bba-4362-8fff-ab4e6abb9f83_label3_val1_val2": "label3",
                    "finalJob": "finalJob",
                },
            )
            self.assertEqual(
                job_name_to_type,
                {
                    "pipetaskInit": lssthtc.WmsNodeType.PAYLOAD,
                    "0682f8f9-12f0-40a5-971e-8b30c7231e5c_label1_val1_val2": lssthtc.WmsNodeType.PAYLOAD,
                    "d0305e2d-f164-4a85-bd24-06afe6c84ed9_label2_val1_val2": lssthtc.WmsNodeType.PAYLOAD,
                    "2806ecc9-1bba-4362-8fff-ab4e6abb9f83_label3_val1_val2": lssthtc.WmsNodeType.PAYLOAD,
                    "finalJob": lssthtc.WmsNodeType.FINAL,
                },
            )

    def test_service(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_problems/tiny_problems.dag", tmp_dir)
            summary, job_name_to_label, job_name_to_type = lssthtc.summarize_dag(tmp_dir)
            self.assertEqual(summary, "pipetaskInit:1;label1:2;label2:2;finalJob:1")
            self.assertEqual(
                job_name_to_label,
                {
                    "pipetaskInit": "pipetaskInit",
                    "057c8caf-66f6-4612-abf7-cdea5b666b1b_label1_val1a_val2b": "label1",
                    "4a7f478b-2e9b-435c-a730-afac3f621658_label1_val1a_val2a": "label1",
                    "40040b97-606d-4997-98d3-e0493055fe7e_label2_val1a_val2b": "label2",
                    "696ee50d-e711-40d6-9caf-ee29ae4a656d_label2_val1a_val2a": "label2",
                    "finalJob": "finalJob",
                    "provisioningJob": "provisioningJob",
                },
            )
            self.assertEqual(
                job_name_to_type,
                {
                    "pipetaskInit": lssthtc.WmsNodeType.PAYLOAD,
                    "057c8caf-66f6-4612-abf7-cdea5b666b1b_label1_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "4a7f478b-2e9b-435c-a730-afac3f621658_label1_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "40040b97-606d-4997-98d3-e0493055fe7e_label2_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "696ee50d-e711-40d6-9caf-ee29ae4a656d_label2_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "finalJob": lssthtc.WmsNodeType.FINAL,
                    "provisioningJob": lssthtc.WmsNodeType.SERVICE,
                },
            )

    def test_noop(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/noop_running_1/noop_running_1.dag", tmp_dir)
            summary, job_name_to_label, job_name_to_type = lssthtc.summarize_dag(tmp_dir)
            self.assertEqual(
                set(summary.split(";")),
                {"pipetaskInit:1", "label1:6", "label2:6", "label3:6", "label4:6", "label5:6", "finalJob:1"},
            )
            self.assertEqual(
                job_name_to_label,
                {
                    "label1_val1a_val2a": "label1",
                    "label1_val1a_val2b": "label1",
                    "label1_val1b_val2a": "label1",
                    "label1_val1b_val2b": "label1",
                    "label1_val1c_val2a": "label1",
                    "label1_val1c_val2b": "label1",
                    "label2_val1a_val2a": "label2",
                    "label2_val1a_val2b": "label2",
                    "label2_val1b_val2a": "label2",
                    "label2_val1b_val2b": "label2",
                    "label2_val1c_val2a": "label2",
                    "label2_val1c_val2b": "label2",
                    "label3_val1a_val2a": "label3",
                    "label3_val1a_val2b": "label3",
                    "label3_val1b_val2a": "label3",
                    "label3_val1b_val2b": "label3",
                    "label3_val1c_val2a": "label3",
                    "label3_val1c_val2b": "label3",
                    "label4_val1a_val2a": "label4",
                    "label4_val1a_val2b": "label4",
                    "label4_val1b_val2a": "label4",
                    "label4_val1b_val2b": "label4",
                    "label4_val1c_val2a": "label4",
                    "label4_val1c_val2b": "label4",
                    "label5_val1a_val2a": "label5",
                    "label5_val1a_val2b": "label5",
                    "label5_val1b_val2a": "label5",
                    "label5_val1b_val2b": "label5",
                    "label5_val1c_val2a": "label5",
                    "label5_val1c_val2b": "label5",
                    "finalJob": "finalJob",
                    "pipetaskInit": "pipetaskInit",
                    "wms_noop_order1_val1a": "order1",
                    "wms_noop_order1_val1b": "order1",
                },
            )
            self.assertEqual(
                job_name_to_type,
                {
                    "label1_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "finalJob": lssthtc.WmsNodeType.FINAL,
                    "pipetaskInit": lssthtc.WmsNodeType.PAYLOAD,
                    "wms_noop_order1_val1a": lssthtc.WmsNodeType.NOOP,
                    "wms_noop_order1_val1b": lssthtc.WmsNodeType.NOOP,
                },
            )

    def test_subdags(self):
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "group_running_1")
            copytree(f"{TESTDIR}/data/group_running_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            summary, job_name_to_label, job_name_to_type = lssthtc.summarize_dag(submit_dir)
            self.assertEqual(
                set(summary.split(";")),
                {"pipetaskInit:1", "label1:6", "label2:6", "label3:6", "label4:6", "label5:6", "finalJob:1"},
            )

            self.assertEqual(
                job_name_to_label,
                {
                    "pipetaskInit": "pipetaskInit",
                    "label1_val1b_val2a": "label1",
                    "label1_val1c_val2a": "label1",
                    "label1_val1a_val2b": "label1",
                    "label1_val1b_val2b": "label1",
                    "label1_val1c_val2b": "label1",
                    "label1_val1a_val2a": "label1",
                    "label2_val1a_val2b": "label2",
                    "label2_val1a_val2a": "label2",
                    "label2_val1b_val2a": "label2",
                    "label2_val1b_val2b": "label2",
                    "label2_val1c_val2a": "label2",
                    "label2_val1c_val2b": "label2",
                    "label3_val1b_val2a": "label3",
                    "label3_val1c_val2a": "label3",
                    "label3_val1a_val2b": "label3",
                    "label3_val1b_val2b": "label3",
                    "label3_val1c_val2b": "label3",
                    "label3_val1a_val2a": "label3",
                    "label4_val1a_val2b": "label4",
                    "label4_val1a_val2a": "label4",
                    "label4_val1b_val2a": "label4",
                    "label4_val1b_val2b": "label4",
                    "label4_val1c_val2a": "label4",
                    "label4_val1c_val2b": "label4",
                    "label5_val1a_val2b": "label5",
                    "label5_val1a_val2a": "label5",
                    "label5_val1b_val2a": "label5",
                    "label5_val1b_val2b": "label5",
                    "label5_val1c_val2a": "label5",
                    "label5_val1c_val2b": "label5",
                    "finalJob": "finalJob",
                    "provisioningJob": "provisioningJob",
                    "wms_group_order1_val1a": "order1",
                    "wms_group_order1_val1b": "order1",
                    "wms_group_order1_val1c": "order1",
                    "wms_check_status_wms_group_order1_val1a": "order1",
                    "wms_check_status_wms_group_order1_val1b": "order1",
                    "wms_check_status_wms_group_order1_val1c": "order1",
                },
            )

            self.assertEqual(
                job_name_to_type,
                {
                    "pipetaskInit": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label1_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label2_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label3_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label4_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1a_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1a_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1b_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1b_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1c_val2a": lssthtc.WmsNodeType.PAYLOAD,
                    "label5_val1c_val2b": lssthtc.WmsNodeType.PAYLOAD,
                    "finalJob": lssthtc.WmsNodeType.FINAL,
                    "provisioningJob": lssthtc.WmsNodeType.SERVICE,
                    "wms_group_order1_val1a": lssthtc.WmsNodeType.SUBDAG,
                    "wms_group_order1_val1b": lssthtc.WmsNodeType.SUBDAG,
                    "wms_group_order1_val1c": lssthtc.WmsNodeType.SUBDAG,
                    "wms_check_status_wms_group_order1_val1a": lssthtc.WmsNodeType.SUBDAG_CHECK,
                    "wms_check_status_wms_group_order1_val1b": lssthtc.WmsNodeType.SUBDAG_CHECK,
                    "wms_check_status_wms_group_order1_val1c": lssthtc.WmsNodeType.SUBDAG_CHECK,
                },
            )


class ReadDagNodesLogTestCase(unittest.TestCase):
    """Test read_dag_nodes_log function."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        rmtree(self.tmpdir, ignore_errors=True)

    def testFileMissing(self):
        with self.assertRaisesRegex(FileNotFoundError, "DAGMan node log not found in"):
            _ = lssthtc.read_dag_nodes_log(self.tmpdir)

    def testBadFile(self):
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "tiny_problems")
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            results = lssthtc.read_dag_nodes_log(submit_dir)
            self.assertEqual(results["9231.0"]["JobStatus"], lssthtc.JobStatus.COMPLETED)
            self.assertEqual(results["9231.0"]["ExitCode"], 1)
            self.assertEqual(len(results), 6)

    def testRegular(self):
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "tiny_problems")
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            results = lssthtc.read_dag_nodes_log(submit_dir)
            self.assertEqual(results["9231.0"]["JobStatus"], lssthtc.JobStatus.COMPLETED)
            self.assertEqual(results["9231.0"]["ExitCode"], 1)
            self.assertEqual(len(results), 6)

    def testSubdags(self):
        """Making sure it gets data from subdag dirs and doesn't
        fail if some subdags haven't started running yet.
        """
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "group_running_1")
            copytree(f"{TESTDIR}/data/group_running_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            results = lssthtc.read_dag_nodes_log(submit_dir)
            # main dag
            self.assertEqual(results["10094.0"]["JobStatus"], lssthtc.JobStatus.RUNNING)
            # subdag
            self.assertEqual(results["10112.0"]["JobStatus"], lssthtc.JobStatus.COMPLETED)
            self.assertEqual(results["10116.0"]["JobStatus"], lssthtc.JobStatus.RUNNING)


class ReadNodeStatusTestCase(unittest.TestCase):
    """Test read_node_status function."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        rmtree(self.tmpdir, ignore_errors=True)

    def testServiceJobNotSubmitted(self):
        # tiny_prov_no_submit files have successful workflow
        # but provisioningJob could not submit.
        copy2(f"{TESTDIR}/data/tiny_prov_no_submit/tiny_prov_no_submit.dag.nodes.log", self.tmpdir)
        copy2(f"{TESTDIR}/data/tiny_prov_no_submit/tiny_prov_no_submit.dag.dagman.log", self.tmpdir)
        copy2(f"{TESTDIR}/data/tiny_prov_no_submit/tiny_prov_no_submit.node_status", self.tmpdir)
        copy2(f"{TESTDIR}/data/tiny_prov_no_submit/tiny_prov_no_submit.dag", self.tmpdir)

        jobs = lssthtc.read_node_status(self.tmpdir)
        found = [
            id_
            for id_ in jobs
            if jobs[id_].get("wms_node_type", lssthtc.WmsNodeType.UNKNOWN) == lssthtc.WmsNodeType.SERVICE
        ]
        self.assertEqual(len(found), 1)
        self.assertEqual(jobs[found[0]]["DAGNodeName"], "provisioningJob")
        self.assertEqual(jobs[found[0]]["NodeStatus"], lssthtc.NodeStatus.NOT_READY)

    def testMissingStatusFile(self):
        copy2(f"{TESTDIR}/data/tiny_problems/tiny_problems.dag.nodes.log", self.tmpdir)
        copy2(f"{TESTDIR}/data/tiny_problems/tiny_problems.dag.dagman.log", self.tmpdir)
        copy2(f"{TESTDIR}/data/tiny_problems/tiny_problems.dag", self.tmpdir)

        jobs = lssthtc.read_node_status(self.tmpdir)
        self.assertEqual(len(jobs), 7)
        self.assertEqual(jobs["9230.0"]["DAGNodeName"], "pipetaskInit")
        self.assertEqual(jobs["9230.0"]["wms_node_type"], lssthtc.WmsNodeType.PAYLOAD)
        self.assertEqual(jobs["9230.0"]["JobStatus"], lssthtc.JobStatus.COMPLETED)
        found = [
            id_
            for id_ in jobs
            if jobs[id_].get("wms_node_type", lssthtc.WmsNodeType.UNKNOWN) == lssthtc.WmsNodeType.SERVICE
        ]
        self.assertEqual(len(found), 1)
        self.assertEqual(jobs[found[0]]["DAGNodeName"], "provisioningJob")

    def testSubdagsRunning(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/group_running_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            jobs = lssthtc.read_node_status(submit_dir)
            self.assertEqual(len(jobs), 39)  # includes non-payload jobs
            # not guaranteed ids are same, so use names instead
            job_name_to_id = {}
            for id_, info in jobs.items():
                job_name_to_id[info.get("DAGNodeName", id_)] = id_
            job_type_to_names = {}
            for id_, info in jobs.items():
                job_type_to_names.setdefault(
                    info.get("wms_node_type", lssthtc.WmsNodeType.UNKNOWN), set()
                ).add(info.get("DAGNodeName", id_))

            # check counts
            self.assertNotIn(lssthtc.WmsNodeType.NOOP, job_type_to_names)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.PAYLOAD]), 31)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.FINAL]), 1)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SERVICE]), 1)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SUBDAG]), 3)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SUBDAG_CHECK]), 3)

            # spot check some statuses
            self.assertEqual(
                jobs[job_name_to_id["label3_val1a_val2b"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )
            self.assertEqual(
                jobs[job_name_to_id["wms_group_order1_val1a"]]["NodeStatus"], lssthtc.NodeStatus.SUBMITTED
            )
            self.assertEqual(
                jobs[job_name_to_id["label5_val1a_val2a"]]["NodeStatus"], lssthtc.NodeStatus.NOT_READY
            )
            self.assertEqual(
                jobs[job_name_to_id["label2_val1a_val2a"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )

    def testSubdagsFailed(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/group_failed_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            jobs = lssthtc.read_node_status(submit_dir)
            self.assertEqual(len(jobs), 39)
            # not guaranteed ids are same, so use names instead
            job_name_to_id = {}
            for id_, info in jobs.items():
                job_name_to_id[info.get("DAGNodeName", id_)] = id_
            job_type_to_names = {}
            for id_, info in jobs.items():
                job_type_to_names.setdefault(
                    info.get("wms_node_type", lssthtc.WmsNodeType.UNKNOWN), set()
                ).add(info.get("DAGNodeName", id_))

            # check counts
            self.assertNotIn(lssthtc.WmsNodeType.NOOP, job_type_to_names)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.PAYLOAD]), 31)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.FINAL]), 1)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SERVICE]), 1)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SUBDAG]), 3)
            self.assertEqual(len(job_type_to_names[lssthtc.WmsNodeType.SUBDAG_CHECK]), 3)

            # spot check some statuses
            self.assertEqual(
                jobs[job_name_to_id["label3_val1a_val2b"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )
            self.assertEqual(
                jobs[job_name_to_id["wms_group_order1_val1a"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )
            self.assertEqual(
                jobs[job_name_to_id["label5_val1a_val2a"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )

            self.assertEqual(
                jobs[job_name_to_id["label5_val1b_val2a"]]["NodeStatus"], lssthtc.NodeStatus.FUTILE
            )
            self.assertEqual(
                jobs[job_name_to_id["wms_group_order1_val1b"]]["NodeStatus"], lssthtc.NodeStatus.DONE
            )
            self.assertEqual(
                jobs[job_name_to_id["wms_check_status_wms_group_order1_val1b"]]["NodeStatus"],
                lssthtc.NodeStatus.ERROR,
            )


class HTCJobTestCase(unittest.TestCase):
    """Test HTCJob methods."""

    def testWriteDagCommandsPayload(self):
        job = lssthtc.HTCJob(
            "job1",
            "label1",
            {"executable": "/bin/sleep", "arguments": "60", "log": "job1.log"},
            {"dir": "jobs/label1"},
        )
        job.subfile = "job1.sub"

        mockfh = io.StringIO()
        job.write_dag_commands(mockfh, "../..")
        self.assertIn('JOB job1 "job1.sub" DIR "../../jobs/label1"', mockfh.getvalue())

    def testWriteDagCommandsNotJob(self):
        # Testing giving command_name, no dag_rel_path and no dir
        job = lssthtc.HTCJob(
            "finalJob",
            "finalJob",
            {"executable": "/bin/sleep", "arguments": "60", "log": "job1.log"},
        )
        job.subfile = "jobs/finalJob/finalJob.sub"
        mockfh = io.StringIO()
        job.write_dag_commands(mockfh, "", "FINAL")
        self.assertIn('FINAL finalJob "jobs/finalJob/finalJob.sub"', mockfh.getvalue())

    def testWriteDagCommandsNoop(self):
        job = lssthtc.HTCJob("wms_noop_job1", "label1", {}, {"noop": True})
        job.subfile = "notthere.sub"
        mockfh = io.StringIO()
        job.write_dag_commands(mockfh, "")
        self.assertIn("NOOP", mockfh.getvalue())

    def testWriteSubmitFile(self):
        job = lssthtc.HTCJob(
            "job1",
            "label1",
            {"executable": "/bin/sleep", "arguments": "60", "log": "job1.log"},
        )
        with temporaryDirectory() as tmp_dir:
            filename = pathlib.Path(tmp_dir) / "label1/job1.sub"
            job.write_submit_file(filename.parent)
            self.assertTrue(filename.exists())
            # Try to make Submit object from file to find any syntax issues
            _ = lssthtc.htc_create_submit_from_file(filename)

    def testWriteSubmitFileExists(self):
        job = lssthtc.HTCJob(
            "job1",
            "label1",
            {"executable": "/bin/sleep", "arguments": "60", "log": "job1.log"},
        )
        with temporaryDirectory() as tmp_dir:
            filename = pathlib.Path(tmp_dir) / "job1.sub"
            job.subfile = filename
            with open(filename, "w"):
                pass  # make empty file
            job.write_submit_file(filename.parent)
            # make sure didn't overwrite file
            self.assertEqual(filename.stat().st_size, 0, "Incorrectly overwrote existing file")


class HtcWriteJobCommands(unittest.TestCase):
    """Test _htc_write_job_commands function."""

    def testAllCommands(self):
        dag_cmds = {
            "pre": {
                "defer": {"status": 1, "time": 120},
                "debug": {"filename": "debug_pre.txt", "type": "ALL"},
                "executable": "exec1",
                "arguments": "arg1 arg2",
            },
            "post": {
                "defer": {"status": 2, "time": 180},
                "debug": {"filename": "debug_post.txt", "type": "ALL"},
                "executable": "exec2",
                "arguments": "arg3 arg4",
            },
            "vars": {"num": 8, "spaces": "a space"},
            "pre_skip": "1",
            "retry": 3,
            "retry_unless_exit": 1,
            "abort_dag_on": {"node_exit": 100, "abort_exit": 4},
        }

        truth = """SCRIPT DEFER 1 120 DEBUG debug_pre.txt ALL PRE job1 exec1 arg1 arg2
SCRIPT DEFER 2 180 DEBUG debug_post.txt ALL POST job1 exec2 arg3 arg4
VARS job1 num="8"
VARS job1 spaces="a space"
PRE_SKIP job1 1
RETRY job1 3 UNLESS-EXIT 1
ABORT-DAG-ON job1 100 RETURN 4
"""
        mockfh = io.StringIO()
        lssthtc._htc_write_job_commands(mockfh, "job1", dag_cmds)
        self.assertEqual(mockfh.getvalue(), truth)

    def testPartialCommands(self):
        # Trigger skipping the inner if clauses.
        dag_cmds = {
            "pre": {
                "executable": "exec1",
            },
            "post": {
                "executable": "exec2",
            },
            "vars": {"num": 8, "spaces": "a space"},
            "pre_skip": "1",
            "retry": 3,
        }

        truth = """SCRIPT PRE job1 exec1
SCRIPT POST job1 exec2
VARS job1 num="8"
VARS job1 spaces="a space"
PRE_SKIP job1 1
RETRY job1 3
"""
        mockfh = io.StringIO()
        lssthtc._htc_write_job_commands(mockfh, "job1", dag_cmds)
        self.assertEqual(mockfh.getvalue(), truth)

    def testNoCommands(self):
        dag_cmds = {}
        mockfh = io.StringIO()
        lssthtc._htc_write_job_commands(mockfh, "job2", dag_cmds)
        self.assertEqual(mockfh.getvalue(), "")


class HTCBackupFilesSinglePathTestCase(unittest.TestCase):
    """Test htc_backup_files_single_path function."""

    def testSrcDestSame(self):
        with temporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(
                RuntimeError, "Destination directory is same as the source directory"
            ):
                lssthtc.htc_backup_files_single_path(tmp_dir, tmp_dir)

    def testSuccess(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "the_src_dir"
            copytree(f"{TESTDIR}/data/tiny_success", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            backup_dir = test_tmp_dir / "the_dest_dir"
            backup_dir.mkdir()
            lssthtc.htc_backup_files_single_path(submit_dir, backup_dir)
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./tiny_success.dag.dagman.log",
                    "./tiny_success.dag.dagman.out",
                    "./tiny_success.dag",
                },
            )
            result_backup = []
            for root, _, files in os.walk(backup_dir):
                result_backup.extend([str(os.path.join(os.path.relpath(root, backup_dir), f)) for f in files])
            self.assertEqual(
                set(result_backup),
                {
                    "./tiny_success.info.json",
                    "./tiny_success.dag.metrics",
                    "./tiny_success.dag.nodes.log",
                    "./tiny_success.node_status",
                },
            )


class HTCBackupFilesTestCase(unittest.TestCase):
    """Test htc_backup_files function."""

    def testDirectoryNotFound(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            with self.assertRaises(FileNotFoundError):
                _ = lssthtc.htc_backup_files(submit_dir)

    def testSuccess(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/tiny_success", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            result_rescue = lssthtc.htc_backup_files(submit_dir)
            self.assertIsNone(result_rescue)
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./tiny_success.dag.dagman.log",
                    "./tiny_success.dag.dagman.out",
                    "./tiny_success.dag",
                    "000/tiny_success.info.json",
                    "000/tiny_success.dag.metrics",
                    "000/tiny_success.dag.nodes.log",
                    "000/tiny_success.node_status",
                },
            )

    def testDestNotInSubmitDir(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            with self.assertLogs("lsst.ctrl.bps.htcondor", level="WARNING") as cm:
                result_rescue = lssthtc.htc_backup_files(submit_dir, test_tmp_dir / "backup")
            self.assertIn("Invalid backup location:", cm.output[-1])
            result_rescue = lssthtc.htc_backup_files(submit_dir)
            self.assertTrue((submit_dir / "tiny_problems.dag.rescue001").samefile(result_rescue))
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./tiny_problems.dag.dagman.log",
                    "./tiny_problems.dag.dagman.out",
                    "./tiny_problems.dag",
                    "./tiny_problems.dag.rescue001",
                    "001/tiny_problems.info.json",
                    "001/tiny_problems.dag.metrics",
                    "001/tiny_problems.dag.nodes.log",
                    "001/tiny_problems.node_status",
                },
            )

    def testDestInSubmitDir(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            backup_dir = submit_dir / "subdir"
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            result_rescue = lssthtc.htc_backup_files(submit_dir, backup_dir)
            self.assertTrue((submit_dir / "tiny_problems.dag.rescue001").samefile(result_rescue))
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./tiny_problems.dag.dagman.log",
                    "./tiny_problems.dag.dagman.out",
                    "./tiny_problems.dag",
                    "./tiny_problems.dag.rescue001",
                    "subdir/001/tiny_problems.info.json",
                    "subdir/001/tiny_problems.dag.metrics",
                    "subdir/001/tiny_problems.dag.nodes.log",
                    "subdir/001/tiny_problems.node_status",
                },
            )

    def testRelativeSubdir(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            result_rescue = lssthtc.htc_backup_files(submit_dir, "reldir")
            self.assertTrue((submit_dir / "tiny_problems.dag.rescue001").samefile(result_rescue))
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./tiny_problems.dag.dagman.log",
                    "./tiny_problems.dag.dagman.out",
                    "./tiny_problems.dag",
                    "./tiny_problems.dag.rescue001",
                    "reldir/001/tiny_problems.info.json",
                    "reldir/001/tiny_problems.dag.metrics",
                    "reldir/001/tiny_problems.dag.nodes.log",
                    "reldir/001/tiny_problems.node_status",
                },
            )

    def testSubdags(self):
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/group_failed_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            result_rescue = lssthtc.htc_backup_files(submit_dir)
            self.assertTrue(result_rescue.samefile(submit_dir / "group_failed_1.dag.rescue001"))
            result_submit = []
            for root, _, files in os.walk(submit_dir):
                result_submit.extend([str(os.path.join(os.path.relpath(root, submit_dir), f)) for f in files])
            self.assertEqual(
                set(result_submit),
                {
                    "./group_failed_1.dag",
                    "./group_failed_1.dag.dagman.log",
                    "./group_failed_1.dag.dagman.out",
                    "./group_failed_1.dag.rescue001",
                    "subdags/wms_group_order1_val1a/group_order1_val1a.dag",
                    "subdags/wms_group_order1_val1a/group_order1_val1a.dag.dagman.log",
                    "subdags/wms_group_order1_val1a/group_order1_val1a.dag.dagman.out",
                    "subdags/wms_group_order1_val1b/group_order1_val1b.dag",
                    "subdags/wms_group_order1_val1b/group_order1_val1b.dag.dagman.log",
                    "subdags/wms_group_order1_val1b/group_order1_val1b.dag.dagman.out",
                    "subdags/wms_group_order1_val1b/group_order1_val1b.dag.rescue001",
                    "subdags/wms_group_order1_val1c/group_order1_val1c.dag",
                    "subdags/wms_group_order1_val1c/group_order1_val1c.dag.dagman.log",
                    "subdags/wms_group_order1_val1c/group_order1_val1c.dag.dagman.out",
                    "001/group_failed_1.dag.nodes.log",
                    "001/group_failed_1.info.json",
                    "001/group_failed_1.node_status",
                    "001/subdags/wms_group_order1_val1a/group_order1_val1a.dag.nodes.log",
                    "001/subdags/wms_group_order1_val1a/group_order1_val1a.node_status",
                    "001/subdags/wms_group_order1_val1a/wms_group_order1_val1a.dag.post.out",
                    "001/subdags/wms_group_order1_val1a/wms_group_order1_val1a.status.txt",
                    "001/subdags/wms_group_order1_val1b/group_order1_val1b.dag.nodes.log",
                    "001/subdags/wms_group_order1_val1b/group_order1_val1b.node_status",
                    "001/subdags/wms_group_order1_val1b/wms_group_order1_val1b.status.txt",
                    "001/subdags/wms_group_order1_val1b/wms_group_order1_val1b.dag.post.out",
                    "001/subdags/wms_group_order1_val1c/group_order1_val1c.dag.nodes.log",
                    "001/subdags/wms_group_order1_val1c/group_order1_val1c.node_status",
                    "001/subdags/wms_group_order1_val1c/wms_group_order1_val1c.dag.post.out",
                    "001/subdags/wms_group_order1_val1c/wms_group_order1_val1c.status.txt",
                },
            )


class UpdateRescueFileTestCase(unittest.TestCase):
    """Test _update_rescue_file function."""

    def testSuccess(self):
        self.maxDiff = None
        with temporaryDirectory() as tmp_dir:
            test_tmp_dir = pathlib.Path(tmp_dir)
            submit_dir = test_tmp_dir / "submit"
            copytree(f"{TESTDIR}/data/group_failed_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            rescue_file = submit_dir / "group_failed_1.dag.rescue001"
            lssthtc._update_rescue_file(rescue_file)

            with open(rescue_file) as fh:
                lines = fh.readlines()
                results = "".join(lines)

            truth = """# Rescue DAG file, created after running
#   the u_testuser_DM-46294_group_fail_20250310T160455Z.dag DAG file
# Created 3/10/2025 16:08:56 UTC
# Rescue DAG version: 2.0.1 (partial)
#
# Total number of Nodes: 26
# Nodes premarked DONE: 21
# Nodes that failed: 2
#   wms_group_order1_val1b,finalJob,<ENDLIST>

DONE pipetaskInit
DONE label1_val1c_val2a
DONE label1_val1b_val2b
DONE label1_val1b_val2a
DONE label1_val1c_val2b
DONE label1_val1a_val2a
DONE label1_val1a_val2b
DONE label3_val1c_val2a
DONE label3_val1b_val2b
DONE label3_val1b_val2a
DONE label3_val1c_val2b
DONE label3_val1a_val2a
DONE label3_val1a_val2b
DONE wms_group_order1_val1a
DONE label5_val1a_val2a
DONE label5_val1a_val2b
DONE wms_group_order1_val1c
DONE label5_val1c_val2a
DONE label5_val1c_val2b
DONE wms_check_status_wms_group_order1_val1a
DONE wms_check_status_wms_group_order1_val1c
"""

            print("results = ", results, file=sys.stderr)
            print("truth = ", truth, file=sys.stderr)
            self.assertEqual(results, truth)


class ReadDagStatusTestCase(unittest.TestCase):
    """Test read_dag_status function and read_single_dag_status."""

    def testFileMissing(self):
        with temporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(FileNotFoundError, "DAGMan node status not found"):
                _ = lssthtc.read_dag_status(tmp_dir)

    def testRegular(self):
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "tiny_problems")
            copytree(f"{TESTDIR}/data/tiny_problems", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            results = lssthtc.read_dag_status(submit_dir)
            truth = {
                "JobProcsHeld": 0,
                "NodesPost": 0,
                "JobProcsIdle": 0,
                "NodesTotal": 6,
                "NodesFailed": 2,
                "NodesDone": 3,
                "NodesQueued": 0,
                "NodesPre": 0,
                "NodesFutile": 1,
                "NodesUnready": 0,
            }
            self.assertEqual(results, results | truth)

    def testSubdags(self):
        """Making sure it gets data from subdag dirs and doesn't
        fail if some subdags haven't started running yet.
        """
        self.maxDiff = None
        with temporaryDirectory() as tmp_dir:
            submit_dir = os.path.join(tmp_dir, "submit")
            copytree(f"{TESTDIR}/data/group_running_1", submit_dir, ignore=ignore_patterns("*~", ".???*"))
            results = lssthtc.read_dag_status(submit_dir)
            truth = {
                "JobProcsHeld": 0,
                "NodesPost": 0,
                "JobProcsIdle": 0,
                "NodesTotal": 34,
                "NodesFailed": 0,
                "NodesDone": 17,
                "NodesQueued": 3,
                "NodesPre": 0,
                "NodesFutile": 0,
                "NodesUnready": 14,
            }
            self.assertEqual(results, results | truth)


class ReadDagInfoTestCase(unittest.TestCase):
    """Test read_dag_info function."""

    def testFileMissing(self):
        with temporaryDirectory() as tmp_dir:
            with self.assertRaisesRegex(FileNotFoundError, "File with DAGMan job information not found in "):
                _ = lssthtc.read_dag_info(tmp_dir)

    def testSuccess(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.info.json", tmp_dir)
            results = lssthtc.read_dag_info(tmp_dir)

        truth = {
            "test02": {
                "9208.0": {
                    "ClusterId": 9208,
                    "GlobalJobId": "test02#9208.0#1739465078",
                    "bps_wms_service": "lsst.ctrl.bps.htcondor.htcondor_service.HTCondorService",
                    "bps_project": "dev",
                    "bps_payload": "tiny",
                    "bps_operator": "testuser",
                    "bps_wms_workflow": "lsst.ctrl.bps.htcondor.htcondor_service.HTCondorWorkflow",
                    "bps_provisioning_job": "provisioningJob",
                    "bps_run_quanta": "label1:1;label2:1",
                    "bps_campaign": "quick",
                    "bps_runsite": "testpool",
                    "bps_job_summary": "pipetaskInit:1;label1:1;label2:1;finalJob:1",
                    "bps_run": "u_testuser_tiny_20250213T164427Z",
                    "bps_isjob": "True",
                }
            }
        }

        self.assertEqual(results, truth)

    def testPermissionError(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.info.json", tmp_dir)
            with unittest.mock.patch("lsst.ctrl.bps.htcondor.lssthtc.open") as mocked_open:
                mocked_open.side_effect = PermissionError
                with self.assertLogs("lsst.ctrl.bps.htcondor", level="DEBUG") as cm:
                    results = lssthtc.read_dag_info(tmp_dir)
                self.assertIn("Retrieving DAGMan job information failed:", cm.output[-1])
                self.assertEqual({}, results)


class HtcWriteCondorFileTestCase(unittest.TestCase):
    """Test htc_write_condor_file function."""

    def testSuccess(self):
        with temporaryDirectory() as tmp_dir:
            job_name = "job1"
            filename = pathlib.Path(tmp_dir) / f"label1/{job_name}.sub"
            job = {
                "executable": "$(CTRL_MPEXEC_DIR)/bin/pipetask",
                "arguments": "-a -b 2 -c",
                "request_memory": "2000",
                "environment": "one=1 two=\"2\" three='spacey 'quoted' value'",
                "log": f"{job_name}.log",
            }
            job_attrs = {
                "bps_job_name": job_name,
                "bps_job_label": "label1",
                "bps_job_quanta": "task1:8;task2:8",
            }
            expected = [
                "executable=$(CTRL_MPEXEC_DIR)/bin/pipetask\n",
                "arguments=-a -b 2 -c\n",
                "request_memory=2000\n",
                "environment=\"one=1 two=\"2\" three='spacey 'quoted' value'\"\n",
                f"output={job_name}.$(Cluster).out\n",
                f"error={job_name}.$(Cluster).out\n",
                f"log={job_name}.log\n",
                f'+bps_job_name = "{job_name}"\n',
                '+bps_job_label = "label1"\n',
                '+bps_job_quanta = "task1:8;task2:8"\n',
                "queue\n",
            ]

            lssthtc.htc_write_condor_file(filename, job_name, job, job_attrs)
            with open(filename, encoding="utf-8") as f:
                actual = f.readlines()

            self.assertEqual(set(actual), set(expected))
            self.assertTrue(filename.exists())
            # Try to make Submit object from file to find any syntax issues
            _ = lssthtc.htc_create_submit_from_file(filename)


class HtcCreateSubmitFromDagTestCase(unittest.TestCase):
    """Test htc_create_submit_from_dag function."""

    @classmethod
    def setUpClass(cls):
        cls.bindir = None
        # htcondor.Submit.from_dag requires condor_dagman executable in path.
        if not which("condor_dagman"):  # pragma: no cover
            cls.bindir = tempfile.TemporaryDirectory()
            fake_dagman_exec = pathlib.Path(cls.bindir.name) / "condor_dagman"
            with open(fake_dagman_exec, "w") as fh:
                print("#!/bin/bash", file=fh)
                print("echo fake_condor_dagman $@", file=fh)
                print("exit 0", file=fh)
            fake_dagman_exec.chmod(fake_dagman_exec.stat().st_mode | stat.S_IEXEC)
            os.environ["PATH"] = f"{os.environ['PATH']}:{cls.bindir.name}"

    @classmethod
    def tearDownClass(cls):
        if cls.bindir:
            cls.bindir.cleanup()

    @unittest.mock.patch.dict(os.environ, {"_CONDOR_DAGMAN_MAX_JOBS_IDLE": "42"})
    def testMaxIdleEnvVar(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            submit = lssthtc.htc_create_submit_from_dag(str(dag_filename), {})
            self.assertIn("-MaxIdle 42", submit["arguments"])
            self.assertEqual("true", os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"].lower())

    @unittest.mock.patch.dict(os.environ, {})
    def testMaxIdleGiven(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            submit = lssthtc.htc_create_submit_from_dag(str(dag_filename), {"MaxIdle": 37})
            self.assertIn("-MaxIdle 37", submit["arguments"])
            self.assertEqual("true", os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"].lower())

    @unittest.mock.patch.dict(os.environ, {})
    def testNoMaxJobsIdle(self):
        """Note: Since the produced arguments differ depending on
        HTCondor version when no MaxIdle passed to from_dag, not
        checking arguments string here.  Instead just making sure
        lssthtc code doesn't pass MaxIdle value to from_dag.
        """
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            with unittest.mock.patch("htcondor.Submit.from_dag") as submit_mock:
                with unittest.mock.patch("htcondor.param") as mock_param:
                    mock_param.__contains__.return_value = False
                    _ = lssthtc.htc_create_submit_from_dag(str(dag_filename), {})
                    self.assertEqual("true", os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"].lower())
                    submit_mock.assert_called_once_with(str(dag_filename), {})

    @unittest.mock.patch.dict(os.environ, {})
    def testDoRecurseGivenWithNoEnv(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            submit = lssthtc.htc_create_submit_from_dag(str(dag_filename), {"do_recurse": True})
            self.assertIn("-do_recurse", submit["arguments"])
            self.assertEqual("true", os.environ["_CONDOR_DAGMAN_GENERATE_SUBDAG_SUBMITS"].lower())
            self.assertEqual("true", os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"].lower())

    @unittest.mock.patch.dict(os.environ, {"_CONDOR_DAGMAN_GENERATE_SUBDAG_SUBMITS": "False"})
    def testDoRecurseGivenWithEnv(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            submit = lssthtc.htc_create_submit_from_dag(str(dag_filename), {"do_recurse": True})
            self.assertIn("-do_recurse", submit["arguments"])
            self.assertEqual("true", os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"].lower())
            self.assertEqual("false", os.environ["_CONDOR_DAGMAN_GENERATE_SUBDAG_SUBMITS"].lower())

    @unittest.mock.patch.dict(os.environ, {"_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV": "*_DIR"})
    def testGetEnvOverridden(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/tiny_success/tiny_success.dag", tmp_dir)
            dag_filename = pathlib.Path(tmp_dir) / "tiny_success.dag"
            submit = lssthtc.htc_create_submit_from_dag(str(dag_filename), {"do_recurse": True})
            self.assertIn("-do_recurse", submit["arguments"])
            self.assertEqual(os.environ["_CONDOR_DAGMAN_MANAGER_JOB_APPEND_GETENV"], "*_DIR")


if __name__ == "__main__":
    unittest.main()
