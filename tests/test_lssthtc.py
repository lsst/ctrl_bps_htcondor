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

import logging
import os
import pathlib
import tempfile
import unittest
from shutil import copy2, rmtree

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
                    "pipetaskInit": "payload",
                    "0682f8f9-12f0-40a5-971e-8b30c7231e5c_label1_val1_val2": "payload",
                    "d0305e2d-f164-4a85-bd24-06afe6c84ed9_label2_val1_val2": "payload",
                    "2806ecc9-1bba-4362-8fff-ab4e6abb9f83_label3_val1_val2": "payload",
                    "finalJob": "final",
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
                    "4a7f478b-2e9b-435c-a730-afac3f621658_label1_val1_val2a": "label1",
                    "057c8caf-66f6-4612-abf7-cdea5b666b1b_label1_val1_val2b": "label1",
                    "696ee50d-e711-40d6-9caf-ee29ae4a656d_label2_val1_val2a": "label2",
                    "40040b97-606d-4997-98d3-e0493055fe7e_label2_val1_val2b": "label2",
                    "finalJob": "finalJob",
                    "provisioningJob": "provisioningJob",
                },
            )
            self.assertEqual(
                job_name_to_type,
                {
                    "pipetaskInit": "payload",
                    "4a7f478b-2e9b-435c-a730-afac3f621658_label1_val1_val2a": "payload",
                    "057c8caf-66f6-4612-abf7-cdea5b666b1b_label1_val1_val2b": "payload",
                    "696ee50d-e711-40d6-9caf-ee29ae4a656d_label2_val1_val2a": "payload",
                    "40040b97-606d-4997-98d3-e0493055fe7e_label2_val1_val2b": "payload",
                    "finalJob": "final",
                    "provisioningJob": "service",
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
            _, _ = lssthtc.read_dag_nodes_log(self.tmpdir)


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
        found = [id_ for id_ in jobs if jobs[id_].get("bps_job_type", "MISS") == "service"]
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
        self.assertEqual(jobs["9230.0"]["bps_job_type"], "payload")
        self.assertEqual(jobs["9230.0"]["JobStatus"], lssthtc.JobStatus.COMPLETED)
        found = [id_ for id_ in jobs if jobs[id_].get("bps_job_type", "MISS") == "service"]
        self.assertEqual(len(found), 1)
        self.assertEqual(jobs[found[0]]["DAGNodeName"], "provisioningJob")


if __name__ == "__main__":
    unittest.main()
