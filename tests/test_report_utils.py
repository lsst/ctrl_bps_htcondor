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

"""Unit tests for the report utilities."""

import logging
import os
import unittest
from pathlib import Path
from shutil import copy2, copytree

import htcondor

from lsst.ctrl.bps import (
    WmsSpecificInfo,
    WmsStates,
)
from lsst.ctrl.bps.htcondor import lssthtc, report_utils
from lsst.utils.tests import temporaryDirectory

logger = logging.getLogger("lsst.ctrl.bps.htcondor")

TESTDIR = os.path.abspath(os.path.dirname(__file__))

LOCATE_SUCCESS = """[
        CondorPlatform = "$CondorPlatform: X86_64-CentOS_7.9 $";
        MyType = "Scheduler";
        Machine = "testmachine";
        Name = "testmachine";
        CondorVersion = "$CondorVersion: 23.0.3 2024-04-04 $";
        MyAddress = "<127.0.0.1:9618?addrs=127.0.0.1-9618+snip>"
    ]
"""


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
        actual = report_utils._get_exit_code_summary(self.jobs)
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
            report_utils._get_exit_code_summary(jobs)
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
            report_utils._get_exit_code_summary(jobs)
        self.assertIn("lsst.ctrl.bps.htcondor", cm.records[0].name)
        self.assertIn("Attribute", cm.output[0])
        self.assertIn("not found", cm.output[0])


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

        total, result = report_utils._get_state_counts_from_dag_job(job)
        self.assertEqual(total, 22)
        self.assertEqual(result, truth)


class GetInfoFromPathTestCase(unittest.TestCase):
    """Test _get_info_from_path function."""

    def test_tmpdir_abort(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_tmpdir_abort.dag.dagman.out", tmp_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(tmp_dir)
            self.assertEqual(wms_workflow_id, lssthtc.MISSING_ID)
            self.assertEqual(jobs, {})
            self.assertIn("Cannot submit from /tmp", message)

    def test_no_dagman_messages(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/test_no_messages.dag.dagman.out", tmp_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(tmp_dir)
            self.assertEqual(wms_workflow_id, lssthtc.MISSING_ID)
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
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(tmp_dir)
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
            wms_workflow_id, jobs, message = report_utils._get_info_from_path("subdir")
            self.assertEqual(wms_workflow_id, "1163.0")
            self.assertEqual(len(jobs), 6)  # dag, pipetaskInit, 3 science, finalJob
            self.assertEqual(message, "")
            self.assertEqual(jobs["1163.0"]["Iwd"], str(abs_path))
            os.chdir(orig_dir)


class AddServiceJobSpecificInfoTestCase(unittest.TestCase):
    """Test _add_service_job_specific_info function.

    Note: The job_ad's are hardcoded in these tests.  The
    values in the dictionaries come from plugin code as
    well as HTCondor.  Changes in either of those codes
    that produce data for the job_ad can break this
    function without breaking these unit tests.

    Also, since hold status/messages stick around, testing
    various cases with and without job being held just to
    ensure get right status in both cases.
    """

    def testNotSubmitted(self):
        # Service job not submitted yet or can't be submitted.
        # (Typically an plugin bug.)
        # At this function level, can't tell if not submitted
        # yet or problem so it never will.
        job_ad = {
            "ClusterId": -64,
            "DAGManJobID": "8997.0",
            "DAGNodeName": "provisioningJob",
            "NodeStatus": lssthtc.NodeStatus.NOT_READY,
            "ProcId": 0,
            "bps_job_label": "service_provisioningJob",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "UNREADY", "status_details": ""}
        )

    def testRunning(self):
        # DAG hasn't completed (Running or held),
        # Service job is running.
        job_ad = {
            "ClusterId": 8523,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.RUNNING,
        }

        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "RUNNING", "status_details": ""}
        )

    def testDied(self):
        # DAG hasn't completed (Running or held),
        # Service job failed (completed non-zero exit code)
        job_ad = {
            "ClusterId": 8761,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.COMPLETED,
            "ExitCode": 4,
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "FAILED", "status_details": ""}
        )

    def testDeleted(self):
        # Deleted by user (never held)
        job_ad = {
            "ClusterId": 9086,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "via condor_rm (by user mgower)",
            "job_evicted_time": "2025-02-11T11:35:04",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "DELETED", "status_details": ""}
        )

    def testSucceedEarly(self):
        # DAG hasn't completed (Running or held),
        # Service job completed with exit code 0
        job_ad = {
            "ClusterId": 8761,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.COMPLETED,
            "ExitCode": 0,
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context,
            {
                "job_name": "provisioningJob",
                "status": "SUCCEEDED",
                "status_details": "(Note: Finished before workflow.)",
            },
        )

    def testSucceedOldRemoveMessage(self):
        # DAG completed, job was in running state when removed.
        job_ad = {
            "ClusterId": 8761,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.REMOVED,
            "Reason": "Removed by DAGMan (by user mgower)",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""}
        )

    def testSucceed(self):
        # DAG completed, job was in running state when removed.
        job_ad = {
            "ClusterId": 8761,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.REMOVED,
            "Reason": (
                "removed because <OtherJobRemoveRequirements = DAGManJobId =?= 8556>"
                " fired when job (8556.0) was removed"
            ),
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""}
        )

    def testUserHeldWhileRunning(self):
        # DAG hasn't completed (Running or held),
        # user put at least service job on hold
        job_ad = {
            "ClusterId": 8523,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": htcondor.JobStatus.HELD,
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
        }

        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context,
            {
                "job_name": "provisioningJob",
                "status": "HELD",
                "status_details": "(via condor_hold (by user mgower))",
            },
        )

    def testHeldByHTC(self):
        # Job put on hold by HTCondor, removed when DAG ends
        job_ad = {
            "ClusterId": 8693,
            "DAGNodeName": "provisioningJob",
            "HoldReason": "Failed to execute",
            "HoldReasonCode": 6,
            "HoldReasonSubCode": 2,
            "JobStatus": htcondor.JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "Removed by DAGMan (by user mgower)",
            "job_held_time": "2025-02-07T12:50:07",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context,
            {
                "job_name": "provisioningJob",
                "status": "DELETED",
                "status_details": "(Job was held for the following reason: Failed to execute)",
            },
        )

    def testHeldReleasedRunning(self):
        # DAG hasn't completed (Running or held),
        # Since held info will be in job_ad, make sure knows released.
        job_ad = {
            "ClusterId": 8625,
            "DAGNodeName": "provisioningJob",
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.RUNNING,
            "LogNotes": "DAG Node: provisioningJob",
            "ProcId": 0,
            "job_held_time": "2025-02-07T12:33:34",
            "job_released_time": "2025-02-07T12:33:47",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "RUNNING", "status_details": ""}
        )

    def testHeldReleasedDied(self):
        # Since held info will be in job_ad,
        # make sure knows status after released.
        job_ad = {
            "ClusterId": 9120,
            "DAGNodeName": "provisioningJob",
            "ExitBySignal": False,
            "ExitCode": 4,
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.COMPLETED,
            "ProcId": 0,
            "Reason": "via condor_release (by user mgower)",
            "ReturnValue": 4,
            "TerminatedNormally": True,
            "job_held_time": "2025-02-11T11:46:40",
            "job_released_time": "2025-02-11T11:46:47",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "FAILED", "status_details": ""}
        )

    def testHeldReleasedSuccessEarly(self):
        # Since held info will be in job_ad,
        # make sure knows status after released.
        job_ad = {
            "ClusterId": 9154,
            "DAGNodeName": "provisioningJob",
            "ExitBySignal": False,
            "ExitCode": 0,
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.COMPLETED,
            "ProcId": 0,
            "Reason": "via condor_release (by user mgower)",
            "TerminatedNormally": True,
            "job_held_time": "2025-02-11T11:55:20",
            "job_released_time": "2025-02-11T11:55:25",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context,
            {
                "job_name": "provisioningJob",
                "status": "SUCCEEDED",
                "status_details": "(Note: Finished before workflow.)",
            },
        )

    def testHeldReleasedSuccess(self):
        # DAG has completed.
        # Since held info will be in job_ad,
        # make sure knows status after released.
        job_ad = {
            "ClusterId": 8625,
            "DAGNodeName": "provisioningJob",
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "removed because <OtherJobRemoveRequirements = DAGManJobId =?= "
            "8624> fired when job (8624.0) was removed",
            "job_held_time": "2025-02-07T12:33:34",
            "job_released_time": "2025-02-07T12:33:47",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""}
        )

    def testHeldReleasedDeleted(self):
        # Since held info will be in job_ad,
        # make sure knows status after released.
        job_ad = {
            "ClusterId": 9086,
            "DAGNodeName": "provisioningJob",
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "via condor_rm (by user mgower)",
            "job_evicted_time": "2025-02-11T11:35:04",
            "job_held_time": "2025-02-11T11:35:04",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "DELETED", "status_details": ""}
        )

    def testHeldReleasedHeld(self):
        # Since release info will be in job_ad,
        # make sure knows held after release.
        job_ad = {
            "ClusterId": 8659,
            "DAGNodeName": "provisioningJob",
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
            "JobStatus": htcondor.JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "Removed by DAGMan (by user mgower)",
            "TerminatedNormally": False,
            "job_held_time": "2025-02-07T12:36:15",
            "job_released_time": "2025-02-07T12:36:07",
        }
        results = WmsSpecificInfo()
        report_utils._add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context,
            {
                "job_name": "provisioningJob",
                "status": "DELETED",
                "status_details": "(Job was held for the following reason: via condor_hold (by user mgower))",
            },
        )


class GetRunSummaryTestCase(unittest.TestCase):
    """Test _get_run_summary function."""

    def testJobSummaryInJobAd(self):
        summary = "pipetaskInit:1;label1:2;label2:2;finalJob:1"
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "bps_job_summary": summary}
        results = report_utils._get_run_summary(job_ad)
        self.assertEqual(results, summary)

    def testRunSummaryInJobAd(self):
        summary = "pipetaskInit:1;label1:2;label2:2;finalJob:1"
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "bps_run_summary": summary}
        results = report_utils._get_run_summary(job_ad)
        self.assertEqual(results, summary)

    def testSummaryFromDag(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/good.dag", tmp_dir)
            job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "Iwd": tmp_dir}
            results = report_utils._get_run_summary(job_ad)
            self.assertEqual(results, "pipetaskInit:1;label1:1;label2:1;label3:1;finalJob:1")

    def testSummaryNoDag(self):
        with self.assertLogs(logger=logger, level="WARNING") as cm:
            with temporaryDirectory() as tmp_dir:
                job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "Iwd": tmp_dir}
                results = report_utils._get_run_summary(job_ad)
                self.assertEqual(results, "")
            self.assertIn("lsst.ctrl.bps.htcondor", cm.records[0].name)
            self.assertIn("Could not get run summary for htcondor job", cm.output[0])


class IsServiceJobTestCase(unittest.TestCase):
    """Test is_service_job function."""

    def testNotServiceJob(self):
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "wms_node_type": lssthtc.WmsNodeType.PAYLOAD}
        self.assertFalse(report_utils.is_service_job(job_ad))

    def testIsServiceJob(self):
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "wms_node_type": lssthtc.WmsNodeType.SERVICE}
        self.assertTrue(report_utils.is_service_job(job_ad))

    def testMissingBpsType(self):
        job_ad = {
            "ClusterId": 8659,
            "DAGNodeName": "testJob",
        }
        self.assertFalse(report_utils.is_service_job(job_ad))


class CreateDetailedReportFromJobsTestCase(unittest.TestCase):
    """Test _create_detailed_report_from_jobs function."""

    def testTinySuccess(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_success")
            copytree(f"{TESTDIR}/data/tiny_success", test_submit_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(test_submit_dir)
            run_reports = report_utils._create_detailed_report_from_jobs(wms_workflow_id, jobs)
            self.assertEqual(len(run_reports), 1)
            report = run_reports[wms_workflow_id]
            self.assertEqual(report.wms_id, wms_workflow_id)
            self.assertEqual(report.state, WmsStates.SUCCEEDED)
            self.assertTrue(os.path.samefile(report.path, test_submit_dir))
            self.assertEqual(report.run_summary, "pipetaskInit:1;label1:1;label2:1;finalJob:1")
            self.assertEqual(
                report.job_state_counts,
                {
                    WmsStates.UNKNOWN: 0,
                    WmsStates.MISFIT: 0,
                    WmsStates.UNREADY: 0,
                    WmsStates.READY: 0,
                    WmsStates.PENDING: 0,
                    WmsStates.RUNNING: 0,
                    WmsStates.DELETED: 0,
                    WmsStates.HELD: 0,
                    WmsStates.SUCCEEDED: 4,
                    WmsStates.FAILED: 0,
                    WmsStates.PRUNED: 0,
                },
            )
            self.assertEqual(
                report.specific_info.context,
                {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""},
            )

    def testTinyProblems(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_problems")
            copytree(f"{TESTDIR}/data/tiny_problems", test_submit_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(test_submit_dir)
            run_reports = report_utils._create_detailed_report_from_jobs(wms_workflow_id, jobs)
            self.assertEqual(len(run_reports), 1)
            report = run_reports[wms_workflow_id]
            self.assertEqual(report.wms_id, wms_workflow_id)
            self.assertEqual(report.state, WmsStates.FAILED)
            self.assertTrue(os.path.samefile(report.path, test_submit_dir))
            self.assertEqual(report.run_summary, "pipetaskInit:1;label1:2;label2:2;finalJob:1")
            self.assertEqual(
                report.job_state_counts,
                {
                    WmsStates.UNKNOWN: 0,
                    WmsStates.MISFIT: 0,
                    WmsStates.UNREADY: 0,
                    WmsStates.READY: 0,
                    WmsStates.PENDING: 0,
                    WmsStates.RUNNING: 0,
                    WmsStates.DELETED: 0,
                    WmsStates.HELD: 0,
                    WmsStates.SUCCEEDED: 4,
                    WmsStates.FAILED: 1,
                    WmsStates.PRUNED: 1,
                },
            )
            self.assertEqual(
                run_reports[wms_workflow_id].specific_info.context,
                {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""},
            )

    def testTinyRunning(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_running")
            copytree(f"{TESTDIR}/data/tiny_running", test_submit_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(test_submit_dir)
            run_reports = report_utils._create_detailed_report_from_jobs(wms_workflow_id, jobs)
            self.assertEqual(len(run_reports), 1)
            report = run_reports[wms_workflow_id]
            self.assertEqual(report.wms_id, wms_workflow_id)
            self.assertEqual(report.state, WmsStates.RUNNING)
            self.assertTrue(os.path.samefile(report.path, test_submit_dir))
            self.assertEqual(report.run_summary, "pipetaskInit:1;label1:1;label2:1;finalJob:1")
            self.assertEqual(
                report.job_state_counts,
                {
                    WmsStates.UNKNOWN: 0,
                    WmsStates.MISFIT: 0,
                    WmsStates.UNREADY: 2,
                    WmsStates.READY: 0,
                    WmsStates.PENDING: 0,
                    WmsStates.RUNNING: 1,
                    WmsStates.DELETED: 0,
                    WmsStates.HELD: 0,
                    WmsStates.SUCCEEDED: 1,
                    WmsStates.FAILED: 0,
                    WmsStates.PRUNED: 0,
                },
            )
            self.assertEqual(
                report.specific_info.context,
                {"job_name": "provisioningJob", "status": "RUNNING", "status_details": ""},
            )

    def testNoopRunning(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "noop_running_1")
            copytree(f"{TESTDIR}/data/noop_running_1", test_submit_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(test_submit_dir)
            run_reports = report_utils._create_detailed_report_from_jobs(wms_workflow_id, jobs)
            self.assertEqual(len(run_reports), 1)
            report = run_reports[wms_workflow_id]
            self.assertEqual(report.wms_id, wms_workflow_id)
            self.assertEqual(report.state, WmsStates.RUNNING)
            self.assertTrue(os.path.samefile(report.path, test_submit_dir))
            self.assertEqual(
                set(report.run_summary.split(";")),
                {"pipetaskInit:1", "label1:6", "label2:6", "label3:6", "label4:6", "label5:6", "finalJob:1"},
            )
            self.assertEqual(
                report.job_state_counts,
                {
                    WmsStates.UNKNOWN: 0,
                    WmsStates.MISFIT: 0,
                    WmsStates.UNREADY: 12,
                    WmsStates.READY: 0,
                    WmsStates.PENDING: 1,
                    WmsStates.RUNNING: 10,
                    WmsStates.DELETED: 0,
                    WmsStates.HELD: 0,
                    WmsStates.SUCCEEDED: 9,
                    WmsStates.FAILED: 0,
                    WmsStates.PRUNED: 0,
                },
            )
            self.assertEqual(report.total_number_jobs, 32)
            self.assertIsNone(report.specific_info)

    def testNoopFailed(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "noop_failed_1")
            copytree(f"{TESTDIR}/data/noop_failed_1", test_submit_dir)
            wms_workflow_id, jobs, message = report_utils._get_info_from_path(test_submit_dir)
            run_reports = report_utils._create_detailed_report_from_jobs(wms_workflow_id, jobs)
            self.assertEqual(len(run_reports), 1)
            report = run_reports[wms_workflow_id]
            self.assertEqual(report.wms_id, wms_workflow_id)
            self.assertEqual(report.state, WmsStates.FAILED)
            self.assertTrue(os.path.samefile(report.path, test_submit_dir))
            self.assertEqual(
                set(report.run_summary.split(";")),
                {"pipetaskInit:1", "label1:6", "label2:6", "label3:6", "label4:6", "label5:6", "finalJob:1"},
            )
            self.assertEqual(
                report.job_state_counts,
                {
                    WmsStates.UNKNOWN: 0,
                    WmsStates.MISFIT: 0,
                    WmsStates.UNREADY: 0,
                    WmsStates.READY: 0,
                    WmsStates.PENDING: 0,
                    WmsStates.RUNNING: 0,
                    WmsStates.DELETED: 0,
                    WmsStates.HELD: 0,
                    WmsStates.SUCCEEDED: 27,
                    WmsStates.FAILED: 1,
                    WmsStates.PRUNED: 4,
                },
            )
            self.assertEqual(report.total_number_jobs, 32)
            self.assertIsNone(report.specific_info)
            self.assertEqual(
                report.exit_code_summary,
                {
                    "pipetaskInit": [],
                    "label1": [],
                    "label2": [1],
                    "label3": [],
                    "label4": [],
                    "label5": [],
                    "finalJob": [],
                },
            )


class GetStatusFromIdTestCase(unittest.TestCase):
    """Test _get_status_from_id function."""

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.report_utils._get_info_from_schedd")
    def testNotFound(self, mock_get):
        mock_get.return_value = {}

        state, message = report_utils._get_status_from_id("100", 0, {})

        mock_get.assert_called_once_with("100", 0, {})

        self.assertEqual(state, WmsStates.UNKNOWN)
        self.assertEqual(message, "DAGMan job 100 not found in queue or history.  Check id or try path.")

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.report_utils._htc_status_to_wms_state")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.report_utils._get_info_from_schedd")
    def testFound(self, mock_get, mock_conversion):
        fake_id = "100.0"
        dag_ads = {fake_id: {"JobStatus": lssthtc.JobStatus.RUNNING}}
        mock_get.return_value = {"schedd1": dag_ads}
        mock_conversion.return_value = WmsStates.RUNNING

        state, message = report_utils._get_status_from_id(fake_id, 0, {})

        mock_get.assert_called_once_with(fake_id, 0, {})
        mock_conversion.assert_called_once_with(dag_ads[fake_id])

        self.assertEqual(state, WmsStates.RUNNING)
        self.assertEqual(message, "")


class GetStatusFromPathTestCase(unittest.TestCase):
    """Test _get_status_from_path function."""

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.report_utils.read_dag_log")
    def testNoDagLog(self, mock_read):
        mock_read.side_effect = FileNotFoundError

        fake_path = "/fake/path"
        state, message = report_utils._get_status_from_path(fake_path)

        mock_read.assert_called_once_with(Path(fake_path))

        self.assertEqual(state, WmsStates.UNKNOWN)
        self.assertEqual(message, f"DAGMan log not found in {fake_path}.  Check path.")

    def testSuccess(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_success")
            copytree(f"{TESTDIR}/data/tiny_success", test_submit_dir)
            state, message = report_utils._get_status_from_path(test_submit_dir)

            self.assertEqual(state, WmsStates.SUCCEEDED)
            self.assertEqual(message, "")

    def testFailure(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_problems")
            copytree(f"{TESTDIR}/data/tiny_problems", test_submit_dir)
            state, message = report_utils._get_status_from_path(test_submit_dir)

            self.assertEqual(state, WmsStates.FAILED)
            self.assertEqual(message, "")


if __name__ == "__main__":
    unittest.main()
