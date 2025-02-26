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
import os
import unittest
from pathlib import Path
from shutil import copy2, copytree

import htcondor

from lsst.ctrl.bps import BpsConfig, GenericWorkflowExec, GenericWorkflowJob, WmsSpecificInfo, WmsStates
from lsst.ctrl.bps.htcondor.htcondor_config import HTC_DEFAULTS_URI
from lsst.ctrl.bps.htcondor.htcondor_service import (
    HTCondorService,
    JobStatus,
    NodeStatus,
    WmsIdType,
    _add_service_job_specific_info,
    _create_detailed_report_from_jobs,
    _get_exit_code_summary,
    _get_info_from_path,
    _get_run_summary,
    _get_state_counts_from_dag_job,
    _htc_node_status_to_wms_state,
    _htc_status_to_wms_state,
    _translate_job_cmds,
    _wms_id_to_dir,
    is_service_job,
)
from lsst.ctrl.bps.htcondor.lssthtc import MISSING_ID
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

PING_SUCCESS = """[
        AuthCommand = 60011;
        AuthMethods = "FS_REMOTE";
        Command = 60040;
        AuthorizationSucceeded = true;
        ValidCommands = "60002,60003,60011,60014,60045,60046,60047,60048,60049,60050,60052,523";
        TriedAuthentication = true;
        RemoteVersion = "$CondorVersion: 10.9.0 2023-09-28 BuildID: 678228 PackageID: 10.9.0-1 $";
        MyRemoteUserName = "testuser@testmachine";
        Authentication = "YES";
    ]
"""


class HTCondorServiceTestCase(unittest.TestCase):
    """Test selected methods of the HTCondor WMS service class."""

    def setUp(self):
        config = BpsConfig({}, wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService")
        self.service = HTCondorService(config)

    def tearDown(self):
        pass

    def testDefaults(self):
        self.assertEqual(self.service.defaults["memoryLimit"], 491520)

    def testDefaultsPath(self):
        self.assertEqual(self.service.defaults_uri, HTC_DEFAULTS_URI)
        self.assertFalse(self.service.defaults_uri.isdir())

    @unittest.mock.patch.object(htcondor.Collector, "locate", return_value=LOCATE_SUCCESS)
    @unittest.mock.patch.object(htcondor.SecMan, "ping", return_value=PING_SUCCESS)
    def testPingSuccess(self, mock_locate, mock_ping):
        status, message = self.service.ping(None)
        self.assertEqual(status, 0)
        self.assertEqual(message, "")

    def testPingFailure(self):
        with unittest.mock.patch("htcondor.Collector.locate") as locate_mock:
            locate_mock.side_effect = htcondor.HTCondorLocateError()
            status, message = self.service.ping(None)
            self.assertEqual(status, 1)
            self.assertEqual(message, "Could not locate Schedd service.")

    @unittest.mock.patch.object(htcondor.Collector, "locate", return_value=LOCATE_SUCCESS)
    def testPingPermission(self, mock_locate):
        with unittest.mock.patch("htcondor.SecMan.ping") as ping_mock:
            ping_mock.side_effect = htcondor.HTCondorIOError()
            status, message = self.service.ping(None)
            self.assertEqual(status, 1)
            self.assertEqual(message, "Permission problem with Schedd service.")


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


class TranslateJobCmdsTestCase(unittest.TestCase):
    """Test _translate_job_cmds method."""

    def setUp(self):
        self.gw_exec = GenericWorkflowExec("test_exec", "/dummy/dir/pipetask")
        self.cached_vals = {"profile": {}, "bpsUseShared": True}

    def testRetryUnlessNone(self):
        gwjob = GenericWorkflowJob("retryUnless", executable=self.gw_exec)
        gwjob.retry_unless_exit = None
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertNotIn("retry_until", htc_commands)

    def testRetryUnlessInt(self):
        gwjob = GenericWorkflowJob("retryUnlessInt", executable=self.gw_exec)
        gwjob.retry_unless_exit = 3
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(int(htc_commands["retry_until"]), gwjob.retry_unless_exit)

    def testRetryUnlessList(self):
        gwjob = GenericWorkflowJob("retryUnlessList", executable=self.gw_exec)
        gwjob.retry_unless_exit = [1, 2]
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["retry_until"], "member(ExitCode, {1,2})")

    def testRetryUnlessBad(self):
        gwjob = GenericWorkflowJob("retryUnlessBad", executable=self.gw_exec)
        gwjob.retry_unless_exit = "1,2,3"
        with self.assertRaises(ValueError) as cm:
            _ = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertIn("retryUnlessExit", str(cm.exception))

    def testEnvironmentBasic(self):
        gwjob = GenericWorkflowJob("jobEnvironment", executable=self.gw_exec)
        gwjob.environment = {"TEST_INT": 1, "TEST_STR": "TWO"}
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_INT=1 TEST_STR='TWO'")

    def testEnvironmentSpaces(self):
        gwjob = GenericWorkflowJob("jobEnvironment", executable=self.gw_exec)
        gwjob.environment = {"TEST_SPACES": "spacey value"}
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_SPACES='spacey value'")

    def testEnvironmentSingleQuotes(self):
        gwjob = GenericWorkflowJob("jobEnvironment", executable=self.gw_exec)
        gwjob.environment = {"TEST_SINGLE_QUOTES": "spacey 'quoted' value"}
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_SINGLE_QUOTES='spacey ''quoted'' value'")

    def testEnvironmentDoubleQuotes(self):
        gwjob = GenericWorkflowJob("jobEnvironment", executable=self.gw_exec)
        gwjob.environment = {"TEST_DOUBLE_QUOTES": 'spacey "double" value'}
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], """TEST_DOUBLE_QUOTES='spacey ""double"" value'""")

    def testEnvironmentWithEnvVars(self):
        gwjob = GenericWorkflowJob("jobEnvironment", executable=self.gw_exec)
        gwjob.environment = {"TEST_ENV_VAR": "<ENV:CTRL_BPS_DIR>/tests"}
        htc_commands = _translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_ENV_VAR='$ENV(CTRL_BPS_DIR)/tests'")


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


class WmsIdToDirTestCase(unittest.TestCase):
    """Test _wms_id_to_dir function."""

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testInvalidIdType(self, _wms_id_type_mock):
        _wms_id_type_mock.return_value = WmsIdType.UNKNOWN
        with self.assertRaises(TypeError) as cm:
            _, _ = _wms_id_to_dir("not_used")
        self.assertIn("Invalid job id type", str(cm.exception))

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testAbsPathId(self, mock_wms_id_type):
        mock_wms_id_type.return_value = WmsIdType.PATH
        with temporaryDirectory() as tmp_dir:
            wms_path, id_type = _wms_id_to_dir(tmp_dir)
            self.assertEqual(id_type, WmsIdType.PATH)
            self.assertEqual(Path(tmp_dir).resolve(), wms_path)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testRelPathId(self, _wms_id_type_mock):
        _wms_id_type_mock.return_value = WmsIdType.PATH
        orig_dir = Path.cwd()
        with temporaryDirectory() as tmp_dir:
            os.chdir(tmp_dir)
            abs_path = Path(tmp_dir) / "newdir"
            abs_path.mkdir()
            wms_path, id_type = _wms_id_to_dir("newdir")
            self.assertEqual(id_type, WmsIdType.PATH)
            self.assertEqual(abs_path.resolve(), wms_path)
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
            "NodeStatus": NodeStatus.NOT_READY,
            "ProcId": 0,
            "bps_job_label": "service_provisioningJob",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.RUNNING,
        }

        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.COMPLETED,
            "ExitCode": 4,
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "FAILED", "status_details": ""}
        )

    def testDeleted(self):
        # Deleted by user (never held)
        job_ad = {
            "ClusterId": 9086,
            "DAGNodeName": "provisioningJob",
            "JobStatus": JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "via condor_rm (by user mgower)",
            "job_evicted_time": "2025-02-11T11:35:04",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.COMPLETED,
            "ExitCode": 0,
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.REMOVED,
            "Reason": "Removed by DAGMan (by user mgower)",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
        self.assertEqual(
            results.context, {"job_name": "provisioningJob", "status": "SUCCEEDED", "status_details": ""}
        )

    def testSucceed(self):
        # DAG completed, job was in running state when removed.
        job_ad = {
            "ClusterId": 8761,
            "ProcId": 0,
            "DAGNodeName": "provisioningJob",
            "JobStatus": JobStatus.REMOVED,
            "Reason": (
                "removed because <OtherJobRemoveRequirements = DAGManJobId =?= 8556>"
                " fired when job (8556.0) was removed"
            ),
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.HELD,
            "HoldReason": "via condor_hold (by user mgower)",
            "HoldReasonCode": 1,
            "HoldReasonSubCode": 0,
        }

        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "Removed by DAGMan (by user mgower)",
            "job_held_time": "2025-02-07T12:50:07",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.RUNNING,
            "LogNotes": "DAG Node: provisioningJob",
            "ProcId": 0,
            "job_held_time": "2025-02-07T12:33:34",
            "job_released_time": "2025-02-07T12:33:47",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.COMPLETED,
            "ProcId": 0,
            "Reason": "via condor_release (by user mgower)",
            "ReturnValue": 4,
            "TerminatedNormally": True,
            "job_held_time": "2025-02-11T11:46:40",
            "job_released_time": "2025-02-11T11:46:47",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.COMPLETED,
            "ProcId": 0,
            "Reason": "via condor_release (by user mgower)",
            "TerminatedNormally": True,
            "job_held_time": "2025-02-11T11:55:20",
            "job_released_time": "2025-02-11T11:55:25",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "removed because <OtherJobRemoveRequirements = DAGManJobId =?= "
            "8624> fired when job (8624.0) was removed",
            "job_held_time": "2025-02-07T12:33:34",
            "job_released_time": "2025-02-07T12:33:47",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "via condor_rm (by user mgower)",
            "job_evicted_time": "2025-02-11T11:35:04",
            "job_held_time": "2025-02-11T11:35:04",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
            "JobStatus": JobStatus.REMOVED,
            "ProcId": 0,
            "Reason": "Removed by DAGMan (by user mgower)",
            "TerminatedNormally": False,
            "job_held_time": "2025-02-07T12:36:15",
            "job_released_time": "2025-02-07T12:36:07",
        }
        results = WmsSpecificInfo()
        _add_service_job_specific_info(job_ad, results)
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
        results = _get_run_summary(job_ad)
        self.assertEqual(results, summary)

    def testRunSummaryInJobAd(self):
        summary = "pipetaskInit:1;label1:2;label2:2;finalJob:1"
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "bps_run_summary": summary}
        results = _get_run_summary(job_ad)
        self.assertEqual(results, summary)

    def testSummaryFromDag(self):
        with temporaryDirectory() as tmp_dir:
            copy2(f"{TESTDIR}/data/good.dag", tmp_dir)
            job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "Iwd": tmp_dir}
            results = _get_run_summary(job_ad)
            self.assertEqual(results, "pipetaskInit:1;label1:1;label2:1;label3:1;finalJob:1")

    def testSummaryNoDag(self):
        with self.assertLogs(logger=logger, level="WARNING") as cm:
            with temporaryDirectory() as tmp_dir:
                job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "Iwd": tmp_dir}
                results = _get_run_summary(job_ad)
                self.assertEqual(results, "")
            self.assertIn("lsst.ctrl.bps.htcondor", cm.records[0].name)
            self.assertIn("Could not get run summary for htcondor job", cm.output[0])


class IsServiceJobTestCase(unittest.TestCase):
    """Test is_service_job function."""

    def testNotServiceJob(self):
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "bps_job_type": "payload"}
        self.assertFalse(is_service_job(job_ad))

    def testIsServiceJob(self):
        job_ad = {"ClusterId": 8659, "DAGNodeName": "testJob", "bps_job_type": "service"}
        self.assertTrue(is_service_job(job_ad))

    def testMissingBpsType(self):
        job_ad = {
            "ClusterId": 8659,
            "DAGNodeName": "testJob",
        }
        self.assertFalse(is_service_job(job_ad))


class CreateDetailedReportFromJobsTestCase(unittest.TestCase):
    """Test _create_detailed_report_from_jobs function."""

    def testTinySuccess(self):
        with temporaryDirectory() as tmp_dir:
            test_submit_dir = os.path.join(tmp_dir, "tiny_success")
            copytree(f"{TESTDIR}/data/tiny_success", test_submit_dir)
            wms_workflow_id, jobs, message = _get_info_from_path(test_submit_dir)
            run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
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
            wms_workflow_id, jobs, message = _get_info_from_path(test_submit_dir)
            run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
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
            wms_workflow_id, jobs, message = _get_info_from_path(test_submit_dir)
            run_reports = _create_detailed_report_from_jobs(wms_workflow_id, jobs)
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


if __name__ == "__main__":
    unittest.main()
