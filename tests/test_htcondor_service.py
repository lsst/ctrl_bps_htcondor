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
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from htcondor2 import Collector, HTCondorException

import lsst.ctrl.bps.htcondor.lssthtc as lssthtc
from lsst.ctrl.bps import BpsConfig, WmsStates
from lsst.ctrl.bps.htcondor import htcondor_service
from lsst.ctrl.bps.htcondor.htcondor_config import HTC_DEFAULTS_URI
from lsst.ctrl.bps.htcondor.htcondor_workflow import HTCondorWorkflow
from lsst.ctrl.bps.tests.gw_test_utils import make_3_label_workflow
from lsst.daf.butler import Config

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
        self.service = htcondor_service.HTCondorService(config)

    def tearDown(self):
        pass

    def testDefaults(self):
        self.assertEqual(self.service.defaults["memoryLimit"], 491520)

    def testDefaultsPath(self):
        self.assertEqual(self.service.defaults_uri, HTC_DEFAULTS_URI)
        self.assertFalse(self.service.defaults_uri.isdir())

    @patch("lsst.ctrl.bps.htcondor.htcondor_service.ping", return_value=PING_SUCCESS)
    @patch.object(Collector, "locate", return_value=LOCATE_SUCCESS)
    def testPingSuccess(self, mock_locate, mock_ping):
        status, message = self.service.ping(None)
        self.assertEqual(status, 0)
        self.assertEqual(message, "")

    def testPingFailure(self):
        with patch("lsst.ctrl.bps.htcondor.htcondor_service.Collector.locate") as locate_mock:
            locate_mock.side_effect = HTCondorException("Unable to locate local daemon.")
            status, message = self.service.ping(None)
            self.assertEqual(status, 1)
            self.assertIn(message, "Could not locate Schedd service.")

    @patch.object(Collector, "locate", return_value=LOCATE_SUCCESS)
    def testPingPermission(self, mock_locate):
        with patch("lsst.ctrl.bps.htcondor.htcondor_service.ping") as ping_mock:
            ping_mock.side_effect = HTCondorException("Failed to connect to schedd.")
            status, message = self.service.ping(None)
            self.assertEqual(status, 1)
            self.assertEqual(message, "Permission problem with Schedd service.")

    @patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_id")
    @patch("lsst.ctrl.bps.htcondor.htcondor_service._locate_schedds")
    @patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testGetStatusLocal(self, mock_type, mock_locate, mock_status):
        mock_type.return_value = htcondor_service.WmsIdType.LOCAL
        mock_locate.return_value = {}
        mock_status.return_value = (WmsStates.RUNNING, "")

        fake_id = "100"
        state, message = self.service.get_status(fake_id)

        mock_type.assert_called_once_with(fake_id)
        mock_locate.assert_called_once_with(locate_all=False)
        mock_status.assert_called_once_with(fake_id, 1, schedds={})

        self.assertEqual(state, WmsStates.RUNNING)
        self.assertEqual(message, "")

    @patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_id")
    @patch("lsst.ctrl.bps.htcondor.htcondor_service._locate_schedds")
    @patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testGetStatusGlobal(self, mock_type, mock_locate, mock_status):
        mock_type.return_value = htcondor_service.WmsIdType.GLOBAL
        mock_locate.return_value = {}
        fake_message = ""
        mock_status.return_value = (WmsStates.RUNNING, fake_message)

        fake_id = "100"
        state, message = self.service.get_status(fake_id, 2)

        mock_type.assert_called_once_with(fake_id)
        mock_locate.assert_called_once_with(locate_all=True)
        mock_status.assert_called_once_with(fake_id, 2, schedds={})

        self.assertEqual(state, WmsStates.RUNNING)
        self.assertEqual(message, fake_message)

    @patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_path")
    @patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testGetStatusPath(self, mock_type, mock_status):
        fake_message = "fake message"
        mock_type.return_value = htcondor_service.WmsIdType.PATH
        mock_status.return_value = (WmsStates.FAILED, fake_message)

        fake_id = "/fake/path"
        state, message = self.service.get_status(fake_id)

        mock_type.assert_called_once_with(fake_id)
        mock_status.assert_called_once_with(fake_id)

        self.assertEqual(state, WmsStates.FAILED)
        self.assertEqual(message, fake_message)

    @patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testGetStatusUnknownType(self, mock_type):
        mock_type.return_value = htcondor_service.WmsIdType.UNKNOWN

        fake_id = "100.0"
        state, message = self.service.get_status(fake_id)

        mock_type.assert_called_once_with(fake_id)

        self.assertEqual(state, WmsStates.UNKNOWN)
        self.assertEqual(message, "Invalid job id")

    @patch("lsst.ctrl.bps.htcondor.htcondor_workflow.HTCondorWorkflow.write")
    def testPrepare(self, mock_write):
        generic_workflow = make_3_label_workflow("test1", True)
        config = BpsConfig(
            {
                "bpsUseShared": True,
                "overwriteJobFiles": False,
                "memoryLimit": 491520,
                "profile": {},
                "attrs": {},
                "nodeset": "set1",
            }
        )

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            htc_workflow = self.service.prepare(config, generic_workflow, tmpdir)
            mock_write.assert_called_once()
            self.assertEqual(len(htc_workflow.dag), 19)  # 3 visit * 2 detectors * 3 labels + init

    @patch("lsst.ctrl.bps.htcondor.htcondor_workflow.HTCondorWorkflow.write")
    def testPrepareProvision(self, mock_write):
        # Leaves testing provisioning code to test_provisioner.py.
        # Just checking HTCondorService.prepare bits (like nodeset).
        timestamp = "20260130T211713Z"
        generic_workflow = make_3_label_workflow("test1", True)
        config = BpsConfig(
            {
                "bpsUseShared": True,
                "overwriteJobFiles": False,
                "profile": {"requirements": "dummy_val == 3"},
                "attrs": {},
                "nodeset": "set1",  # this shouldn't be used with auto-provisioning
                "provisionResources": True,
                "provisioning": {"provisioningMaxWallTime": 1200},
                "bps_defined": {"timestamp": timestamp},
            },
            defaults=Config(HTC_DEFAULTS_URI),
        )

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            prov_config = Path(f"{tmpdir}/condor-info.py")
            config[".provisioning.provisioningScriptConfigPath"] = str(prov_config)
            config[".provisioning.provisioningScriptConfig"] = "foo"

            htc_workflow = self.service.prepare(config, generic_workflow, tmpdir)
            mock_write.assert_called_once()
            self.assertEqual(config[".bps_defined.nodeset"], timestamp)
            self.assertEqual(len(htc_workflow.dag), 19)  # 3 visit * 2 dets * 3 labels + init
            self.assertIsNotNone(htc_workflow.dag.graph["service_job"])

            prov_script = Path(tmpdir) / "provisioningJob.bash"
            self.assertTrue(prov_script.is_file())
            script_contents = prov_script.read_text()
            self.assertIn(f"--nodeset '{timestamp}'", script_contents)

    def testSubmitWithConfigPath(self):
        """Only testing value for wms_config_path being passed
        correctly to htc_create_submit_from_dag.  Aborting submission
        after that call to skip rest of submit function.
        """

        def _fake_htc_create_submit_from_dag(filename, submit_options, wms_config_path):
            raise RuntimeError("Fake exception from mock")

        dag_filename = "should_not_matter.dag"
        wms_config_path = "dagman.conf"
        submit_options = {"DAGMAN_MAX_JOBS_SUBMITTED": 30}
        attribs = {"bps_wms_config_path": wms_config_path}

        workflow = HTCondorWorkflow("testSuccess")
        workflow.dag = lssthtc.HTCDag("testSuccess")
        workflow.dag.graph["dag_filename"] = dag_filename
        workflow.dag.graph["attr"] = dict(attribs)
        workflow.dag.graph["submit_options"] = dict(submit_options)

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            workflow.submit_path = tmpdir
            with patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_create_submit_from_dag") as create_mock:
                create_mock.side_effect = _fake_htc_create_submit_from_dag
                with self.assertRaisesRegex(RuntimeError, "Fake exception from mock"):
                    self.service.submit(workflow)
                create_mock.assert_called_once_with(dag_filename, submit_options, wms_config_path)

    def testSubmitWithoutConfigPath(self):
        """Only testing that values are being passed correctly to
        htc_create_submit_from_dag when there isn't a wms config path.
        Aborting submission after that call to skip rest of submit function.
        """

        def _fake_htc_create_submit_from_dag(filename, submit_options, wms_config_path):
            raise RuntimeError("Fake exception from mock")

        dag_filename = "should_not_matter.dag"
        wms_config_path = None
        submit_options = {"DAGMAN_MAX_JOBS_SUBMITTED": 30}
        attribs = {}

        workflow = HTCondorWorkflow("testSuccess")
        workflow.dag = lssthtc.HTCDag("testSuccess")
        workflow.dag.graph["dag_filename"] = dag_filename
        workflow.dag.graph["attr"] = dict(attribs)
        workflow.dag.graph["submit_options"] = dict(submit_options)

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            workflow.submit_path = tmpdir
            with patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_create_submit_from_dag") as create_mock:
                create_mock.side_effect = _fake_htc_create_submit_from_dag
                with self.assertRaisesRegex(RuntimeError, "Fake exception from mock"):
                    self.service.submit(workflow)
                create_mock.assert_called_once_with(dag_filename, submit_options, wms_config_path)


class RestartTestCase(unittest.TestCase):
    """Test HTCondorService.restart using mocked lssthtc functions."""

    def setUp(self):
        config = BpsConfig({}, wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService")
        self.service = htcondor_service.HTCondorService(config)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testIdNotFound(self, mock_to_dir):
        mock_to_dir.return_value = (None, htcondor_service.WmsIdType.UNKNOWN)
        run_id, run_name, message = self.service.restart("bad_id")
        self.assertIsNone(run_id)
        self.assertIsNone(run_name)
        self.assertIn("not found", message)
        self.assertIn("submit directory", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testSubmitDirNotFound(self, mock_to_dir):
        mock_to_dir.return_value = (Path("/does/not/exist"), htcondor_service.WmsIdType.LOCAL)
        run_id, run_name, message = self.service.restart("100.0")
        self.assertIsNone(run_id)
        self.assertIsNone(run_name)
        self.assertIn("submit directory", message)
        self.assertIn("not found", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testNoRescueDag(self, mock_to_dir):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            run_id, run_name, message = self.service.restart(tmpdir)
            self.assertIsNone(run_id)
            self.assertIsNone(run_name)
            self.assertIn("rescue DAG", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.condor_q")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testAlreadyInQueue(self, mock_to_dir, mock_condor_q):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            (Path(tmpdir) / "test.dag.rescue001").touch()
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            mock_condor_q.return_value = {"schedd": {"1.0": {"GlobalJobId": "schedd#1.0#123"}}}
            run_id, run_name, message = self.service.restart(tmpdir)
            self.assertIsNone(run_id)
            self.assertIsNone(run_name)
            self.assertIn("already in the job queue", message)
            self.assertIn("schedd#1.0#123", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_status")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.condor_q")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testAllJobsFinished(self, mock_to_dir, mock_condor_q, mock_status):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            (Path(tmpdir) / "test.dag.rescue001").touch()
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            mock_condor_q.return_value = {}
            mock_status.return_value = {"NodesTotal": 5, "NodesDone": 5}
            run_id, run_name, message = self.service.restart(tmpdir)
            self.assertIsNone(run_id)
            self.assertIsNone(run_name)
            self.assertIn("finished successfully", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_backup_files")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_info")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_status")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.condor_q")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testNoCondorSub(self, mock_to_dir, mock_condor_q, mock_status, mock_read_info, mock_backup):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            (Path(tmpdir) / "test.dag.rescue001").touch()
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            mock_condor_q.return_value = {}
            mock_status.return_value = {"NodesTotal": 5, "NodesDone": 3}
            mock_read_info.return_value = (
                "info.json",
                {"schedd": {"1.0": {"bps_job_summary": "sum", "bps_run_quanta": "quanta"}}},
            )
            mock_backup.return_value = Path(tmpdir) / "test.dag.rescue001"
            run_id, run_name, message = self.service.restart(tmpdir)
            self.assertIsNone(run_id)
            self.assertIsNone(run_name)
            self.assertIn("submit description file not found", message)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_submit_dag")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_create_submit_from_file")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_backup_files")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_info")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_status")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.condor_q")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testSubmitInfoUnavailable(
        self,
        mock_to_dir,
        mock_condor_q,
        mock_status,
        mock_read_info,
        mock_backup,
        mock_create,
        mock_submit,
    ):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            (Path(tmpdir) / "test.dag.rescue001").touch()
            (Path(tmpdir) / "test.dag.condor.sub").touch()
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            mock_condor_q.return_value = {}
            mock_status.return_value = {"NodesTotal": 5, "NodesDone": 3}
            mock_read_info.return_value = (
                "info.json",
                {"schedd": {"1.0": {"bps_job_summary": "sum", "bps_run_quanta": "quanta"}}},
            )
            mock_backup.return_value = Path(tmpdir) / "test.dag.rescue001"
            mock_submit.return_value = {}
            run_id, run_name, message = self.service.restart(tmpdir)
            self.assertIsNone(run_id)
            self.assertIsNone(run_name)
            self.assertEqual(message, "DAGMan job information unavailable")

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.write_dag_info")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_submit_dag")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_create_submit_from_file")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.htc_backup_files")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_info")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.read_dag_status")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service.condor_q")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_to_dir")
    def testSuccess(
        self,
        mock_to_dir,
        mock_condor_q,
        mock_status,
        mock_read_info,
        mock_backup,
        mock_create,
        mock_submit,
        mock_write_info,
    ):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            (Path(tmpdir) / "test.dag.rescue001").touch()
            (Path(tmpdir) / "test.dag.condor.sub").touch()
            mock_to_dir.return_value = (Path(tmpdir), htcondor_service.WmsIdType.PATH)
            mock_condor_q.return_value = {}
            mock_status.return_value = {"NodesTotal": 5, "NodesDone": 3}
            info_filename = "info.json"
            mock_read_info.return_value = (
                info_filename,
                {"schedd": {"1.0": {"bps_job_summary": "sum", "bps_run_quanta": "quanta"}}},
            )
            mock_backup.return_value = Path(tmpdir) / "test.dag.rescue001"
            schedd_dag_info = {"schedd": {"2.0": {"ClusterId": 2, "ProcId": 0, "bps_run": "myrun"}}}
            mock_submit.return_value = schedd_dag_info

            run_id, run_name, message = self.service.restart(tmpdir)

            self.assertEqual(run_id, "2.0")
            self.assertEqual(run_name, "myrun")
            self.assertEqual(message, "")
            mock_write_info.assert_called_once_with(info_filename, schedd_dag_info)
            # Summaries from the previous run should be carried forward.
            dag_ad = schedd_dag_info["schedd"]["2.0"]
            self.assertEqual(dag_ad["bps_job_summary"], "sum")
            self.assertEqual(dag_ad["bps_run_quanta"], "quanta")


class RunSubmissionChecksTestCase(unittest.TestCase):
    """Test HTCondorService.run_submission_checks."""

    @staticmethod
    def _make_service(config_dict):
        config = BpsConfig(config_dict, wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService")
        return htcondor_service.HTCondorService(config)

    def testBpsMakeCommandMissing(self):
        # bpsMakeCommand absent defaults to True, so no checks are performed.
        service = self._make_service({})
        self.assertIsNone(service.run_submission_checks())

    def testBpsMakeCommandTrue(self):
        service = self._make_service({"bpsMakeCommand": True})
        self.assertIsNone(service.run_submission_checks())

    def testMissingPayloadCommand(self):
        service = self._make_service({"bpsMakeCommand": False})
        with self.assertRaisesRegex(KeyError, "Missing 'payloadCommand'"):
            service.run_submission_checks()

    def testPayloadCommandWithoutSetupEnv(self):
        # payloadCommand present but does not reference setupEnv, so the
        # remaining checks are skipped.
        service = self._make_service({"bpsMakeCommand": False, "payloadCommand": "run_thing --flag"})
        self.assertIsNone(service.run_submission_checks())

    def testMissingSetupEnv(self):
        service = self._make_service({"bpsMakeCommand": False, "payloadCommand": "run_thing {setupEnv}"})
        with self.assertRaisesRegex(KeyError, "Missing 'setupEnv'"):
            service.run_submission_checks()

    def testSetupEnvWithoutLsstVersion(self):
        service = self._make_service(
            {
                "bpsMakeCommand": False,
                "payloadCommand": "run_thing {setupEnv}",
                "setupEnv": "source /opt/lsst/setup.sh",
            }
        )
        self.assertIsNone(service.run_submission_checks())

    def testMissingLsstVersion(self):
        service = self._make_service(
            {
                "bpsMakeCommand": False,
                "payloadCommand": "run_thing {setupEnv}",
                "setupEnv": "setup lsst_distrib -t {lsstVersion}",
            }
        )
        with self.assertRaisesRegex(KeyError, "Missing 'lsstVersion'"):
            service.run_submission_checks()

    def testAllPresent(self):
        service = self._make_service(
            {
                "bpsMakeCommand": False,
                "payloadCommand": "run_thing {setupEnv}",
                "setupEnv": "setup lsst_distrib -t {lsstVersion}",
                "lsstVersion": "w_2026_01",
            }
        )
        self.assertIsNone(service.run_submission_checks())
