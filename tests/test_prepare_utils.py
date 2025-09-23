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

"""Unit tests for prepare utility functions."""

import logging
import os
import unittest

from lsst.ctrl.bps import (
    BPS_DEFAULTS,
    BPS_SEARCH_ORDER,
    BpsConfig,
    GenericWorkflowExec,
    GenericWorkflowFile,
    GenericWorkflowJob,
)
from lsst.ctrl.bps.htcondor import prepare_utils
from lsst.ctrl.bps.tests.gw_test_utils import make_3_label_workflow, make_3_label_workflow_groups_sort

logger = logging.getLogger("lsst.ctrl.bps.htcondor")

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class TranslateJobCmdsTestCase(unittest.TestCase):
    """Test _translate_job_cmds method."""

    def setUp(self):
        self.gw_exec = GenericWorkflowExec("test_exec", "/dummy/dir/pipetask")
        self.cached_vals = {"profile": {}, "bpsUseShared": True, "memoryLimit": 32768}

    def testRetryUnlessNone(self):
        gwjob = GenericWorkflowJob("retryUnless", "label1", executable=self.gw_exec)
        gwjob.retry_unless_exit = None
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertNotIn("retry_until", htc_commands)

    def testRetryUnlessInt(self):
        gwjob = GenericWorkflowJob("retryUnlessInt", "label1", executable=self.gw_exec)
        gwjob.retry_unless_exit = 3
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(int(htc_commands["retry_until"]), gwjob.retry_unless_exit)

    def testRetryUnlessList(self):
        gwjob = GenericWorkflowJob("retryUnlessList", "label1", executable=self.gw_exec)
        gwjob.retry_unless_exit = [1, 2]
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["retry_until"], "member(ExitCode, {1,2})")

    def testRetryUnlessBad(self):
        gwjob = GenericWorkflowJob("retryUnlessBad", "label1", executable=self.gw_exec)
        gwjob.retry_unless_exit = "1,2,3"
        with self.assertRaises(ValueError) as cm:
            _ = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertIn("retryUnlessExit", str(cm.exception))

    def testEnvironmentBasic(self):
        gwjob = GenericWorkflowJob("jobEnvironment", "label1", executable=self.gw_exec)
        gwjob.environment = {"TEST_INT": 1, "TEST_STR": "TWO"}
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_INT=1 TEST_STR='TWO'")

    def testEnvironmentSpaces(self):
        gwjob = GenericWorkflowJob("jobEnvironment", "label1", executable=self.gw_exec)
        gwjob.environment = {"TEST_SPACES": "spacey value"}
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_SPACES='spacey value'")

    def testEnvironmentSingleQuotes(self):
        gwjob = GenericWorkflowJob("jobEnvironment", "label1", executable=self.gw_exec)
        gwjob.environment = {"TEST_SINGLE_QUOTES": "spacey 'quoted' value"}
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_SINGLE_QUOTES='spacey ''quoted'' value'")

    def testEnvironmentDoubleQuotes(self):
        gwjob = GenericWorkflowJob("jobEnvironment", "label1", executable=self.gw_exec)
        gwjob.environment = {"TEST_DOUBLE_QUOTES": 'spacey "double" value'}
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], """TEST_DOUBLE_QUOTES='spacey ""double"" value'""")

    def testEnvironmentWithEnvVars(self):
        gwjob = GenericWorkflowJob("jobEnvironment", "label1", executable=self.gw_exec)
        gwjob.environment = {"TEST_ENV_VAR": "<ENV:CTRL_BPS_DIR>/tests"}
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        self.assertEqual(htc_commands["environment"], "TEST_ENV_VAR='$ENV(CTRL_BPS_DIR)/tests'")

    def testPeriodicRelease(self):
        gwjob = GenericWorkflowJob("periodicRelease", "label1", executable=self.gw_exec)
        gwjob.request_memory = 2048
        gwjob.memory_multiplier = 2
        gwjob.number_of_retries = 3
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        release = (
            "JobStatus == 5 && NumJobStarts <= JobMaxRetries && "
            "(HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 || "
            "HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34) && "
            "min({int(2048 * pow(2, NumJobStarts - 1)), 32768}) < 32768"
        )
        self.assertEqual(htc_commands["periodic_release"], release)

    def testPeriodicRemoveNoRetries(self):
        gwjob = GenericWorkflowJob("periodicRelease", "label1", executable=self.gw_exec)
        gwjob.request_memory = 2048
        gwjob.memory_multiplier = 1
        gwjob.number_of_retries = 0
        htc_commands = prepare_utils._translate_job_cmds(self.cached_vals, None, gwjob)
        remove = "JobStatus == 5 && (NumJobStarts > JobMaxRetries)"
        self.assertEqual(htc_commands["periodic_remove"], remove)
        self.assertEqual(htc_commands["max_retries"], 0)


class GroupToSubdagTestCase(unittest.TestCase):
    """Test _group_to_subdag function."""

    def testBlocking(self):
        gw = make_3_label_workflow_groups_sort("test1", True)
        gwjob = gw.get_job("group_order1_10001")
        config = BpsConfig(
            {},
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
        )

        htc_job = prepare_utils._group_to_subdag(config, gwjob, "the_prefix")
        self.assertEqual(len(htc_job.subdag), len(gwjob))


class GatherSiteValuesTestCase(unittest.TestCase):
    """Test _gather_site_values function."""

    def testAllThere(self):
        config = BpsConfig(
            {},
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
        )
        compute_site = "notThere"
        results = prepare_utils._gather_site_values(config, compute_site)
        self.assertEqual(results["memoryLimit"], BPS_DEFAULTS["memoryLimit"])

    def testNotSpecified(self):
        config = BpsConfig(
            {},
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        compute_site = "notThere"
        results = prepare_utils._gather_site_values(config, compute_site)
        self.assertEqual(results["memoryLimit"], BPS_DEFAULTS["memoryLimit"])


class GatherLabelValuesTestCase(unittest.TestCase):
    """Test _gather_labels_values function."""

    def testClusterLabel(self):
        # Test cluster value overrides pipetask.
        label = "label1"
        config = BpsConfig(
            {
                "cluster": {
                    "label1": {
                        "releaseExpr": "cluster_val",
                        "overwriteJobFiles": False,
                        "profile": {"condor": {"prof_val1": 3}},
                    }
                },
                "pipetask": {"label1": {"releaseExpr": "pipetask_val"}},
            },
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        results = prepare_utils._gather_label_values(config, label)
        self.assertEqual(
            results,
            {
                "attrs": {},
                "profile": {"prof_val1": 3},
                "releaseExpr": "cluster_val",
                "overwriteJobFiles": False,
            },
        )

    def testPipetaskLabel(self):
        label = "label1"
        config = BpsConfig(
            {
                "pipetask": {
                    "label1": {
                        "releaseExpr": "pipetask_val",
                        "overwriteJobFiles": False,
                        "profile": {"condor": {"prof_val1": 3}},
                    }
                }
            },
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        results = prepare_utils._gather_label_values(config, label)
        self.assertEqual(
            results,
            {
                "attrs": {},
                "profile": {"prof_val1": 3},
                "releaseExpr": "pipetask_val",
                "overwriteJobFiles": False,
            },
        )

    def testNoSection(self):
        label = "notThere"
        config = BpsConfig(
            {},
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        results = prepare_utils._gather_label_values(config, label)
        self.assertEqual(results, {"attrs": {}, "profile": {}, "overwriteJobFiles": True})

    def testNoOverwriteSpecified(self):
        label = "notthere"
        config = BpsConfig(
            {},
            search_order=BPS_SEARCH_ORDER,
            defaults={},
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        results = prepare_utils._gather_label_values(config, label)
        self.assertEqual(results, {"attrs": {}, "profile": {}, "overwriteJobFiles": True})

    def testFinalJob(self):
        label = "finalJob"
        config = BpsConfig(
            {"finalJob": {"profile": {"condor": {"prof_val2": 6, "+attr_val1": 5}}}},
            search_order=BPS_SEARCH_ORDER,
            defaults=BPS_DEFAULTS,
            wms_service_class_fqn="lsst.ctrl.bps.htcondor.HTCondorService",
        )
        results = prepare_utils._gather_label_values(config, label)
        self.assertEqual(
            results, {"attrs": {"attr_val1": 5}, "profile": {"prof_val2": 6}, "overwriteJobFiles": False}
        )


class CreateCheckJobTestCase(unittest.TestCase):
    """Test _create_check_job function."""

    def testSuccess(self):
        group_job_name = "group_order1_val1a"
        job_label = "order1"
        job = prepare_utils._create_check_job(group_job_name, job_label)
        self.assertIn(group_job_name, job.name)
        self.assertEqual(job.label, job_label)
        self.assertIn("check_group_status.sub", job.subfile)


class CreatePeriodicReleaseExprTestCase(unittest.TestCase):
    """Test _create_periodic_release_expr function."""

    def testNoReleaseExpr(self):
        results = prepare_utils._create_periodic_release_expr(2048, 1, 32768, "")
        self.assertEqual(results, "")

    def testMultiplierNone(self):
        results = prepare_utils._create_periodic_release_expr(2048, None, 32768, "")
        self.assertEqual(results, "")

    def testJustMemoryReleaseExpr(self):
        self.maxDiff = None  # so test error shows entire strings
        results = prepare_utils._create_periodic_release_expr(2048, 2, 32768, "")
        truth = (
            "JobStatus == 5 && NumJobStarts <= JobMaxRetries && "
            "(HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 || "
            "HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34) && "
            "min({int(2048 * pow(2, NumJobStarts - 1)), 32768}) < 32768"
        )
        self.assertEqual(results, truth)

    def testJustUserReleaseExpr(self):
        results = prepare_utils._create_periodic_release_expr(2048, 1, 32768, "True")
        truth = "JobStatus == 5 && NumJobStarts <= JobMaxRetries && HoldReasonCode =!= 1 && True"
        self.assertEqual(results, truth)

    def testJustUserReleaseExprMultiplierNone(self):
        results = prepare_utils._create_periodic_release_expr(2048, None, 32768, "True")
        truth = "JobStatus == 5 && NumJobStarts <= JobMaxRetries && HoldReasonCode =!= 1 && True"
        self.assertEqual(results, truth)

    def testMemoryAndUserReleaseExpr(self):
        self.maxDiff = None  # so test error shows entire strings
        results = prepare_utils._create_periodic_release_expr(2048, 2, 32768, "True")
        truth = (
            "JobStatus == 5 && NumJobStarts <= JobMaxRetries && "
            "((HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 || "
            "HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34) && "
            "min({int(2048 * pow(2, NumJobStarts - 1)), 32768}) < 32768 || "
            "HoldReasonCode =!= 1 && True)"
        )
        self.assertEqual(results, truth)


class CreatePeriodicRemoveExprTestCase(unittest.TestCase):
    """Test _create_periodic_release_expr function."""

    def testBasicRemoveExpr(self):
        """Function assumes only called if max_retries >= 0."""
        results = prepare_utils._create_periodic_remove_expr(2048, 1, 32768)
        truth = "JobStatus == 5 && (NumJobStarts > JobMaxRetries)"
        self.assertEqual(results, truth)

    def testBasicRemoveExprMultiplierNone(self):
        """Function assumes only called if max_retries >= 0."""
        results = prepare_utils._create_periodic_remove_expr(2048, None, 32768)
        truth = "JobStatus == 5 && (NumJobStarts > JobMaxRetries)"
        self.assertEqual(results, truth)

    def testMemoryRemoveExpr(self):
        self.maxDiff = None  # so test error shows entire strings
        results = prepare_utils._create_periodic_remove_expr(2048, 2, 32768)
        truth = (
            "JobStatus == 5 && (NumJobStarts > JobMaxRetries || "
            "((HoldReasonCode =?= 34 && HoldReasonSubCode =?= 0 || "
            "HoldReasonCode =?= 3 && HoldReasonSubCode =?= 34) && "
            "min({int(2048 * pow(2, NumJobStarts - 1)), 32768}) == 32768))"
        )
        self.assertEqual(results, truth)


class HandleJobOutputsTestCase(unittest.TestCase):
    """Test _handle_job_outputs function."""

    def setUp(self):
        self.job_name = "test_job"
        self.out_prefix = "/test/prefix"

    def tearDown(self):
        pass

    def testNoOutputsSharedFilesystem(self):
        """Test with shared filesystem and no outputs."""
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = []

        result = prepare_utils._handle_job_outputs(mock_workflow, self.job_name, True, self.out_prefix)

        self.assertEqual(result, {"transfer_output_files": '""'})

    def testWithOutputsSharedFilesystem(self):
        """Test with shared filesystem and outputs present (still empty)."""
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = [
            GenericWorkflowFile(name="output.txt", src_uri="/path/to/output.txt")
        ]

        result = prepare_utils._handle_job_outputs(mock_workflow, self.job_name, True, self.out_prefix)

        self.assertEqual(result, {"transfer_output_files": '""'})

    def testNoOutputsNoSharedFilesystem(self):
        """Test without shared filesystem and no outputs."""
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = []

        result = prepare_utils._handle_job_outputs(mock_workflow, self.job_name, False, self.out_prefix)

        self.assertEqual(result, {"transfer_output_files": '""'})

    def testWithAnOutputNoSharedFilesystem(self):
        """Test without shared filesystem and single output file."""
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = [
            GenericWorkflowFile(name="output.txt", src_uri="/path/to/output.txt")
        ]

        result = prepare_utils._handle_job_outputs(mock_workflow, self.job_name, False, self.out_prefix)

        expected = {
            "transfer_output_files": "output.txt",
            "transfer_output_remaps": '"output.txt=/path/to/output.txt"',
        }
        self.assertEqual(result, expected)

    def testWithOutputsNoSharedFilesystem(self):
        """Test without shared filesystem and multiple output files."""
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = [
            GenericWorkflowFile(name="output1.txt", src_uri="/path/output1.txt"),
            GenericWorkflowFile(name="output2.txt", src_uri="/another/path/output2.txt"),
        ]

        result = prepare_utils._handle_job_outputs(mock_workflow, self.job_name, False, self.out_prefix)

        expected = {
            "transfer_output_files": "output1.txt,output2.txt",
            "transfer_output_remaps": '"output1.txt=/path/output1.txt;output2.txt=/another/path/output2.txt"',
        }
        self.assertEqual(result, expected)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.prepare_utils._LOG")
    def testLogging(self, mock_log):
        mock_workflow = unittest.mock.Mock()
        mock_workflow.get_job_outputs.return_value = [
            GenericWorkflowFile(name="output.txt", src_uri="/path/to/output.txt")
        ]

        prepare_utils._handle_job_outputs(mock_workflow, self.job_name, False, self.out_prefix)

        self.assertTrue(mock_log.debug.called)
        debug_calls = mock_log.debug.call_args_list
        self.assertTrue(any("src_uri=" in str(call) for call in debug_calls))
        self.assertTrue(any("transfer_output_files=" in str(call) for call in debug_calls))
        self.assertTrue(any("transfer_output_remaps=" in str(call) for call in debug_calls))


class CreateJobTestCase(unittest.TestCase):
    """Test _create_job function."""

    def setUp(self):
        self.generic_workflow = make_3_label_workflow("test1", True)

    def testNoOverwrite(self):
        template = "{label}/{tract}/{patch}/{band}/{subfilter}/{physical_filter}/{visit}/{exposure}"
        cached_values = {
            "bpsUseShared": True,
            "overwriteJobFiles": False,
            "memoryLimit": 491520,
            "profile": {},
            "attrs": {},
        }
        gwjob = self.generic_workflow.get_final()
        out_prefix = "submit"
        htc_job = prepare_utils._create_job(template, cached_values, self.generic_workflow, gwjob, out_prefix)
        self.assertEqual(htc_job.name, gwjob.name)
        self.assertEqual(htc_job.label, gwjob.label)
        self.assertIn("NumJobStarts", htc_job.cmds["output"])
        self.assertIn("NumJobStarts", htc_job.cmds["error"])
        self.assertNotIn("NumJobStarts", htc_job.cmds["log"])
        self.assertTrue(htc_job.cmds["error"].endswith(".out"))
        self.assertTrue(htc_job.cmds["output"].endswith(".out"))
        self.assertTrue(htc_job.cmds["log"].endswith(".log"))


if __name__ == "__main__":
    unittest.main()
