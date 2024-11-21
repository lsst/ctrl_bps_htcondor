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

"""Unit tests for the prepare-related functions."""

import logging
import unittest

from lsst.ctrl.bps import GenericWorkflowExec, GenericWorkflowJob
from lsst.ctrl.bps.htcondor.prepare import _translate_job_cmds

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class TranslateJobCmdsTestCase(unittest.TestCase):
    """Test _translate_job_cmds method."""

    def setUp(self):
        self.gw_exec = GenericWorkflowExec("test_exec", "/dummy/dir/pipetask")
        self.cached_vals = {"profile": {}}

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
