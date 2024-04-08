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

"""Unit tests for job ClassAd handlers."""

import logging
import unittest
from typing import Any

from lsst.ctrl.bps.htcondor.handlers import (
    Chain,
    Handler,
    JobCompletedWithExecTicketHandler,
    JobCompletedWithoutExecTicketHandler,
    JobHeldByOtherHandler,
    JobHeldBySignalHandler,
    JobHeldByUserHandler,
)

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class DummyHandler(Handler):
    """A concrete handler that does nothing."""

    def handle(self, ad: dict[str, Any]) -> dict[str, Any]:
        pass


class RaisingHandler(Handler):
    """A concrete handler that raises KeyError exception."""

    def handle(self, ad: dict[str, Any]) -> dict[str, Any]:
        raise KeyError("foo")


class ChainTestCase(unittest.TestCase):
    """Test the Chain class."""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testDefaultInitialization(self):
        chain = Chain()
        self.assertEqual(len(chain), 0)

    def testCustomInitialization(self):
        handler = DummyHandler()
        chain = Chain(handlers=[handler])
        self.assertEqual(len(chain), 1)
        self.assertIs(chain[0], handler)

    def testAppendingHandler(self):
        first = DummyHandler()
        second = DummyHandler()
        chain = Chain(handlers=[first])
        chain.append(second)
        self.assertEqual(len(chain), 2)
        self.assertIs(chain[0], first)
        self.assertIs(chain[1], second)

    def testAppendingNonHandler(self):
        handler = "foo"
        chain = Chain()
        with self.assertRaises(TypeError):
            chain.append(handler)


class JobCompletedWithExecTicketHandlerTestCase(unittest.TestCase):
    """Test the handler for a completed job with the ticket of execution."""

    def setUp(self):
        self.ad = {"ClusterId": 1, "ProcId": 0, "MyType": "JobTerminatedEvent"}
        self.handler = JobCompletedWithExecTicketHandler()

    def tearDown(self):
        pass

    def testNormalTermination(self):
        ad = self.ad | {"ToE": {"ExitBySignal": False, "ExitCode": 0}}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertFalse(result["ExitBySignal"])
        self.assertIn("ExitCode", result)
        self.assertEqual(result["ExitCode"], 0)

    def testAbnormalTermination(self):
        ad = self.ad | {"ToE": {"ExitBySignal": True, "ExitSignal": 9}}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertTrue(result["ExitBySignal"])
        self.assertIn("ExitSignal", result)
        self.assertEqual(result["ExitSignal"], 9)

    def testNotHandlingMissingExecTicket(self):
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(self.ad)
        self.assertIsNone(result)
        self.assertIn("ticket of execution", cm.output[0])
        self.assertIn("missing", cm.output[0])

    def testNotHandlingJobNotCompleted(self):
        ad = self.ad | {"MyType": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("job not completed", cm.output[0])


class JobCompletedWithoutExecTicketHandlerTestCase(unittest.TestCase):
    """Test the handler for a completed job w/o the ticket of execution."""

    def setUp(self):
        self.ad = {"ClusterId": 1, "ProcId": 0, "MyType": "JobTerminatedEvent"}
        self.handler = JobCompletedWithoutExecTicketHandler()

    def tearDown(self):
        pass

    def testNormalTermination(self):
        ad = self.ad | {"TerminatedNormally": True, "ReturnValue": 0}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertFalse(result["ExitBySignal"])
        self.assertIn("ExitCode", result)
        self.assertEqual(result["ExitCode"], 0)

    def testAbnormalTermination(self):
        ad = self.ad | {"TerminatedNormally": False, "TerminatedBySignal": 9}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertTrue(result["ExitBySignal"])
        self.assertIn("ExitSignal", result)
        self.assertEqual(result["ExitSignal"], 9)

    def testNotHandlingExecTicketExists(self):
        ad = self.ad | {"ToE": {"ExitBySignal": False, "ExitCode": 0}}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("ticket of execution", cm.output[0])
        self.assertIn("found", cm.output[0])

    def testNotHandlingJobNotCompleted(self):
        ad = self.ad | {"MyType": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("job not completed", cm.output[0])


class JobHeldOtherTestCase(unittest.TestCase):
    """Test the handler for a held job."""

    def setUp(self):
        self.ad = {"ClusterId": 1, "ProcId": 0, "MyType": "JobHeldEvent"}
        self.handler = JobHeldByOtherHandler()

    def tearDown(self):
        pass

    def testHeld(self):
        ad = self.ad | {"HoldReasonCode": 42}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertFalse(result["ExitBySignal"])
        self.assertIn("ExitCode", result)
        self.assertEqual(result["ExitCode"], 42)

    def testHeldBySignal(self):
        ad = self.ad | {"HoldReasonCode": 3}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("invalid hold reason code", cm.output[0])
        self.assertIn("HoldReasonCode = 3", cm.output[0])

    def testHeldByUser(self):
        ad = self.ad | {"HoldReasonCode": 1}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("invalid hold reason code", cm.output[0])
        self.assertIn("HoldReasonCode = 1", cm.output[0])

    def testNotHandlingJobNotHeld(self):
        ad = self.ad | {"MyType": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("job not held", cm.output[0])


class JobHeldBySignalHandlerTestCase(unittest.TestCase):
    """Test the handler for a job held by a signal."""

    def setUp(self):
        self.ad = {"ClusterId": 1, "ProcId": 0, "MyType": "JobHeldEvent"}
        self.handler = JobHeldBySignalHandler()

    def tearDown(self):
        pass

    def testSignalAvailable(self):
        ad = self.ad | {"HoldReasonCode": 3, "HoldReason": "Job raised a signal 9."}
        result = self.handler.handle(ad)
        self.assertIsNotNone(ad)
        self.assertIn("ExitBySignal", result)
        self.assertTrue(result["ExitBySignal"])
        self.assertIn("ExitSignal", result)
        self.assertEqual(int(result["ExitSignal"]), 9)

    def testSignalNotAvailable(self):
        ad = self.ad | {"HoldReasonCode": 3, "HoldReason": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("signal not found", cm.output[0])

    def testNotHandlingInvalidHoldReasonCode(self):
        ad = self.ad | {"HoldReasonCode": 1, "HoldReason": "via condor_hold (by user foo)"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("not held by a signal", cm.output[0])

    def testNotHandlingJobNotHeld(self):
        ad = self.ad | {"MyType": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("job not held", cm.output[0])


class JobHeldByUserHandlerTestCase(unittest.TestCase):
    """Test the handler for a job held by the user."""

    def setUp(self):
        self.ad = {"ClusterId": 1, "ProcId": 0, "MyType": "JobHeldEvent"}
        self.handler = JobHeldByUserHandler()

    def tearDown(self):
        pass

    def testHandling(self):
        ad = self.ad | {"HoldReasonCode": 1}
        result = self.handler.handle(ad)
        self.assertIsNotNone(result)
        self.assertIn("ExitBySignal", result)
        self.assertFalse(result["ExitBySignal"])
        self.assertIn("ExitCode", result)
        self.assertEqual(result["ExitCode"], 0)

    def testNotHandlingInvalidHoldReaconCode(self):
        ad = self.ad | {"HoldReasonCode": 3, "HoldReason": "Job raised a signal 9."}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("not held by the user", cm.output[0])

    def testNotHandlingJobNotHeld(self):
        ad = self.ad | {"MyType": "foo"}
        with self.assertLogs(logger=logger, level="DEBUG") as cm:
            result = self.handler.handle(ad)
        self.assertIsNone(result)
        self.assertIn("job not held", cm.output[0])
