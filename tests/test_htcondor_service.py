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
import unittest

import htcondor

from lsst.ctrl.bps import (
    BpsConfig,
    WmsStates,
)
from lsst.ctrl.bps.htcondor import htcondor_service
from lsst.ctrl.bps.htcondor.htcondor_config import HTC_DEFAULTS_URI

logger = logging.getLogger("lsst.ctrl.bps.htcondor")

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

    @unittest.mock.patch.object(htcondor.SecMan, "ping", return_value=PING_SUCCESS)
    @unittest.mock.patch.object(htcondor.Collector, "locate", return_value=LOCATE_SUCCESS)
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

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_id")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._locate_schedds")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
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

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_id")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._locate_schedds")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
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

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._get_status_from_path")
    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
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

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.htcondor_service._wms_id_type")
    def testGetStatusUnknownType(self, mock_type):
        mock_type.return_value = htcondor_service.WmsIdType.UNKNOWN

        fake_id = "100.0"
        state, message = self.service.get_status(fake_id)

        mock_type.assert_called_once_with(fake_id)

        self.assertEqual(state, WmsStates.UNKNOWN)
        self.assertEqual(message, "Invalid job id")
