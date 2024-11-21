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

"""Unit tests for the util classes and functions."""

import logging
import os
import unittest
from pathlib import Path

from lsst.ctrl.bps.htcondor.utils import WmsIdType, _wms_id_to_dir
from lsst.utils.tests import temporaryDirectory

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class WmsIdToDirTestCase(unittest.TestCase):
    """Test _wms_id_to_dir function."""

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.utils._wms_id_type")
    def testInvalidIdType(self, _wms_id_type_mock):
        _wms_id_type_mock.return_value = WmsIdType.UNKNOWN
        with self.assertRaises(TypeError) as cm:
            _, _ = _wms_id_to_dir("not_used")
        self.assertIn("Invalid job id type", str(cm.exception))

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.utils._wms_id_type")
    def testAbsPathId(self, mock_wms_id_type):
        mock_wms_id_type.return_value = WmsIdType.PATH
        with temporaryDirectory() as tmp_dir:
            wms_path, id_type = _wms_id_to_dir(tmp_dir)
            self.assertEqual(id_type, WmsIdType.PATH)
            self.assertEqual(Path(tmp_dir).resolve(), wms_path)

    @unittest.mock.patch("lsst.ctrl.bps.htcondor.utils._wms_id_type")
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
