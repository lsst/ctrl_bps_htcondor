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

"""Unit tests for DagmanConfigurator class."""

import logging
import os
import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.htcondor import HTCDag
from lsst.ctrl.bps.htcondor.dagman_configurator import DagmanConfigurator

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class DagmanConfiguratorTestCase(unittest.TestCase):
    """Unit tests for DagmanConfigurator class."""

    def setUp(self):
        self.config = BpsConfig(
            {
                "site": {
                    "foo": {
                        "wmsConfig": {"DAGMAN_USE_STRICT": 1},
                    },
                },
                "wmsConfig": {"DAGMAN_USE_STRICT": 0},
            }
        )

    def tearDown(self):
        pass

    def testInitDefaultSearchOptions(self):
        """Test object instantiation with default search options."""
        configurator = DagmanConfigurator(self.config)
        self.assertIn("DAGMAN_USE_STRICT", configurator.options)
        self.assertEqual(configurator.options["DAGMAN_USE_STRICT"], 0)
        self.assertIsNone(configurator.config_path)
        self.assertIsNone(configurator.prefix)

    def testInitCustomSearchOptions(self):
        """Test object instantiation with custom search options."""
        configurator = DagmanConfigurator(self.config, search_opts={"curvals": {"computeSite": "foo"}})
        self.assertIn("DAGMAN_USE_STRICT", configurator.options)
        self.assertEqual(configurator.options["DAGMAN_USE_STRICT"], 1)
        self.assertIsNone(configurator.config_path)
        self.assertIsNone(configurator.prefix)

    def testInitWrongOptionType(self):
        self.config[".wmsConfig.DAGMAN_USE_STRICT"] = "foo"
        with self.assertRaisesRegex(ValidationError, "DAGMAN_USE_STRICT".lower()):
            DagmanConfigurator(self.config)

    def testInitUnsupportedDagmanOption(self):
        """Test object instantiation with unsupported DAGMAN options."""
        self.config[".wmsConfig.DAGMAN_UNSUPPORTED_OPTION"] = "foo"
        with self.assertLogs(logger=logger, level="WARNING") as cm:
            configurator = DagmanConfigurator(self.config)
            self.assertIn("DAGMAN_UNSUPPORTED_OPTION", cm.output[0])
            self.assertNotIn("DAGMAN_UNSUPPORTED_OPTION", configurator.options)

    def testInitNoWmsConfig(self):
        """Test object instantiation fails when no WMS-specific options."""
        del self.config["wmsConfig"]
        with self.assertRaisesRegex(KeyError, "not found"):
            DagmanConfigurator(self.config)

    def testPrepare(self):
        """Test if the method creates the configuration file."""
        configurator = DagmanConfigurator(self.config)
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            configurator.prepare("dagman.conf", prefix=tmpdir)
            self.assertIn(Path(tmpdir), list(configurator.config_path.parents))
            self.assertTrue(configurator.config_path.is_file())
            self.assertEqual(configurator.config_path.read_text(), "DAGMAN_USE_STRICT = 0")

    def testPrepareWithUnsupportedOption(self):
        """Test if the method does not include unsupported options."""
        self.config[".wmsConfig.DAGMAN_UNSUPPORTED_OPTION"] = "foo"
        configurator = DagmanConfigurator(self.config)
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            configurator.prepare("dagman.conf", prefix=tmpdir)
            self.assertIn(Path(tmpdir), list(configurator.config_path.parents))
            self.assertTrue(configurator.config_path.is_file())
            self.assertEqual(configurator.config_path.read_text(), "DAGMAN_USE_STRICT = 0")

    def testPrepareConfigWriteFailure(self):
        """Test if the method raises when it can't create the configuration."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            os.chmod(tmpdir, 0o500)

            configurator = DagmanConfigurator(self.config)
            with self.assertLogs(logger=logger, level="ERROR") as cm, self.assertRaises(OSError):
                configurator.prepare("dagman.conf", f"{tmpdir}/subdir")
            self.assertIn("Could not write", cm.output[0])

            os.chmod(tmpdir, 0o700)

    def testConfigure(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            dag = HTCDag(name="test_configure")
            configurator = DagmanConfigurator(self.config)
            configurator.prepare("dagman.conf", prefix=tmpdir)
            configurator.configure(dag)
        self.assertIn("bps_wms_config_path", dag.graph["attr"])
        self.assertEqual(dag.graph["attr"]["bps_wms_config_path"], "dagman.conf")

    def testConfigureIfNotPrepared(self):
        """Test if the method raises when prepare step was skipped."""
        dag = HTCDag(name="test_configure_not_prepared")
        configurator = DagmanConfigurator(self.config)
        with self.assertRaisesRegex(RuntimeError, "file does not exist"):
            configurator.configure(dag)
