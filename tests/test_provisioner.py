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
"""Unit tests for Provisioner class."""
import logging
import os
import tempfile
import unittest
from pathlib import Path

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.htcondor import HTCDag
from lsst.ctrl.bps.htcondor.provisioner import Provisioner

logger = logging.getLogger("lsst.ctrl.bps.htcondor")


class ProvisionerTestCase(unittest.TestCase):
    """Unit tests for Provisioner class."""

    def setUp(self):
        self.config = BpsConfig({}, defaults={"wmsServiceClass": "lsst.ctrl.bps.htcondor.HTCondorService"})

    def tearDown(self):
        pass

    def testConfigureWithoutExistingConfig(self):
        """Test if configuration file is created if necessary."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            filename = f"{tmpdir}/condor-info.py"
            self.config[".provisioning.provisioningScriptConfig"] = "foo"
            self.config[".provisioning.provisioningScriptConfigPath"] = filename

            provisioner = Provisioner(self.config)
            with self.assertLogs(level="INFO") as cm:
                provisioner.configure()

            self.assertRegex(cm.output[0], "file.*not found")
            self.assertRegex(cm.output[0], "new one")
            self.assertTrue(Path(filename).is_file())
            self.assertEqual(Path(filename).read_text(), "foo")
            self.assertTrue(provisioner.is_configured)

    def testConfigureWithExistingConfig(self):
        """Test if existing configuration file is left unchanged."""
        with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8") as tmpfile:
            self.config[".provisioning.provisioningScriptConfig"] = "bar"
            self.config[".provisioning.provisioningScriptConfigPath"] = tmpfile.name
            tmpfile.write("foo")
            tmpfile.flush()

            provisioner = Provisioner(self.config)
            with self.assertLogs(level="INFO") as cm:
                provisioner.configure()

            self.assertRegex(cm.output[0], "file.*exists")
            self.assertEqual(Path(tmpfile.name).read_text(), "foo")
            self.assertTrue(provisioner.is_configured)

    def testConfigureNoConfigRequired(self):
        """Test when no configuration file is required."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            filename = f"{tmpdir}/condor-info.py"
            self.config[".provisioning.provisioningScriptConfig"] = ""
            self.config[".provisioning.provisioningScriptConfigPath"] = filename

            provisioner = Provisioner(self.config)
            with self.assertLogs(level="INFO") as cm:
                provisioner.configure()

        self.assertRegex(cm.output[0], "Configuration.*not provided")
        self.assertFalse(Path(filename).is_file())
        self.assertTrue(provisioner.is_configured)

    def testConfigureOsError(self):
        """Test if method raises when the configuration can't be created."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            filename = f"{tmpdir}/subdir/condor-info.py"
            self.config[".provisioning.provisioningScriptConfigPath"] = filename
            os.chmod(tmpdir, 0o500)

            provisioner = Provisioner(self.config)
            with self.assertLogs(logger=logger, level="ERROR") as cm, self.assertRaises(OSError):
                provisioner.configure()

            self.assertRegex(cm.output[0], "Cannot create configuration")

            os.chmod(tmpdir, 0o700)

    def testPrepare(self):
        """Test if provisioning script is created."""
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            prov_config = Path(f"{tmpdir}/condor-info.py")
            self.config[".provisioning.provisioningScriptConfigPath"] = str(prov_config)
            self.config[".provisioning.provisioningScriptConfig"] = "foo"
            self.config[".provisioning.provisioningScript"] = "bar"

            prov_script = Path(tmpdir) / "provisioning_job.sh"
            provisioner = Provisioner(self.config)
            with self.assertLogs(logger=logger, level="DEBUG") as cm:
                provisioner.configure()
                provisioner.prepare(prov_script.name, prefix=prov_script.parent)

            self.assertRegex(cm.output[1], "Writing.*provisioning script")
            self.assertTrue(provisioner.is_configured)
            self.assertTrue(prov_config.is_file())
            self.assertEqual(prov_config.read_text(), "foo")
            self.assertTrue(prov_script.is_file())
            self.assertEqual(prov_script.read_text(), "bar")

    def testPrepareIfNotConfigured(self):
        """Test if method raises when the configuration step was skipped."""
        provisioner = Provisioner(self.config)
        with self.assertRaises(RuntimeError):
            provisioner.prepare("provisioning_job.sh", prefix="")

    def testProvision(self):
        """Test if provisioning job is added to the DAG."""
        script = Path("provisioningJob.sh")
        cmds = {
            "universe": "local",
            "executable": str(script),
            "should_transfer_files": "NO",
            "getenv": "True",
            "output": f"jobs/{script.stem}/{script.stem}.$(Cluster).out",
            "error": f"jobs/{script.stem}/{script.stem}.$(Cluster).err",
            "log": f"jobs/{script.stem}/{script.stem}.$(Cluster).log",
        }
        dag = HTCDag(name="default")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            self.config[".provisioning.provisioningMaxWallTime"] = 3600
            self.config[".provisioning.provisioningScriptConfigPath"] = f"{tmpdir}/condor-info.py"

            provisioner = Provisioner(self.config)
            provisioner.configure()
            provisioner.prepare(script.name, prefix=tmpdir)
            provisioner.provision(dag)

        self.assertIsNotNone(dag.graph["service_job"])
        self.assertEqual(dag.graph["service_job"].name, script.stem)
        self.assertEqual(dag.graph["service_job"].label, script.stem)
        self.assertEqual(dict(dag.graph["service_job"].cmds), cmds)
        self.assertIn("bps_provisioning_job", dag.graph["attr"])

    def testProvisionError(self):
        """Test if method raises when the prepare step was skipped."""
        dag = HTCDag(name="test")
        provisioner = Provisioner(self.config)
        with self.assertRaises(RuntimeError):
            provisioner.provision(dag)


if __name__ == "__main__":
    unittest.main()
