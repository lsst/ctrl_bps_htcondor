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


"""Unit tests for TelemetryConfigurator class."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.htcondor import HTCDag, HTCJob
from lsst.ctrl.bps.htcondor.telemetry_configurator import TelemetryConfigurator

TELEMETRY_CONFIG = """\
pipeline:
  outputs:
    - name: opensearch
      host: ${TLM_TEST_HOST}
      index: {operator}
      tag: {notAVar}
"""

# BPS variables and environment variables are expanded at submission time,
# undefined BPS variables are replaced by an empty string.
EXPECTED_CONFIG = """\
pipeline:
  outputs:
    - name: opensearch
      host: localhost
      index: jdoe
      tag: \n"""


def _make_job(name: str, subdir: str | None = None, **dagcmds) -> HTCJob:
    job = HTCJob(name, label=name)
    if subdir is not None:
        job.subdir = Path(subdir)
        job.add_dag_cmds({"dir": subdir})
    job.add_dag_cmds(dagcmds)
    return job


class TelemetryConfiguratorTestCase(unittest.TestCase):
    """Unit tests for TelemetryConfigurator class."""

    def setUp(self):
        patcher = mock.patch.dict(os.environ, {"TLM_TEST_HOST": "localhost"})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.config = BpsConfig(
            {
                "operator": "jdoe",
                "telemetry": {
                    "telemetryConfig": TELEMETRY_CONFIG,
                    "telemetryConfigPath": "telemetry.yaml",
                },
            }
        )
        self.tmpdir = tempfile.TemporaryDirectory()
        self.prefix = Path(self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def testInit(self):
        """Test that variables in the configuration are expanded."""
        configurator = TelemetryConfigurator(self.config)
        self.assertEqual(configurator.config, EXPECTED_CONFIG)
        self.assertEqual(configurator.config_file, Path("telemetry.yaml"))
        self.assertFalse(configurator.is_prepared)

    def testInitNoTelemetry(self):
        """Test that KeyError is raised if there is no telemetry section."""
        with self.assertRaises(KeyError):
            TelemetryConfigurator(BpsConfig({"telemetryConfig": "foo"}))

    def testInitNoAppConfig(self):
        """Test that the telemetry app configuration is optional."""
        for telemetry in ({"telemetrySetup": "foo"}, {"telemetryConfig": ""}):
            with self.subTest(telemetry=telemetry):
                configurator = TelemetryConfigurator(BpsConfig({"telemetry": telemetry}))
                self.assertIsNone(configurator.config)
                self.assertIsNone(configurator.config_file)

    def testInitBadConfigPath(self):
        """Test that ValueError is raised for a missing/absolute file name."""
        for path in (None, "/abs/telemetry.yaml"):
            with self.subTest(path=path):
                config = BpsConfig({"telemetry": {"telemetryConfig": "foo"}})
                if path is not None:
                    config[".telemetry.telemetryConfigPath"] = path
                with self.assertRaises(ValueError):
                    TelemetryConfigurator(config)

    def testPrepare(self):
        """Test writing the configuration file."""
        configurator = TelemetryConfigurator(self.config)
        configurator.prepare(prefix=self.prefix)
        self.assertTrue(configurator.is_prepared)
        self.assertEqual((self.prefix / "telemetry.yaml").read_text(), EXPECTED_CONFIG)

    def testPrepareFailure(self):
        """Test that the error is raised if the file cannot be written."""
        configurator = TelemetryConfigurator(self.config)
        os.chmod(self.prefix, 0o500)
        try:
            with self.assertRaises(OSError):
                configurator.prepare(prefix=self.prefix / "sub")
        finally:
            os.chmod(self.prefix, 0o700)
        self.assertFalse(configurator.is_prepared)

    def testPrepareCopiesSettings(self):
        """Test that settings are copied so payloadCommand can use them."""
        config = BpsConfig(
            {
                "telemetrySetup": "",
                "payloadCommand": "{telemetrySetup} run; {telemetryTeardown}",
                "pipetask": {"label2": {"telemetrySetup": ""}},
                "telemetry": {
                    "telemetryConfig": "foo",
                    "telemetryConfigPath": "tlm.conf",
                    "telemetrySetup": "start -c {telemetryConfigPath}",
                    "telemetryTeardown": "stop",
                },
            }
        )
        configurator = TelemetryConfigurator(config)
        configurator.prepare(prefix=self.prefix)
        self.assertNotIn("telemetryConfig", config)
        self.assertEqual(config.search("payloadCommand")[1], "start -c tlm.conf run; stop")
        # Per-label settings still take precedence.
        _, value = config.search("payloadCommand", opt={"curvals": {"curr_pipetask": "label2"}})
        self.assertEqual(value, " run; stop")
        self.assertEqual((self.prefix / "tlm.conf").read_text(), "foo")

    def testPrepareNoAppConfig(self):
        """Test that no file is written without the app configuration."""
        config = BpsConfig({"telemetry": {"telemetrySetup": "foo"}})
        configurator = TelemetryConfigurator(config)
        configurator.prepare(prefix=self.prefix)
        self.assertEqual(config["telemetrySetup"], "foo")
        self.assertEqual(list(self.prefix.iterdir()), [])

        dag = HTCDag(name="dag")
        job = _make_job("job", "jobs/label")
        dag.add_job(job)
        configurator.configure(dag)
        self.assertNotIn("transfer_input_files", job.cmds)

    def testConfigureWithoutPrepare(self):
        """Test that the configure step requires the prepare step."""
        configurator = TelemetryConfigurator(self.config)
        with self.assertRaises(RuntimeError):
            configurator.configure(HTCDag(name="dag"))

    def testConfigure(self):
        """Test adding the configuration file to the payload jobs only."""
        dag = HTCDag(name="dag")

        job1 = _make_job("job1", "jobs/label1/1")
        dag.add_job(job1)
        job2 = _make_job("job2", "jobs/label2")
        job2.add_job_cmds({"transfer_input_files": "/abs/input.txt"})
        dag.add_job(job2)
        noop = _make_job("noop", noop=True)
        dag.add_job(noop)
        check = _make_job("check", dir="subdags/wms_group")
        dag.add_job(check)

        subdag = HTCDag(name="group")
        job3 = _make_job("job3", "jobs/label3/a/b")
        subdag.add_job(job3)
        group = _make_job("wms_group", dir="subdags/wms_group")
        group.subdag = subdag
        dag.add_job(group)

        final = _make_job("final", "jobs/final")
        dag.add_final_job(final)

        configurator = TelemetryConfigurator(self.config)
        configurator.prepare(prefix=self.prefix)
        configurator.configure(dag)
        # Running it again must not add duplicates.
        configurator.configure(dag)

        self.assertEqual(job1.cmds["transfer_input_files"], "../../../telemetry.yaml")
        self.assertEqual(job2.cmds["transfer_input_files"], "/abs/input.txt,../../telemetry.yaml")
        self.assertEqual(job3.cmds["transfer_input_files"], "../../../../telemetry.yaml")
        self.assertEqual(final.cmds["transfer_input_files"], "../../telemetry.yaml")
        for job in (noop, check, group):
            self.assertNotIn("transfer_input_files", job.cmds)


if __name__ == "__main__":
    unittest.main()
