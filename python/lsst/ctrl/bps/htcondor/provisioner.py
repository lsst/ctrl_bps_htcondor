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

"""Module enabling provisioning resources during workflow execution."""

__all__ = ["Provisioner"]

import logging
from pathlib import Path
from typing import Any

from lsst.ctrl.bps import BpsConfig

from .lssthtc import HTCDag, HTCJob

_LOG = logging.getLogger(__name__)


class Provisioner:
    """Class responsible for enabling provisioning necessary resources.

    Parameters
    ----------
    config : `lsst.ctr.bps.BpsConfig`
        BPS configuration.
    search_opts : `dict` [`str`, `object`], optional
        Options to use while searching the BPS configuration for values.
    """

    def __init__(self, config: BpsConfig, search_opts: dict[str, Any] | None = None) -> None:
        self.config: BpsConfig = config
        self.search_opts: dict[str, Any] = {
            "expandVars": True,
            "searchobj": self.config[".provisioning"],
            "required": True,
        }
        if search_opts is not None:
            self.search_opts |= search_opts
        self.script_name: Path | None = None
        self.script_file: Path | None = None
        self.is_configured: bool = False
        self.is_prepared: bool = False

    def configure(self) -> None:
        """Create the configuration file for the provisioning script.

        The content of the configuration file for the provisioning script
        must be specified by ``provisioningScriptConfig`` setting in the BPS
        config and its location by ``provisioningScriptConfigPath``,
        respectively.

        The method is effectively a no-op if *any* of the conditions below
        is met:

        1. the value of the ``provisioningScriptConfig`` is an empty string
           (in such a case the class assumes that the provisioning script
           does not require any configuration file),
        2. the file specified by ``provisioningScriptConfigPath`` already
           exists.

        Raises
        ------
        OSError
            Raised if the configuration file cannot be created.
        """
        search_opts = self.search_opts | {"expandEnvVars": True}

        _, script_config_path = self.config.search("provisioningScriptConfigPath", opt=search_opts)
        script_config_path = Path(script_config_path)
        if script_config_path.is_file():
            _LOG.info(
                "Configuration file for the provisioning script '%s' already exists; "
                "no further actions required",
                script_config_path,
            )
        else:
            _, script_config_content = self.config.search("provisioningScriptConfig", opt=search_opts)
            if script_config_content:
                _LOG.info(
                    "Configuration file for provisioning script '%s' not found; "
                    "creating a new one using the content specified by 'provisioningScriptConfig' setting",
                    script_config_path,
                )

                # If necessary, create directory that will hold the script's
                # configuration file.
                prefix = script_config_path.parent
                try:
                    prefix.mkdir(parents=True, exist_ok=True)
                except OSError as exc:
                    _LOG.error(
                        "Cannot create configuration file for the provisioning script '%s': %s",
                        script_config_path,
                        exc.strerror,
                    )
                    raise

                script_config_path.write_text(script_config_content)
            else:
                _LOG.info(
                    "Configuration for the provisioning script not provided; "
                    "assuming the provisioning script requires no configuration file"
                )
        self.is_configured = True

    def prepare(self, filename: Path | str, prefix: Path | str = None) -> None:
        """Create the script responsible for provisioning resources.

        The script is created based on the content defined by
        the ``provisioningScript`` setting in the BPS configuration.

        Parameters
        ----------
        filename : `pathlib.Path` | `str`
            Name of the file to use when creating the provisioning script.
        prefix : `pathlib.Path` | `str`, optional
            Directory in which to output the provisioning script. If not
            provided, the script will be written to the current directory.

        Raises
        ------
        RuntimeError
            Raised if the configuration step was omitted.
        """
        if not self.is_configured:
            raise RuntimeError(
                f"Cannot create provisioning script: run {type(self).__qualname__}.configure() first"
            )

        self.script_name = Path(filename)
        self.script_file = Path(prefix) / self.script_name if prefix else self.script_name

        search_opts = self.search_opts | {"expandEnvVars": False}
        _, script_content = self.config.search("provisioningScript", opt=search_opts)

        _LOG.debug("Writing provisioning script to %s", self.script_file)
        with open(self.script_file, mode="w", encoding="utf8") as file:
            file.write(script_content)
        self.script_file.chmod(0o755)

        self.is_prepared = True

    def provision(self, dag: HTCDag) -> None:
        """Add the provisioning job to the HTCondor workflow.

        Parameters
        ----------
        dag : `lsst.ctrl.bps.htcondor.HTCDag`
            HTCondor DAG.

        Raises
        ------
        RuntimeError
            Raised if the prepare step was omitted.
        """
        if not self.is_prepared:
            raise RuntimeError(
                f"Cannot add provisioning job to the workflow: "
                f"run {type(self).__qualname__}.prepare() to create it"
            )

        name = self.script_name.stem
        job = HTCJob(name=name, label=name)
        job.subfile = Path("jobs") / job.label / f"{name}.sub"
        job.add_job_attrs({"bps_job_name": job.name, "bps_job_label": job.label, "bps_job_quanta": ""})
        cmds = {
            "universe": "local",
            "executable": f"{self.script_name}",
            "should_transfer_files": "NO",
            "getenv": "True",
        }
        cmds |= {
            stream: str(job.subfile.with_suffix(f".$(Cluster).{stream[:3]}"))
            for stream in ("output", "error", "log")
        }
        job.add_job_cmds(cmds)

        dag.add_service_job(job)
        dag.add_attribs({"bps_provisioning_job": job.label})
