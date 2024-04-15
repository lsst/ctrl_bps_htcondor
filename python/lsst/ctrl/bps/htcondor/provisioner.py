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
        self.search_opts: dict[str, Any] = search_opts if search_opts is not None else {}
        self.script_name: Path | None = None
        self.script_file: Path | None = None

    def configure(self, name: Path | str, prefix: Path | str | None = None, overwrite: bool = False) -> None:
        """Create a configuration file for the provisioning script.

        If the configuration file with the specified name already exists in
        the given directory, it will be used instead unless ``overwrite`` is
        ``True``.

        Parameters
        ----------
        name : `pathlib.Path` | `str`
            Name of the file where to output the configuration for provisioning
            script.
        prefix : `pathlib.Path` | `str`, optional
            Directory to which output the configuration file. If not provided,
            the current working directory will be used.
        overwrite : `bool`
            If ``True``, the new provisioning configuration file will be
            created based on the template defined by
            ``provisioningScriptConfig`` setting in the BPS configuration.

            The previous configruation will be saved at the same directory,
            under the same name with appended ``.bak`` suffix.
        """
        prefix = Path.cwd() if prefix is None else Path(prefix)
        prefix.mkdir(parents=True, exist_ok=True)
        config_file = prefix / name
        if config_file.is_file():
            if overwrite:
                target = config_file.parent / (config_file.name + ".bak")
                _LOG.info("Saving a copy of the existing provisioning configuration to %s", target)
                config_file.rename(target)
                create_config = True
            else:
                _LOG.info("Using existing provisioning configuration from %s.", config_file)
                create_config = False
        else:
            _LOG.warning(
                "Configuration file %s required for provisioning not found. "
                "Creating a new one using the template defined by 'provisioningScriptConfig'",
                config_file,
            )
            create_config = True

        if create_config:
            _LOG.info("Saving provisioning configuration to %s", config_file)
            search_opts = self.search_opts | {"expandEnvVars": True, "required": True}
            _, script_content = self.config.search("provisioningScriptConfig", opt=search_opts)
            config_file.write_text(script_content)

    def prepare(self, name: Path | str, prefix: Path | str = None) -> None:
        """Create the script responsible for the provisioning resources.

        The script is created based on the template defined by
        the ``provisioningScript`` setting in the BPS configuration.

        Parameters
        ----------
        name : `pathlib.Path` | `str`
            Name of the template file to use for creating the provisioning
            script.
        prefix : `pathlib.Path` | `str`, optional
            Directory in which to output the provisioning script.
        """
        self.script_name = Path(name)
        self.script_file = Path(prefix) / self.script_name if prefix else self.script_name

        search_opts = self.search_opts | {"expandEnvVars": False, "required": True}
        _, script_content = self.config.search("provisioningScript", opt=search_opts)

        _LOG.debug("Writing provisioning script to %s", self.script_file)
        with open(self.script_file, mode="w", encoding="utf8") as handle:
            handle.write(script_content)
        self.script_file.chmod(0o755)

    def provision(self, dag: HTCDag, name: str | None = None) -> None:
        """Add the provisioning job to the HTCondor workflow.

        Parameters
        ----------
        dag : `lsst.ctrl.bps.htcondor.HTCDag`
            HTCondor DAG.
        name : `str`, optional
            Name of the HTCJob to create. If not provided, defaults to
            ``provisioningJob``.
        """
        if name is None:
            name = "provisioningJob"
        job = HTCJob(name=name, label=name)
        job.subfile = Path("jobs") / job.label / f"{name}.sub"
        job_cmds = {
            "universe": "local",
            "executable": f"{self.script_name}",
            "should_transfer_files": "NO",
            "getenv": "True",
            "output": f"{job.subfile}.$(Cluster).out",
            "error": f"{job.subfile}.$(Cluster).err",
            "log": f"{job.subfile}.$(Cluster).log",
        }
        job.add_job_attrs({"bps_job_name": job.name, "bps_job_label": job.label, "bps_job_quanta": ""})
        job.add_job_cmds(job_cmds)
        dag.add_service_job(job)

        bps_job_summary = dag.graph["attrs"]["bps_job_summary"] + ";provisioningJob:1"
        dag.add_attribs({"bps_job_summary": bps_job_summary})
