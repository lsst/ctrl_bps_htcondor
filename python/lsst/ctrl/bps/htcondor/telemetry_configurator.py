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

"""Module enabling configuring telemetry via submit YAML."""

from __future__ import annotations

__all__ = ["TelemetryConfigurator"]

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lsst.ctrl.bps import BpsConfig

    from .lssthtc import HTCDag, HTCJob


_LOG = logging.getLogger(__name__)


class TelemetryConfigurator:
    """Class responsible for configuring telemetry.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    search_opts : `dict` [`str`, `~typing.Any`], optional
        Options to use while searching the BPS configuration for values.

    Raises
    ------
    KeyError
        Raised if the BPS configuration has no ``telemetry`` section.
    ValueError
        Raised if the configuration for the telemetry application
        (``telemetryConfig``) is provided, but the name of the file it should
        be written to (``telemetryConfigPath``) is either missing or is not
        a relative path.

    Notes
    -----
    The configuration of the telemetry application is optional. It is treated
    as a string, i.e., no assumptions are made about its format.

    BPS variables (``{var}``) and environment variables (``${VAR}``) in
    ``telemetryConfig`` and ``telemetryConfigPath`` are expanded when the
    workflow is submitted, i.e., on the submit host and before the jobs run.
    BPS variables that cannot be resolved are replaced by empty strings.
    Hence, the configuration must not contain the placeholders meant to be
    expanded by the telemetry application at run time if their syntax
    coincides with any of the above.
    """

    def __init__(self, config: BpsConfig, search_opts: dict[str, Any] | None = None) -> None:
        self._settings: BpsConfig = config.get(".telemetry", None)
        if not self._settings:
            raise KeyError("telemetry section not found")
        opts: dict[str, Any] = dict(search_opts) if search_opts else {}
        opts |= {
            "expandEnvVars": True,
            "replaceVars": True,
            "searchobj": self._settings,
        }

        self._bps_config = config
        self.config: str | None = None
        self.config_file: Path | None = None
        found, text = config.search("telemetryConfig", opts)
        if found and text:
            self.config = text
            found, filename = config.search("telemetryConfigPath", opts)
            if not found or not filename:
                raise ValueError("'telemetryConfigPath' must be set when 'telemetryConfig' is provided")
            self.config_file = Path(filename)
            if self.config_file.is_absolute():
                raise ValueError(f"'telemetryConfigPath' must be a relative path, got '{self.config_file}'")

        self.is_prepared = False

    def prepare(self, prefix: Path | str | None = None) -> None:
        """Prepare the BPS configuration and the telemetry application config.

        Parameters
        ----------
        prefix : `pathlib.Path` | `str`, optional
            Directory in which to write the configuration file (typically, the
            submit directory). If not provided, the file will be written
            relative to the current directory.

        Raises
        ------
        OSError
            Raised if the configuration file cannot be created.

        Notes
        -----
        The settings from the ``telemetry`` section (except
        ``telemetryConfig``) are copied to the top level of the BPS
        configuration so the placeholders in ``payloadCommand`` (e.g.,
        ``{telemetrySetup}``, ``{telemetryTeardown}``) can be resolved.
        As top-level values have the lowest priority in BPS searches, they can
        still be overridden in other sections (e.g., ``pipetask.<label>``).

        This method must be called *before* the HTCondor workflow is created
        from the generic workflow as that is when ``payloadCommand`` gets
        resolved.

        If the telemetry application configuration was provided, it is
        written to a file.
        """
        # Copy the raw values. Do NOT use self._settings.items(): it goes
        # through BpsConfig.__getitem__ which would replace BPS variables and
        # expand environment variables now, using only the telemetry section
        # as the context (e.g., {jobLogDir} would silently become ""). The
        # placeholders must survive until payloadCommand is resolved per job.
        for key, value in self._settings.toDict().items():
            if key != "telemetryConfig":
                _LOG.debug("Copying telemetry setting '%s' to the top level of BPS config", key)
                self._bps_config[f".{key}"] = value

        if self.config is not None and self.config_file is not None:
            config_path = Path(prefix) / self.config_file if prefix else self.config_file
            try:
                config_path.parent.mkdir(parents=True, exist_ok=True)
                config_path.write_text(self.config)
            except OSError as exc:
                _LOG.error("Could not write telemetry configuration file '%s': %s", config_path, exc.strerror)
                raise

        self.is_prepared = True

    def configure(self, dag: HTCDag) -> None:
        """Make jobs in the workflow transfer the telemetry configuration file.

        It is a no-op if no telemetry application configuration was provided.

        Parameters
        ----------
        dag : `lsst.ctrl.bps.htcondor.HTCDag`
            HTCondor DAG.

        Raises
        ------
        RuntimeError
            Raised if the prepare step was omitted.

        Notes
        -----
        The configuration file is added to ``transfer_input_files`` of every
        payload job, including the jobs in subdags and the final job. NOOP
        jobs, jobs using package-provided submit files (e.g. group status
        checks), and service jobs are left unchanged.

        The path is relative to the job's directory (i.e., its initial working
        directory set by DAGMan via ``DIR``) to keep the submit directory
        relocatable.
        """
        if not self.is_prepared:
            raise RuntimeError(
                "cannot add telemetry configuration to the workflow "
                f"(hint: run {type(self).__qualname__}.prepare() first)"
            )
        if self.config_file is None:
            _LOG.debug("No telemetry configuration file; skipping updating job input files.")
            return

        # Jobs in subdags have DIR set relative to the subdag directory, but
        # it resolves to the same location (submit_path / job.subdir) as for
        # the jobs in the top-level DAG.
        cache: dict[str, str] = {}
        self._configure_dag(dag, cache)

    def _configure_dag(self, dag: HTCDag, cache: dict[str, str]) -> None:
        """Walk the DAG recursively and update the payload jobs.

        Parameters
        ----------
        dag : `lsst.ctrl.bps.htcondor.HTCDag`
            HTCondor DAG.
        cache : `dict` [`str`, `str`]
            Mapping between job directories and the path of the configuration
            file relative to them.
        """
        for _, job in dag.nodes(data="data"):
            if job.subdag is not None:
                self._configure_dag(job.subdag, cache)
            else:
                self._configure_job(job, cache)
        final_job = dag.graph.get("final_job")
        if final_job is not None:
            self._configure_job(final_job, cache)

    def _configure_job(self, job: HTCJob, cache: dict[str, str]) -> None:
        """Add the configuration file to the job's input files.

        Parameters
        ----------
        job : `lsst.ctrl.bps.htcondor.HTCJob`
            HTCondor job.
        cache : `dict` [`str`, `str`]
            Mapping between job directories and the path of the configuration
            file relative to them.
        """
        if job.subdir is None or job.dagcmds.get("noop", False):
            return
        subdir = str(job.subdir)
        try:
            path = cache[subdir]
        except KeyError:
            # For a static type checker; configure() only gets here if the file
            # was written.
            assert self.config_file is not None
            path = cache[subdir] = str(self.config_file.relative_to(subdir, walk_up=True))
        inputs = job.cmds.get("transfer_input_files")
        if not inputs:
            job.cmds["transfer_input_files"] = path
        elif path not in inputs.split(","):
            job.cmds["transfer_input_files"] = f"{inputs},{path}"
