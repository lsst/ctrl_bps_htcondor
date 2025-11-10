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

"""Module enabling configuring DAGMan via submit YAML."""

__all__ = ["DagmanConfigurator"]

import logging
import os
from pathlib import Path
from typing import Any

import htcondor
from pydantic import AliasGenerator, ConfigDict, create_model

from lsst.ctrl.bps import BpsConfig

from .lssthtc import HTCDag

_LOG = logging.getLogger(__name__)

# Extract DAGMan configuration options with their types and default values from
# the local HTCondor configuration.
#
# Notes
# -----
# There are some DAGMan configuration options that names do not start with
# ``DAGMAN_`` (e.g., ``MAX_DAGMAN_LOG``). Hence, do not use
# ``key.startswith("DAGMAN_")``.
_fields = {key.lower(): (type(val), val) for key, val in htcondor.param.items() if "DAGMAN_" in key}

# Add some valid configuration options are not set by default by HTCondor and
# are missing from ``htcondor.param``.
#
# Notes
# -----
# A complete list of configuration options HTCondor supports can be found in
# ``src/condor_utils/param_info.in`` in
# `HTCondor GitHub repository <https://github.com/htcondor/htcondor>`_.
_fields.update(
    {
        "dagman_debug": (str, ""),
        "dagman_node_record_info": (str, ""),
        "dagman_record_machine_attrs": (str, ""),
    }
)

# Dynamically create a Pydantic model encapsulating the DAGMan configuration
# options gathered above.
_DagmanOptions = create_model(
    "DagmanOptions",
    __config__=ConfigDict(
        alias_generator=AliasGenerator(
            serialization_alias=lambda name: name.upper(),
        ),
        extra="allow",
        serialize_by_alias=True,
    ),
    **_fields,
)


class DagmanConfigurator:
    """Class responsible for setting WMS-specific configuration options.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    search_opts : `dict` [`str`, `Any`], optional
        Options to use while searching the BPS configuration for values.

    Raises
    ------
    KeyError
        Raised if DAGMan configuration is missing from the BPS configuration.
    """

    def __init__(self, config: BpsConfig, search_opts: dict[str, Any] | None = None) -> None:
        if search_opts is None:
            search_opts = {}
        _, site = config.search("computeSite", search_opts)
        if site:
            search_opts["curvals"] = {"curr_site": site}
        _, wms_config = config.search("wmsConfig", search_opts)
        if not wms_config:
            raise KeyError("WMS-specific configuration not found")
        self._options = _DagmanOptions.model_validate({key.lower(): val for key, val in wms_config.items()})
        if self._options.model_extra:
            unknown_opts = [key.upper() for key in self._options.model_extra]
            _LOG.warning(
                "The following WMS-specific config options were not recognized and will be ignored: %s.",
                ", ".join(unknown_opts),
            )
        self.config_path: Path | None = None
        self.prefix: Path | None = None

    @property
    def options(self) -> dict[str, Any]:
        """DAGMan configuration options set via BPS (`dict` [`str`, `Any`])."""
        return {
            key: val
            for key, val in self._options.model_dump(exclude_unset=True).items()
            if key not in self._options.model_extra
        }

    def prepare(self, filename: os.PathLike | str, prefix: os.PathLike | str | None) -> None:
        """Write WMS-specific configuration to a file.

        Parameters
        ----------
        filename : `str`, optional
            Name of the file to use when creating the DAG configuration.
        prefix : `pathlib.Path` | `str`, optional
            Directory in which to output the DAG configuration file. If not
            provided, the script will be written to the current directory.

        Raises
        ------
        OSError
            Raised if the configuration file cannot be created.
        """
        if prefix:
            self.prefix = Path(prefix)
        self.config_path = self.prefix / filename if self.prefix else Path(filename)
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _LOG.error(
                "Could not write WMS-specific configuration file '%s': %s",
                self.config_path,
                exc.strerror,
            )
            raise

        # Populate the DAG configuration file only with options that were
        # explicitly set in the BPS configuration.
        #
        # Notes
        # -----
        # The Pydantic model we are using to represent the DAGMan configuration
        # options allows for extra fields. However, it seems that
        # BaseModel.model_dump() does not support excluding these fields during
        # serialization at the moment (Pydantic ver. 2.12), so we have to do it
        # manually.
        self.config_path.write_text("\n".join(f"{key} = {val}" for key, val in self.options.items()))

    def configure(self, dag: HTCDag) -> None:
        """Add DAG configuration file to the workflow.

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
        The path to the DAG configuration is added as a DAG attribute named
        ``bps_wms_config_path``. The stored path is relative to the prefix.
        """
        if self.config_path is None:
            raise RuntimeError(
                f"cannot add WMS-specific configuration to the workflow: file does not exist. "
                f"(hint: run {type(self).__qualname__}.prepare() to create it)"
            )
        config_path = self.config_path.relative_to(self.prefix) if self.prefix else self.config_path
        dag.add_attribs({"bps_wms_config_path": str(config_path)})
