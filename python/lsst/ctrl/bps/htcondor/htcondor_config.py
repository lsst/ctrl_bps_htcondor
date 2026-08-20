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

"""URI to plugin's default configuration."""

__all__ = ["HTC_DEFAULTS_URI"]

import sys
from collections.abc import Callable
from importlib.metadata import version
from typing import TYPE_CHECKING, Any, cast

from htcondor2 import HTCondorException
from packaging.version import Version

from lsst.resources import ResourcePath
from lsst.utils import doImport

HTC_DEFAULTS_URI = ResourcePath("resource://lsst.ctrl.bps.htcondor/etc/htcondor_defaults.yaml")


def htc_ping(ad: Any) -> Any:
    """Perform a version-agnostic HTCondor ping against the specified location
    ad.

    Parameters
    ----------
    ad : ``classad`` | ``classad2``
        A location ``classad`` to ping, usually a ``Collector`` or ``Schedd``.

    Note
    ----
    The "preview" of ``htcondor2`` in the HTCondor LTS 24.0 release does not
    implement a ``ping`` function. This function is part of the deprecated and
    removed ``SecMan`` API which is only available in ``htcondor``.
    """
    htc_version = Version(version("htcondor"))
    if htc_version < Version("24.1"):
        SecMan = doImport("htcondor.SecMan")
        HTCondorLocateError = doImport("htcondor.HTCondorLocateError")
        HTCondorIOError = doImport("htcondor.HTCondorIOError")
        if TYPE_CHECKING:
            assert isinstance(SecMan, type)
            HTCondorLocateError = cast(type[Exception], HTCondorLocateError)
            HTCondorIOError = cast(type[Exception], HTCondorIOError)
        secman = SecMan()
        try:
            secman.ping(ad["MyAddress"])
        except HTCondorLocateError as e:
            raise HTCondorException("Unable to locate daemon.") from e
        except HTCondorIOError as e:
            raise HTCondorException("Unable to connect to daemon.") from e
        secman = None
        sys.modules.pop("htcondor")
    else:
        ping = cast(Callable, doImport("htcondor2.ping"))
        return ping(ad)
