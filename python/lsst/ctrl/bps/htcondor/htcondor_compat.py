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

"""Private module managing ``htcondor`` imports."""

from __future__ import annotations

import sys
from collections.abc import Callable
from importlib.metadata import version
from typing import TYPE_CHECKING, cast

from htcondor2 import HTCondorException as _HTCondorException
from packaging.version import Version

from lsst.utils import doImport

HTC_VERSION = Version(version("htcondor"))

HTCondorException = cast(type[Exception], _HTCondorException)

if TYPE_CHECKING:
    from classad2 import ClassAd


def ping(ad: ClassAd) -> None:
    """Perform a version-agnostic HTCondor ping against the specified location
    ad.

    Parameters
    ----------
    ad : ``classad2.ClassAd``
        A location ``ClassAd`` to ping, usually a ``Collector`` or ``Schedd``.

    Note
    ----
    The "preview" of ``htcondor2`` in the HTCondor LTS 24.0 release does not
    implement a ``ping`` function. This function is part of the deprecated and
    removed ``SecMan`` API which is only available in ``htcondor``.
    """
    if HTC_VERSION < Version("24.1"):
        SecMan = cast(type, doImport("htcondor.SecMan"))
        HTCondorLocateError = cast(type[Exception], doImport("htcondor.HTCondorLocateError"))
        HTCondorIOError = cast(type[Exception], doImport("htcondor.HTCondorIOError"))
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
        _ping = cast(Callable, doImport("htcondor2.ping"))
        _ping(ad)

    return None
