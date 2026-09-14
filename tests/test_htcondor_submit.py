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

"""Unit tests for the HTCondor Submit compatibility shim."""

import importlib.metadata
from unittest.mock import patch

import pytest
from classad2 import ClassAd
from htcondor2 import Submit
from htcondor2._schedd import Schedd
from packaging.version import Version

htcondor_pkg_version = Version(importlib.metadata.version("htcondor"))


@pytest.fixture
def mock_schedd_ad() -> ClassAd:
    """Return a mock ClassAd for a Schedd."""
    schedd = ClassAd()
    schedd["MyAddress"] = "<127.0.0.1:2573?addrs=127.0.0.1-2573&alias=schedd.local>"
    schedd["CondorVersion"] = (
        "$CondorVersion: 24.0.23 2026-08-20 BuildID: 943578 PackageID: 24.0.23-1 GitSHA: e1114ad7 $"
    )
    return schedd


@pytest.mark.skipif(
    htcondor_pkg_version >= Version("24.1"), reason="Test only applicable for htcondor < 24.1"
)
def test_htcondor_submit(mock_schedd_ad: ClassAd):
    """Tests the ``htcondor2`` Schedd submit behavior wrt the ``queue_args``.

    Parameters
    ----------
    mock_schedd_ad : ``classad2.ClassAd``
        A minimal mock ad for a Schedd location.

    Notes
    -----
    In ``htcondor==24.0.*``, the ``htcondor2.Schedd.submit`` method is broken
    and raises an ``IndexError`` if no "queue arguments" are provided. A work-
    around is to specify explicit "queue arguments" on the ``Submit`` ad.
    """
    submit_dict = {"executable": "/bin/sleep", "arguments": "10"}
    submit_ad = Submit(submit_dict)

    with patch("htcondor2._schedd._schedd_submit", return_value=None) as mock_submit:
        schedd = Schedd(mock_schedd_ad)

        with pytest.raises(IndexError, match=r"string index out of range"):
            schedd.submit(submit_ad)

        submit_ad.setQArgs("queue 1")
        schedd.submit(submit_ad, itemdata=None)

        assert mock_submit.called, "the patch missed its mark"
