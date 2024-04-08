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

"""Definitions of handlers of HTCondor job ClassAds."""

__all__ = [
    "HTC_JOB_AD_HANDLERS",
    "Chain",
    "Handler",
    "JobCompletedWithExecTicketHandler",
    "JobCompletedWithoutExecTicketHandler",
    "JobHeldByOtherHandler",
    "JobHeldBySignalHandler",
    "JobHeldByUserHandler",
]


import abc
import logging
import re
from collections.abc import Sequence
from typing import Any

_LOG = logging.getLogger(__name__)


class Handler(abc.ABC):
    """Abstract base class defining Handler interface."""

    @abc.abstractmethod
    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        """Handle a ClassAd.

        Parameters
        ----------
        ad : `dict[`str`, Any]`
            The dictionary representing ClassAd that need to be processed.

        Returns
        -------
        ad : `dict[`str`, Any]` | None
            The dictionary representing ClassAd after processing and ``None``
            if the handler was not able to process the ad.

        Notes
        -----
        To optimize the memory usage, the implementation of this method may
        modify the ClassAd in place. In such a case, the ClassAd returned by
        the method will be the same object that was passed to it as
        the argument, but including any modifications that were made.
        """


class Chain(Sequence):
    """Class defining chaining of handlers.

    Parameters
    ----------
    handlers : `Sequence` [`Handler`]
        List of handlers that will be used to initialize the chain.
    """

    def __init__(self, handlers: Sequence[Handler] = None) -> None:
        self._handlers = []
        if handlers is not None:
            for handler in handlers:
                self.append(handler)

    def __getitem__(self, index: int) -> Handler:
        return self._handlers[index]

    def __len__(self) -> int:
        return len(self._handlers)

    def append(self, handler: Handler) -> None:
        """Append a handler to the chain.

        Parameters
        ----------
        handler : `Handler`
            The handler that needs to be added to the chain.

        Raises
        ------
        TypeError
            Raised if the passed object in not a ``Handler``.
        """
        if not isinstance(handler, Handler):
            raise TypeError(f"append() argument must be a 'Handler', not a '{type(handler)}'")
        self._handlers.append(handler)

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        """Handle a ClassAd.

        Parameters
        ----------
        ad : `dict[`str`, Any]`
            The dictionary representing a ClassAd that need to be handled.

        Returns
        -------
        ad : `dict[`str`, Any]`
            A modified ClassAd if any handler in the chain was able to
            process the ad, None otherwise.
        """
        new_ad = None
        for handler in self:
            try:
                new_ad = handler.handle(ad)
            except Exception as e:
                _LOG.warning(
                    "Handler '%s' raised an exception while processing the ad: %s. "
                    "Proceeding to the next handler (if any).",
                    type(handler).__name__,
                    repr(e),
                )
            else:
                if new_ad is not None:
                    break
        return new_ad


class JobCompletedWithExecTicketHandler(Handler):
    """Handler of ClassAds for completed jobs with the ticket of execution.

    Usually, the entry in the event log for a completed job contains the ticket
    of execution -- a record describing how and when the job was terminated.
    If it exists, this handler will use it to add the attributes describing
    job's exit status.
    """

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        if not ad["MyType"].endswith("TerminatedEvent"):
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': job not completed",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        if "ToE" in ad:
            toe = ad["ToE"]
            ad["ExitBySignal"] = toe["ExitBySignal"]
            if ad["ExitBySignal"]:
                ad["ExitSignal"] = toe["ExitSignal"]
            else:
                ad["ExitCode"] = toe["ExitCode"]
        else:
            _LOG.debug(
                "%s: refusing to process the ad for the job '%s.%s': ticket of execution missing",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        return ad


class JobCompletedWithoutExecTicketHandler(Handler):
    """Handler of ClassAds for completed jobs w/o the ticket of execution.

    The entry in the event log for some completed jobs (e.g. jobs that run
    ``condor_dagman``) do *not* contain the ticket of execution -- a record
    describing how and when the job was terminated.  This handler will try
    to use other existing attributes to add the ones describing job's exit
    status.
    """

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        if not ad["MyType"].endswith("TerminatedEvent"):
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': job not completed",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        if "ToE" not in ad:
            ad["ExitBySignal"] = not ad["TerminatedNormally"]
            if ad["ExitBySignal"]:
                ad["ExitSignal"] = ad["TerminatedBySignal"]
            else:
                ad["ExitCode"] = ad["ReturnValue"]
        else:
            _LOG.debug(
                "%s: refusing to process the ad for the job '%s.%s': ticket of execution found",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        return ad


class JobHeldByOtherHandler(Handler):
    """Handler of ClassAds for jobs put on hold."""

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        if not ad["MyType"].endswith("HeldEvent"):
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': job not held",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        if ad["HoldReasonCode"] not in {1, 3}:
            ad["ExitBySignal"] = False
            ad["ExitCode"] = ad["HoldReasonCode"]
        else:
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': "
                "invalid hold reason code: HoldReasonCode = %s",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
                ad["HoldReasonCode"],
            )
            return None
        return ad


class JobHeldBySignalHandler(Handler):
    """Handler of ClassAds for jobs put on hold by signals."""

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        if not ad["MyType"].endswith("HeldEvent"):
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': job not held",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        if ad["HoldReasonCode"] == 3:
            match = re.search(r"signal (\d+)", ad["HoldReason"])
            if match is not None:
                ad["ExitBySignal"] = True
                ad["ExitSignal"] = match.group(1)
            else:
                _LOG.debug(
                    "Handler '%s': refusing to process the ad for the job '%s.%s': "
                    "signal not found: HoldReason = %s",
                    self.__class__.__name__,
                    ad["ClusterId"],
                    ad["ProcId"],
                    ad["HoldReason"],
                )
                return None
        else:
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': "
                "job not held by a signal: HoldReasonCode = %s, HoldReason = %s",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
                ad["HoldReasonCode"],
                ad["HoldReason"],
            )
            return None
        return ad


class JobHeldByUserHandler(Handler):
    """Handler of ClassAds for jobs put on hold by the user."""

    def handle(self, ad: dict[str, Any]) -> dict[str, Any] | None:
        if not ad["MyType"].endswith("HeldEvent"):
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': job not held",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
            )
            return None
        if ad["HoldReasonCode"] == 1:
            ad["ExitBySignal"] = False
            ad["ExitCode"] = 0
        else:
            _LOG.debug(
                "Handler '%s': refusing to process the ad for the job '%s.%s': "
                "job not held by the user: HoldReasonCode = %s, HoldReason = %s",
                self.__class__.__name__,
                ad["ClusterId"],
                ad["ProcId"],
                ad["HoldReasonCode"],
                ad["HoldReason"],
            )
            return None
        return ad


_handlers = [
    JobHeldByUserHandler(),
    JobHeldBySignalHandler(),
    JobHeldByOtherHandler(),
    JobCompletedWithExecTicketHandler(),
    JobCompletedWithoutExecTicketHandler(),
]
HTC_JOB_AD_HANDLERS = Chain(handlers=_handlers)
