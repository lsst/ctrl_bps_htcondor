lsst-ctrl-bps-htcondor v27.0.0 (2024-06-04)
===========================================

New Features
------------

- Updated the open-source license to allow for the code to be distributed with either GPLv3 or BSD 3-clause license. (`DM-37231 <https://rubinobs.atlassian.net/browse/DM-37231>`_)
- Made the plugin properly handle new node status ``FUTILE`` that represents a node that will never run due to the failure of a node that the ``FUTILE`` node depends on either directly or indirectly through a chain of ``PARENT`` / ``CHILD`` relationships. (`DM-38627 <https://rubinobs.atlassian.net/browse/DM-38627>`_)
- Made ``bps restart`` accept other types of run IDs beside the submit directory. (`DM-41561 <https://rubinobs.atlassian.net/browse/DM-41561>`_)
- Added plugin support for reporting error exit codes with ``bps report``. (`DM-42127 <https://rubinobs.atlassian.net/browse/DM-42127>`_)


Bug Fixes
---------

- Fixed bug preventing ``bps cancel`` from working. (`DM-40906 <https://rubinobs.atlassian.net/browse/DM-40906>`_)
- Fixed bug preventing ``bps report`` from showing error codes/counts correctly when called with the submit directory as the run id. (`DM-43381 <https://rubinobs.atlassian.net/browse/DM-43381>`_)
- Fixed ``compute_site`` keyword error in submit introduced by `DM-38138  <https://rubinobs.atlassian.net/browse/DM-38138>`_. (`DM-43721 <https://rubinobs.atlassian.net/browse/DM-43721>`_)


Other Changes and Additions
---------------------------

- Handle changes between different version of HTCondor Python API gracefully so deprecation warnings don't pop up when using ``bps report``. (`DM-37020 <https://rubinobs.atlassian.net/browse/DM-37020>`_)
- Replaced function/methods that are being deprecated by the HTCondor team with their preferred equivalents to remove deprecation warnings during executions of BPS commands. (`DM-42759 <https://rubinobs.atlassian.net/browse/DM-42759>`_)


lsst-ctrl-bps-htcondor v26.0.0 (2023-09-25)
===========================================

No significant changes.
This release includes minor code cleanups and reformatting.
It has been verified to work with Python 3.11.


lsst-ctrl-bps-htcondor v25.0.0 (2023-03-02)
===========================================

Other Changes and Additions
---------------------------

- Made the plugin always report on the latest run even if the old run id was provided to ``bps report``. (`DM-35533 <https://rubinobs.atlassian.net/browse/DM-35533>`_)


lsst-ctrl-bps-htcondor v24.0.0 (2022-08-29)
===========================================

New Features
------------

- This package has been extracted from ``lsst_ctrl_bps`` into a standalone package to make it easier to manage development of the HTCondor plugin.
  (`DM-33521 <https://rubinobs.atlassian.net/browse/DM-33521>`_)
- Add support for a new command,  ``bps restart``, that allows one to restart the failed workflow from the point of its failure. It restarts the workflow as it is just retrying failed jobs, no configuration changes are possible at the moment. (`DM-29575 <https://rubinobs.atlassian.net/browse/DM-29575>`_)
- Add support for a new option of ``bps cancel``, ``--global``, which allows the user to interact (cancel or get the report on) with jobs in any HTCondor job queue. (`DM-29614 <https://rubinobs.atlassian.net/browse/DM-29614>`_)
- Add a configurable memory threshold to the memory scaling mechanism. (`DM-32047 <https://rubinobs.atlassian.net/browse/DM-32047>`_)


Bug Fixes
---------

- HTCondor plugin now correctly passes attributes defined in site's 'profile' section to the HTCondor submission files. (`DM-33887 <https://rubinobs.atlassian.net/browse/DM-33887>`_)


Other Changes and Additions
---------------------------

- Make HTCondor treat all jobs exiting with a signal as if they ran out of memory. (`DM-32968 <https://rubinobs.atlassian.net/browse/DM-32968>`_)
- Make HTCondor plugin pass a group and user attribute to any batch systems that require such attributes for accounting purposes. (`DM-33887 <https://rubinobs.atlassian.net/browse/DM-33887>`_)

ctrl_bps v23.0.0 (2021-12-10)
=============================

New Features
------------

* Added BPS htcondor job setting that should put jobs that
  get the signal 7 when exceeding memory on hold.  Held
  message will say: "Job raised a signal 7.  Usually means
  job has gone over memory limit."  Until bps has the
  automatic memory exceeded retries, you can restart these
  the same way as with jobs that htcondor held for exceeding
  memory limits (``condor_qedit`` and ``condor_release``).

- * Add ``numberOfRetries`` option which specifies the maximum number of retries
    allowed for a job.
  * Add ``memoryMultiplier`` option to allow for increasing the memory
    requirements automatically between retries for jobs which exceeded memory
    during their execution. At the moment this option is only supported by
    HTCondor plugin. (`DM-29756 <https://rubinobs.atlassian.net/browse/DM-29756>`_)
- Change HTCondor bps plugin to use HTCondor curl plugin for local job transfers. (`DM-32074 <https://rubinobs.atlassian.net/browse/DM-32074>`_)

Bug Fixes
---------

- * Fix bug in HTCondor plugin for reporting final job status when ``--id <path>``. (`DM-31887 <https://rubinobs.atlassian.net/browse/DM-31887>`_)
- Fix execution butler with HTCondor plugin bug when output collection has period. (`DM-32201 <https://rubinobs.atlassian.net/browse/DM-32201>`_)
- Disable HTCondor auto detection of files to copy back from jobs. (`DM-32220 <https://rubinobs.atlassian.net/browse/DM-32220>`_)
- * Fixed bug when not using lazy commands but using execution butler.
  * Fixed bug in ``htcondor_service.py`` that overwrote message in bps report. (`DM-32241 <https://rubinobs.atlassian.net/browse/DM-32241>`_)
- * Fixed bug when a pipetask process killed by a signal on the edge node did not expose the failing status. (`DM-32435 <https://rubinobs.atlassian.net/browse/DM-32435>`_)
