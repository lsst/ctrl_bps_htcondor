lsst-ctrl-bps-htcondor v24.0.0 (2022-08-29)
===========================================

New Features
------------

- This package has been extracted from ``lsst_ctrl_bps`` into a standalone package to make it easier to manage development of the HTCondor plugin.
  (`DM-33521 <https://jira.lsstcorp.org/browse/DM-33521>`_)
- Add support for a new command,  ``bps restart``, that allows one to restart the failed workflow from the point of its failure. It restarts the workflow as it is just retrying failed jobs, no configuration changes are possible at the moment. (`DM-29575 <https://jira.lsstcorp.org/browse/DM-29575>`_)
- Add support for a new option of ``bps cancel``, ``--global``, which allows the user to interact (cancel or get the report on) with jobs in any HTCondor job queue. (`DM-29614 <https://jira.lsstcorp.org/browse/DM-29614>`_)
- Add a configurable memory threshold to the memory scaling mechanism. (`DM-32047 <https://jira.lsstcorp.org/browse/DM-32047>`_)


Bug Fixes
---------

- HTCondor plugin now correctly passes attributes defined in site's 'profile' section to the HTCondor submission files. (`DM-33887 <https://jira.lsstcorp.org/browse/DM-33887>`_)


Other Changes and Additions
---------------------------

- Make HTCondor treat all jobs exiting with a signal as if they ran out of memory. (`DM-32968 <https://jira.lsstcorp.org/browse/DM-32968>`_)
- Make HTCondor plugin pass a group and user attribute to any batch systems that require such attributes for accounting purposes. (`DM-33887 <https://jira.lsstcorp.org/browse/DM-33887>`_)

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
    HTCondor plugin. (`DM-29756 <https://jira.lsstcorp.org/browse/DM-29756>`_)
- Change HTCondor bps plugin to use HTCondor curl plugin for local job transfers. (`DM-32074 <https://jira.lsstcorp.org/browse/DM-32074>`_)

Bug Fixes
---------

- * Fix bug in HTCondor plugin for reporting final job status when ``--id <path>``. (`DM-31887 <https://jira.lsstcorp.org/browse/DM-31887>`_)
- Fix execution butler with HTCondor plugin bug when output collection has period. (`DM-32201 <https://jira.lsstcorp.org/browse/DM-32201>`_)
- Disable HTCondor auto detection of files to copy back from jobs. (`DM-32220 <https://jira.lsstcorp.org/browse/DM-32220>`_)
- * Fixed bug when not using lazy commands but using execution butler.
  * Fixed bug in ``htcondor_service.py`` that overwrote message in bps report. (`DM-32241 <https://jira.lsstcorp.org/browse/DM-32241>`_)
- * Fixed bug when a pipetask process killed by a signal on the edge node did not expose the failing status. (`DM-32435 <https://jira.lsstcorp.org/browse/DM-32435>`_)
