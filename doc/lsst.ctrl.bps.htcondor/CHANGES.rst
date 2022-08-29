lsst-ctrl-bps-htcondor v24.0.0 (2022-08-29)
===========================================

New Features
------------

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
