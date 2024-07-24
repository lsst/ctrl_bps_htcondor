.. _htc-plugin-overview:

Overview
--------

LSST Batch Processing Service (BPS) allows large-scale workflows to execute in
well-managed fashion, potentially in multiple environments.  The service is
provided by the `ctrl_bps`_ package.  ``ctrl_bps_htcondor`` is a plugin
allowing `ctrl_bps` to execute workflows on computational resources managed by
`HTCondor`_.

.. _htc-plugin-preqs:

Prerequisites
-------------

#. `ctrl_bps`_, the package providing BPS.
#. `HTCondor`_ cluster.
#. HTCondor's Python `bindings`__.

.. __: https://htcondor.readthedocs.io/en/latest/apis/python-bindings/index.html

.. _htc-plugin-installing:

Installing the plugin
---------------------

Starting from LSST Stack version ``w_2022_18``, the HTCondor plugin package for
Batch Processing Service, ``ctrl_bps_htcondor``, comes with ``lsst_distrib``.
However, if you'd like to  try out its latest features, you may install a
bleeding edge version similarly to any other LSST package:

.. code-block:: bash

   git clone https://github.com/lsst/ctrl_bps_htcondor
   cd ctrl_bps_htcondor
   setup -k -r .
   scons

.. _htc-plugin-wmsclass:

Specifying the plugin
---------------------

The class providing `HTCondor`_ support for `ctrl_bps`_ is ::

    lsst.ctrl.bps.htcondor.HTCondorService

Inform `ctrl_bps`_ about its location using one of the methods described in its
`documentation`__.

.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/index.html

.. _htc-plugin-defining-submission:

Defining a submission
---------------------

BPS configuration files are YAML files with some reserved keywords and some
special features. See `BPS configuration file`__ for details.

The plugin supports all settings described in `ctrl_bps documentation`__
*except* **preemptible**.

.. Describe any plugin specific aspects of defining a submission below if any.

`HTCondor`_ is able to to send jobs to run on a remote compute site, even when
that compute site is running a non-HTCondor system, by sending "pilot jobs", or
**gliedins**, to remote batch systems.

Nodes for HTCondor's glideins can be allocated with help of `ctrl_execute`_.
Once you allocated the nodes, you can specify the site where there are
available in your BPS configuration file. For example:

.. code-block:: YAML

   site:
     acsws02:
       profile:
         condor:
           requirements: '(ALLOCATED_NODE_SET == &quot;${NODESET}&quot;)'
           +JOB_NODE_SET: '&quot;${NODESET}&quot;'

.. note::

   Package `ctrl_execute`_ is not the part of the `lsst_distrib`_ metapackage
   and it needs to be (as well as its dependencies) installed manually.

.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#bps-configuration-file
.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#supported-settings

.. .. _htc-plugin-authenticating:

.. Authenticating
.. --------------

.. Describe any plugin specific aspects of an authentication below if any.

.. _htc-plugin-submit:

Submitting a run
----------------

See `bps submit`_.

.. Describe any plugin specific aspects of a submission below if any.

.. _htc-plugin-report:

Checking status
---------------

See `bps report`_.

.. Describe any plugin specific aspects of checking a submission status below
   if any.

In order to make the summary report (``bps report``) faster, the plugin
uses summary information available with the DAGMan job.  For a running
DAG, this status can lag behind by a few minutes.  Also, DAGMan tracks
deletion of individual jobs as failures (no separate counts for
deleted jobs).  So the summary report flag column will show ``F`` when
there are either failed or deleted jobs.  If getting a detailed report
(``bps report --id <id>``), the plugin reads detailed job information
from files.  So, the detailed report can distinguish between failed and
deleted jobs, and thus will show ``D`` in the flag column for a running
workflow if there is a deleted job.

Occasionally, some jobs are put on hold by HTCondor.  To see the reason why
jobs are being held, use

.. code-block:: bash

   condor_q -hold <id>    # to see a specific job being held
   condor-q -hold <user>  # to see all held jobs owned by the user

.. _htc-plugin-cancel:

Canceling submitted jobs
------------------------

See `bps cancel`_.

.. Describe any plugin specific aspects of canceling submitted jobs below
   if any.

If jobs are hanging around in the queue with an ``X`` status in the report
displayed by ``bps report``, you can add the following to force delete those
jobs from the queue ::

    --pass-thru "-forcex"

.. _htc-plugin-restart:

Restarting a failed run
-----------------------

See `bps restart`_.

.. Describe any plugin specific aspects of restarting failed jobs below
   if any.

A valid run id is one of the following:

* job id, e.g., ``1234.0`` (using just the cluster id, ``1234``, will also
  work),
* global job id (e.g.,
  ``sdfrome002.sdf.slac.stanford.edu#165725.0#1699393748``),
* run's submit directory (e.g.,
  ``/sdf/home/m/mxk/lsst/bps/submit/u/mxk/pipelines_check/20230713T135346Z``).

.. note::

   If you don't remember any of the run's id you may try running

   .. code::

      bps report --username <username> --hist <n>

   where ``<username>`` and ``<n>`` are respectively your user account and the
   number of past days you would like to include in your search.  Keep in mind
   though that availability of the historical records depends on the HTCondor
   configuration and the load of the computational resource in use.
   Consequently, you may still get no results and using the submit directory
   remains your only option.

When execution of a workflow is managed by `HTCondor`_, the BPS is able to
instruct it to automatically retry jobs which failed due to exceeding their
memory allocation with increased memory requirements (see the documentation of
``memoryMultiplier`` option for more details).  However, these increased memory
requirements are not preserved between restarts.  For example, if a job
initially run with 2 GB of memory and failed because of exceeding the limit,
`HTCondor`_ will retry it with 4 GB of memory.  However, if the job and as a
result the entire workflow fails again due to other reasons, the job will ask
for 2 GB of memory during the first execution after the workflow is restarted.

.. _htc-plugin-troubleshooting:

Troubleshooting
---------------

Where is stdout/stderr from pipeline tasks?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For now, stdout/stderr can be found in files in the run submit directory.

Why did my submission fail?
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Check the ``*.dag.dagman.out`` in run submit directory for errors, in
particular for ``ERROR: submit attempt failed``.


.. _HTCondor: https://htcondor.readthedocs.io/en/latest/
.. _bps cancel: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#canceling-submitted-jobs
.. _bps report: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#checking-status
.. _bps restart: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#restarting-a-failed-run
.. _bps submit: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#submitting-a-run
.. _ctrl_bps: https://github.com/lsst/ctrl_bps
.. _ctrl_execute: https://github.com/lsst/ctrl_execute
.. _condor_q: https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
.. _condor_rm: https://htcondor.readthedocs.io/en/latest/man-pages/condor_rm.html
.. _lsst_distrib: https://github.com/lsst/lsst_distrib.git
