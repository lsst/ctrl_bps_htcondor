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

Job Environment
^^^^^^^^^^^^^^^

By default, the htcondor jobs copy the environment from the shell in which
`bps submit` was executed.  To set or override an environment variable via
submission yaml, use an `environment` section.  Other yaml values and pre-existing
environment variables can be used.  Some examples:

.. code-block:: YAML

   environment:
     one: 1
     two: "2"
     three: "spacey 'quoted' value"
     MYPATH: "${CTRL_BPS_DIR}/tests"
     DAF_BUTLER_CACHE_DIRECTORY: "/tmp/mgower/daf_cache/{run_number}"

.. note::

   The `environment` section has to be at the root level.  There is no
   way to change the environment inside another level (e.g., per site,
   per cluster, per pipeline task)


Glideins
^^^^^^^^

`HTCondor`_ is able to to send jobs to run on a remote compute site, even when
that compute site is running a non-HTCondor system, by sending "pilot jobs", or
**glideins**, to remote batch systems.

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

Rarely, a detailed report may warn about job submission issues.  For example:

.. code-block:: bash

   Warn: Job submission issues (last: 01/30/25 10:36:57)

A job submission issue could be intermittent or not.  It may cause
problems with the status or counts in the reports.  To get more information
about the submission issue, look in the ``*.dag.dagman.out`` file for
errors, in particular lines containing ``submit attempt failed``.

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

.. _htc-plugin-provisioning:

Provisioning resources automatically
------------------------------------

Computational resources required to execute a workflow may not always be
managed directly by HTCondor and may need to be provisioned first by a
different workload manager, for example, `Slurm`_.  In such a case
**ctrl_bps_htcondor** can be instructed to run a provisioning job alongside of
the workflow which will firstly create and then maintain `glideins`__ necessary
for the execution of the workflow.

This provisioning job is called ``provisioning_job.bash`` and is managed by
HTCondor.  Be careful not to remove it by accident when using ``condor_rm`` or
``kill`` command.  The job is run on a best-effort basis and will not be
automatically restarted once deleted.

To enable automatic provisioning of the resources, add the following settings to
your BPS configuration:

.. code-block:: yaml

   provisionResources: true
   provisioning:
     provisioningMaxWallTime: <value>

where ``<value>`` is the approximate time your workflow needs to complete,
e.g., 3600, 10:00:00.

This will instruct **ctrl_bps_htcondor** to include a service job that will run
alongside the other payload jobs in the workflow that should automatically
create and maintain glideins required for the payload jobs to run.

If you enable automatic provisioning of resources, you will see the status of
the provisioning job in the output of the ``bps report --id <id>`` command.
Look for the line starting with "Provisioning job status".  For example

.. code-block:: bash
   :emphasize-lines: 8

    X   STATE   %S   ID  OPERATOR PROJECT CAMPAIGN PAYLOAD                  RUN
   --- ------- --- ----- -------- ------- -------- ------- ---------------------------------------
       RUNNING   0   1.0     jdoe     dev    quick  pcheck u_jdoe_pipelines_check_20240924T201447Z


   Path: /home/jdoe/submit/u/jdoe/pipelines_check/20240924T201447Z
   Global job id: node001#1.0#1727208891
   Provisioning job status: RUNNING


                     UNKNOWN MISFIT UNREADY READY PENDING RUNNING DELETED HELD SUCCEEDED FAILED PRUNED EXPECTED
   ----------------- ------- ------ ------- ----- ------- ------- ------- ---- --------- ------ ------ --------
   TOTAL                   0      0       4     0       1       0       0    0         0      0      0        5
   ----------------- ------- ------ ------- ----- ------- ------- ------- ---- --------- ------ ------ --------
   pipetaskInit            0      0       0     0       1       0       0    0         0      0      0        1
   isr                     0      0       1     0       0       0       0    0         0      0      0        1
   characterizeImage       0      0       1     0       0       0       0    0         0      0      0        1
   calibrate               0      0       1     0       0       0       0    0         0      0      0        1
   finalJob                0      0       1     0       0       0       0    0         0      0      0        1

If the provisioning job status is UNREADY, check the end of the report to see
if there is a warning about submission issues.  There may be a temporary problem.
Check the ``*.dag.dagman.out`` in run submit directory for errors, in
particular for ``ERROR: submit attempt failed``.

If the provisioning job status is HELD, the hold reason will appear in parentheses.

The service job managing the glideins will be automatically canceled once the
workflow is completed.  However, the existing glideins will be left for
HTCondor to shut them down once they remain inactive for the period specified
by ``provisioningMaxIdleTime`` (default value: 15 min., see below) or maximum
wall time is reached.

The provisioning job is expected to run as long as the workflow.  If the job
dies, the job status will be `FAILED`.  If the job just completed successfully,
the job status will be `SUCCEEDED` with a message saying it ended early (which
may or may not cause a problem since existing glideins could remain running).
To get more information about either of these cases, check the job output
and error files in the `jobs/provisioningJob` subdirectory.


If the automatic provisioning of the resources is enabled, the script that the
service job is supposed to run in order to provide the required resources *must
be* defined by the ``provisioningScript`` setting in the ``provisioning``
section of your BPS configuration file.  By default, **ctrl_bps_htcondor** will
use ``allocateNodes.py`` from `ctrl_execute`_ package with the following
settings:

.. code-block:: yaml

   provisioning:
     provisioningNodeCount: 10
     provisioningMaxIdleTime: 900
     provisioningCheckInterval: 600
     provisioningQueue: "milano"
     provisioningAccountingUser: "rubin:developers"
     provisioningExtraOptions: ""
     provisioningPlatform: "s3df"
     provisioningScript: |
       #!/bin/bash
       set -e
       set -x
       while true; do
           ${CTRL_EXECUTE_DIR}/bin/allocateNodes.py \
               --account {provisioningAccountingUser} \
               --auto \
               --node-count {provisioningNodeCount} \
               --maximum-wall-clock {provisioningMaxWallTime} \
               --glidein-shutdown {provisioningMaxIdleTime} \
               --queue {provisioningQueue} \
               {provisioningExtraOptions} \
               {provisioningPlatform}
           sleep {provisioningCheckInterval}
       done
       exit 0

``allocateNodes.py`` requires a small configuration file located in the user's
directory to work. With automatic provisioning enabled **ctrl_bps_htcondor**
will create a new file if it does not exist at the location defined by
``provisioningScriptConfigPath`` using the template defined by
``provisioningScriptConfig`` settings in the ``provisioning`` section:

.. code-block:: yaml

   provisioning:
     provisioningScriptConfig: |
       config.platform["{provisioningPlatform}"].user.name="${USER}"
       config.platform["{provisioningPlatform}"].user.home="${HOME}"
     provisioningScriptConfigPath: "${HOME}/.lsst/condor-info.py"

If you're using a custom provisioning script that does not require any
external configuration, set ``provisioningScriptConfig`` to an empty string.

If the file already exists, it will be used as is (BPS will not update it with
config settings). If you wish BPS to overwrite the file with the
``provisioningScriptConfig`` values, you need to manually remove or rename the
existing file.

.. note::

   ``${CTRL_BPS_HTCONDOR_DIR}/python/lsst/ctrl/bps/htcondor/etc/htcondor_defaults.yaml``
   contains default values used by every bps submission when using
   ``ctrl_bps_htcondor`` plugin that are automatically included in your
   submission configuration.

.. __: https://htcondor.readthedocs.io/en/latest/codes-other-values/glossary.html#term-Glidein

.. _htc-plugin-releasing:

Releasing held jobs
-------------------

Occasionally, when HTCondor encounters issues during a job's execution it
places the job in the hold state. You can see what jobs you submitted are being
currently held and why by using the command:

.. code-block::

   condor_q -held

If any of your jobs are being held, it will display something similar to::

    -- Schedd: sdfrome002.sdf.slac.stanford.edu : <172.24.33.226:21305?... @ 10/02/24 10:59:41
    ID           OWNER  HELD_SINCE  HOLD_REASON
    5485584.0    jdoe   9/23 11:04  Error from slot_jdoe_8693_1_1@sdfrome051.sdf.slac.stanford.edu: Failed to execute '/sdf/group/rubin/sw/conda/envs/lsst-scipipe-8.0.0/share/eups/Linux64/ctrl_mpexec/g1ce94f1343+74d41caebd/bin/pipetask' with arguments --long-log --log-level=VERBOSE run-qbb /repo/ops-rehearsal-3-prep /sdf/home/j/jdoe/u/pipelines/submit/u/jdoe/DM-43059/step3/20240301T190055Z/u_jdoe_step3_20240301T190055Z.qgraph --qgraph-node-id 6b5daf05-10fc-462e-82e0-cc618be83a12: (errno=2: 'No such file or directory')
    5471792.0    jdoe   7/10 08:27  File '/sdf/group/rubin/sw/conda/envs/lsst-scipipe-8.0.0/bin/condor_dagman' is missing or not executable
    7636239.0    jdoe   3/20 01:32  Job raised a signal 11. Handling signal as if job has gone over memory limit.
    5497548.0    jdoe   3/6  00:14  Job raised a signal 9. Handling signal as if job has gone over memory limit.
    12863358.0   jdoe   6/27 11:05  Error from slot_jdoe_32400_1_1@sdfrome009.sdf.slac.stanford.edu: Failed to open '/sdf/data/rubin/shared/jdoe/simulation/output/output.0' as standard output: No such file or directory (errno 2)
    20590593.0   jdoe   6/23 13:03  Transfer output files failure at the execution point while sending files to access point sdfrome001. Details: reading from file /lscratch/jdoe/execute/dir_1460253/_condor_stdout: (errno 2) No such file or directory
    12033406.0   jdoe   5/13 10:48  Cannot access initial working directory /sdf/data/rubin/user/jdoe/repo-main-logs/submit/u/jdoe/20240311T231829Z: No such file or directory

.. note::

   If you would like to display held jobs that were submitted for execution
   by other users, use ``condor_q -held <username>`` instead where
   ``<username>`` is the user account which held jobs you would like to check.
   See `condor_q`_ man page for other supported options.

The job that is in the hold state can be released from it with
`condor_release`_ providing the issue that made HTCondor put it in this state
has been resolved. For example, if your job with id 1234.0 was placed in the
hold state because during the execution it exceeded 2048 MiB you requested for
it during the submission, you can double the amount of memory it should request with

.. code-block::

   condor_qedit 1234.0 RequestMemory=4096

and than release it from the hold state with

.. code-block::

   condor_release 1234.0

When the job is released from the hold state HTCondor puts the job into the
IDLE state and will rerun the job using the exact same command and environment
as before.

.. note::

   Placing jobs in the hold state due to missing files or directories usually
   happens when the gliedins expire or there are some filesystem issues.  After
   creating new glideins with ``allocateNodes.py`` (see
   :ref:`htc-plugin-provisioning` for future submissions) or the filesystem
   issues have been resolved typically it should be safe to release the jobs
   from the hold state.

If multiple jobs were placed by HTCondor in the hold state and you only want to
deal with a subset of currently held jobs, use ``-constraint <expression>``
option that both `condor_qedit`_ and `condor_release`_ support where
``<expression>`` can be an arbitrarily complex `HTCondor ClassAd`__ expression.
For example

.. code-block::

   condor_qedit -constraint "JobStatus == 5 && HoldReasonCode == 3 && HoldReasonSubCode == 34" RequestMemory=4096
   condor_release -constraint "JobStatus == 5 && HoldReasonCode == 3 && HoldReasonSubCode == 34"

will only affect jobs that were placed in the hold state (``JobStatus`` is 5)
for a specific reason, here, the memory usage exceeded memory limits
(``HoldReasonCode`` is 3 *and* ``HoldReasonSubCode`` is 34).

.. __: https://htcondor.readthedocs.io/en/latest/classads/index.html

.. note::

   By default, BPS will automatically retry jobs that failed due to the out of
   memory error (see `Automatic memory scaling`__ section in **ctrl_bps**
   documentation for more information regarding this topic) and the issues
   illustrated by the above examples should only occur if automatic memory
   scalling was explicitly disabled in the submit YAML file.

.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#automatic-memory-scaling

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

I enabled automatic provisioning, but my jobs still sit idle in the queue!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The service node responsible for executing the provisioning script runs on a
best-effort basis.  If this node fails to submit correctly or crashes during
the workflow execution, this will not register as an error and the workflow
will continue normally until the existing gliedins expire.  As a result,
payload jobs may get stuck in the job queue if the glideins were not created
or expired before the execution of the workflow could be completed.

Firstly, use ``bps report --id <run id>`` to display the run report and look
for the line

.. code-block::

   Provisioning job status: <status>

If the ``<status>`` is different from RUNNING, it means that the automatic
provisioning is not working.  In such a case, create `glideins manually`__ to
complete your run.

.. __: https://developer.lsst.io/usdf/batch.html#ctrl-bps-htcondor

.. _HTCondor: https://htcondor.readthedocs.io/en/latest/
.. _Slurm: https://slurm.schedmd.com/overview.html
.. _bps cancel: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#canceling-submitted-jobs
.. _bps report: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#checking-status
.. _bps restart: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#restarting-a-failed-run
.. _bps submit: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#submitting-a-run
.. _ctrl_bps: https://github.com/lsst/ctrl_bps
.. _ctrl_execute: https://github.com/lsst/ctrl_execute
.. _condor_q: https://htcondor.readthedocs.io/en/latest/man-pages/condor_q.html
.. _condor_qedit: https://htcondor.readthedocs.io/en/latest/man-pages/condor_qedit.html
.. _condor_release: https://htcondor.readthedocs.io/en/latest/man-pages/condor_release.html
.. _condor_rm: https://htcondor.readthedocs.io/en/latest/man-pages/condor_rm.html
.. _lsst_distrib: https://github.com/lsst/lsst_distrib.git
