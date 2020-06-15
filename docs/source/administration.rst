.. _administration:

Administration
==============

Benji is an important tool when it's responsible for keeping backups of your important data. Backups, scrubs,
restores and cleanups must run smoothly and need to be monitored closely. Also, the database backend and the
collection of data storages need to meet your availability requirements.

Securing Version Metadata
-------------------------

This section shows methods of how to keep version metadata safe even when disaster happens.

Backups and Exports
~~~~~~~~~~~~~~~~~~~

Benji already writes a backup of a *version*'s metadata to the same storage as the block data automatically.
This backup can be restored with ``benji metadata-restore``:

.. command-output::benji metadata-restore --help

The database-less restore option also uses this backup to facilitate restores even when the database is unavailable.
Please see section :ref:`database_less_restore`.

Further copies of the *version* metadata can be made with ``benji metadata-export``. These exports can be stored
somewhere safe and later be restored with ``benji metadata-import``.

.. command-output::benji metadata-export --help

.. command-output::benji metadata-import --help

.. NOTE:: It is advisable to compress the exports as the  JSON export format is quite redundant.

.. NOTE:: A *version*'s metadata can only be imported if a *version* with the same *version* UID  does not exist
    in the database, yet.

.. ATTENTION:: When you remove (``benji rm``) *versions* from the database and then call ``benji cleanup``,
    the blocks containing the backed up data will be removed from storage. No ``benji metadata-import`` can
    bring them back, because Benji's metadata exports only contain information on how to assemble the blocks
    and not the block data themselves.

Database High-Availability
~~~~~~~~~~~~~~~~~~~~~~~~~~

An additional option against data loss is to replicate the SQL database. Please refer to the database documentation.
You should also have a regular database backup in place.

.. CAUTION:: DBMS replication only helps in the case when one server crashes or has a failure. It does not help
    against software-bug related data loss or human error. So the automatic metadata backup and
    ``benji metadata-export`` are the only reliable options for long-term data safety.

Securing Block Data on Storages
-------------------------------

It is advisable for a storage to be redundant and highly available. Most cloud providers have an SLA which
guarantees a certain level of availability. If the storage is self-managed, look into the redundancy and
high-availability options it provides like:

- RAID 1, 5 and 6
- Redundancy options provided by distributed object stores like Ceph or Minio
- DRBD
- Data redundancy and replication mechanisms in filesystems like Btrfs or ZFS

If a storage fails or is affected by data corruption, at best corrupted or incomplete restores are possible.

Benji also supports multiple storages so a backup can be made to more then one storage, so providing a level
of redundancy.

Monitoring
----------

General Advise
~~~~~~~~~~~~~~

* If anything goes wrong Benji exists with a non-zero exit code. Make sure to catch and report this is your scripts.

* Benji writes all output including possible stack traces and command lines to the configured logfile
  (see :ref:`configuration`). If anything goes wrong, you'll be able to visit this logfile and hopefully get enough
  information to troubleshoot the problem.

* Starting Benji with ``--log-level DEBUG`` will increase the log level on the console as well.

* You should also monitor the status of existing backups with commands like ``benji ls 'status != "valid"'``.
  *Versions* with a status of ``incomplete`` are normal while a backup is in progress.

* Benji also changes to process name to indicate what it is currently doing.

.. _machine_output:

Machine output
~~~~~~~~~~~~~~

Some commands can produce machine readable JSON output for usage in scripts::

    INFO: $ benji -m ls
    {
      "versions": [
        {
          "uid": 1,
          "date": "2019-09-27T18:05:21.936087Z",
          "name": "test",
          "snapshot": "",
          "size": 692241,
          "block_size": 4194304,
          "storage_id": 1,
          "status": "valid",
          "protected": false,
          "bytes_read": 692241,
          "bytes_written": 692241,
          "bytes_dedup": 0,
          "bytes_sparse": 0,
          "duration": 0,
          "labels": {
            "label-1": "bla",
            "label-2": "blub"
          }
        }
      ],
      "metadata_version": "2.0.0"
    }

.. NOTE:: Take care to put the ``-m`` between ``benji`` and ``ls``.

All messages emitted by Benji are written to STDERR. In contrast the machine readable output is written to STDOUT.

Here's a table of commands supporting machine readable output and their output:

+------------------+-----------------------------------------------------------+
| Command          | Description of output                                     |
+==================+===========================================================+
| ls               | List of matching *versions*                               |
+------------------+-----------------------------------------------------------+
| backup           | List of newly create *version*                            |
+------------------+-----------------------------------------------------------+
| enforce          | List of removed *versions*                                |
+------------------+-----------------------------------------------------------+
| scrub            | List of scrubbed *versions* and of *versions* with errors |
+------------------+-----------------------------------------------------------+
| deep-scrub       | List of scrubbed *versions* and of *versions* with errors |
+------------------+-----------------------------------------------------------+
| batch-scrub      | List of scrubbed *versions* and of *versions* with errors |
+------------------+-----------------------------------------------------------+
| batch-deep-scrub | List of scrubbed *versions* and of *versions* with errors |
+------------------+-----------------------------------------------------------+

`jq <https://stedolan.github.io/jq/>`_ is an excellent tool for parsing this data and filtering out the bits you want.
Here's a short example, but see the ``scripts/`` and ``images/benji-k8s/scripts/`` directories for more::

    $ benji -m ls | jq -r '.versions[0].date'
    2018-06-07T12:51:19
