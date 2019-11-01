.. include:: global.rst.inc

Backup
======

.. command-output:: benji backup --help

Simple Backup
-------------

A backup can be initiated with ``benji backup``::

    $ benji backup $BACKUP_SOURCE $BACKUP_NAME

``$BACKUP_SOURCE`` should be replaced by a URI specifying the backup source.``$BACKUP_NAME`` specifies the name
given to the backup of this specific source and normally does not change when doing multiple backups of the same
source. Different backups of the same source are differentiated by their *version* UID.

Currently supported schemes for backup sources are **file** and **rbd**. So real-world examples would look like this::

    $ benji backup file:///var/lib/vms/database.img database
    $ benji backup rbd:poolname/database@snapshot1 database

Versions
--------

A backup at a specific point-in-time is called a *version*. Apart from a list of blocks a *version* has a number of
fields describing it:

* **date**: Date and time of the backup
* **uid**: Unique identifier for this version (always starts with the letter ``V`` followed by a number with optional
  leading zeros)
* **name**: Name as specified on the ``benji backup`` command line
* **snapshot**: Snapshot name as specified on the ``benji backup`` command line with the ``--snapshot-name`` option
* **size**: Size of the backed up image in bytes
* **block_size**: Block size in bytes
* **valid**: Validity of this version (either ``valid``, ``invalid``, or ``incomplete``)
* **protected**: Boolean flag indicating if this version is protected from removal
* **labels**: List of label name, value pairs as specified on the ``benji backup`` command line with the ``--label``
  option or by using ``benji label``

You can output this data with::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+------+----------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+----------+----------+------------+-------+-----------+------+
    | 2018-06-07T12:51:19 | V0000000001 | test |          | 41943040 |    4194304 |  True |   False   |      |
    +---------------------+-------------+------+----------+----------+------------+-------+-----------+------+


.. HINT::
    It is possible to filter the output of ``benji ls`` with a filter expression. See :ref:`filter_expressions`.

.. _differential_backup:

Differential Backup
-------------------

Benji only backups changed blocks. It can do this in two different ways:

1. **By reading the whole image**: Benji reads and calculates a checksum for each block. The checksum is then looked
   up in the database. If a block with the same checksum is found, only a reference to this block is saved. Otherwise
   a new block is created and saved to the storage.

2. **By using a hints file**: The hints file is a JSON formatted list of (offset, size, usage) tuples (see
   :ref:`hints_file`). Each tuple indicates if a specific region of the image is used at all or if it has changed
   since the last backup. The format of the hints file understood by Benji matches the output of
   ``rbd diff … --format=json``. If a hints file is specified Benji only reads and checksums blocks hinted at by the
   hints file. The checksum is then again looked up in the database. If a block with the same checksum is found, only
   a reference to this block is saved. Otherwise a new block is created and saved to the storage.
   The hints file is passed via the ``--rbd-hints`` option to ``benji backup``. It is not Ceph RBD specific per se and
   could also be used in other scenarios like the backup of LVM snapshots.

.. NOTE:: Benji does **forward-incremental backups**. In contrast to other backup modes, there is no need to create
    another full backup after the first one. From a restore standpoint all versions are full backups (sometimes
    called a synthetic full backup).

.. NOTE:: If Benji detects that a backup source's size has changed, Benji will assume that the image was extended at the
    end. This is normally the case when you resize partitions or when extending logical volumes or Ceph RBD images.

Examples
~~~~~~~~

LVM and other images
********************

Day 1 (Initial Backup)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ benji backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

Day 2..n (Differential Backups)::

    $ lvcreate --size 1G --snapshot --name snap /dev/vg00/lvol1
    $ benji backup file:///dev/vg00/snap lvol1
    $ lvremove -y /dev/vg00/snap

.. IMPORTANT:: With LVM snapshots, the snapshot volume increases in size as the origin volume changes. If the snapshot
    is 100% full, it is lost and invalid. It is important to monitor the snapshot usage with the ``lvs`` command
    to make sure the snapshot does not fill up completely. The ``--size`` parameter defines the space reserved for
    changes during the snapshot's existence. Snapshots of thin volumes don't need the ``--size`` parameter, they use
    the space available in the pool to keep track of changes. Also note that LVM does read-write-write for any
    overwritten block while a snapshot exists. This may hurt your performance.

Ceph RBD
********

With Ceph RBD Ceph itself is able to calculate the changes between two snapshots. Since the *jewel* version of Ceph
this is a very fast process if the *fast-diff* feature is enabled. In this case only metadata has to be compared.


Manually
^^^^^^^^

In this example, we will backup an RBD image called ``vm1`` which is in the pool ``pool``.

1. Create an initial backup::

    $ rbd snap create pool/vm1@backup1
    $ rbd diff --whole-object pool/vm1@backup1 --format=json > /tmp/vm1.diff
    $ benji backup --snapshot-name backup1 --rbd-hints /tmp/vm1.diff rbd:pool/vm1@backup1 vm1

2. Create a differential backup::

    $ rbd snap create pool/vm1@backup2
    $ rbd diff --whole-object pool/vm1@backup2 --from-snap backup1 --format=json > /tmp/vm1.diff

    # Delete old snapshot
    $ rbd snap rm pool/vm1@backup1

    # Identify the UID of the version corresponding to the last RBD snapshot
    $ benji ls 'name == "vm1" and snapshot == "backup1"'

    # And backup (replace V001234567 with the version UID you identified in the last step)
    $ benji backup --snapshot-name backup2 --rbd-hints /tmp/vm1.diff --base-version V001234567 rbd:pool/vm1@backup2 vm1

Automation
^^^^^^^^^^

Bash
""""

Benji includes an example Bash script ``scripts/ceph.sh`` which automates the process outlined in the last section.

The general workflow of this script is:

* When the backup of an RBD image is initiated, the latest RBD snapshot is looked up.

.. NOTE:: Only RBD snapshots that begin the prefix *b-* are considered. All other snapshots are left alone. This makes
    it possible to have other snapshots that will not be touched by Benji.

* If no RBD snapshot is found, an initial backup is performed.

* If there is an RBD snapshot, Benji is asked if it has corresponding *version* of this snapshot. If not, an initial
  backup is performed.

* If Benji has a *version* of the snapshot, a hints file is created via
  ``rbd diff --whole-object <new snapshot> --from-snap <old snapshot> --format=json``.

* After that Benji only backups the changes as listed in the hints file.

.. NOTE:: This alone won't be enough to be on the safe side. The validity of the backup data needs to checked
    regularly. Please refer to section :ref:`scrubbing`.

Python
""""""

There is also a number of Python modules in the ``benji.helpers`` package. The modules are independent from the rest of
Benji's Python modules and only call the command line interface of Benji.

* ``benji.helpers.ceph``: Implements the same functionality as the Bash scripts described in the previous section.
* ``benji.helpers.utils``: Utility functions used by other modules in the ``benji.helpers`` package.
* ``benji.helpers.settings``: Configuration variables used by other modules in the ``benji.helpers`` package derived from
  environment variables.
* ``benji.helpers.kubernetes``: Helper functions for interacting with Kubernetes (requires ``kubectl``)
* ``benji.helpers.prometheus``: Helper functions and metric definitions for pushing metrics to a Prometheus
  ``pushgateway``

Usage examples for these helpers can be found in ``images/benji-k8s/bin``.

.. NOTE:: If you want to use them as the basis for your own scripts please make copies of the parts you need, so that
    you are not affected by changes in future versions of Benji.

Specifying a block size
-----------------------

To perform a backup Benji splits up the image into equal sized blocks. [1]_

By default the block size specified in the configuration file is used. But the block size can also be set on the
command line on a *version* by *version* basis, but be aware that this will affect deduplication and increase space
usage.

One possible use case for different block sizes is backing up LVM volumes and Ceph images with the same Benji
installation. While for Ceph RBD four megabytes is usually the best size, LVM volumes might profit from a smaller
block size.

If you want to base a new version on an old version (as it can be the case when doing a differential backup) the block
size of the old and new version must match. Benji will terminate with an error if this is not the case.

Labeling *Versions*
-------------------

A *version* can have zero or more associated labels. A label consists of a label name and an optional label value. To
specify a label the ``benji backup`` command provides the command line switch ``--label`` which can be repeated
multiple times to set multiple labels at once.

    $ benji backup --label example.com/label=value --label example.com/label-2 rbd:cephstorage/test_vm test_vm

If no label value is specified it is set to an empty string.

Later on it is possible to add, change or remove labels with ``benji label``::

    $ benji label V0000000001 example.com/label-1=value-1 example.com/label-2

To remove a label specify its name followed by a dash (``-``)::

    $ benji label V0000000001 example.com/label-1-

It is no error to change or remove a label which already exists or which does not exist anymore respectively.

.. _hints_file:

The Hints File
--------------

Example of a hints file::

    [{"offset":0,"length":4194304,"exists":"true"},
    {"offset":4194304,"length":4194304,"exists":"true"},
    {"offset":8388608,"length":4194304,"exists":"true"},
    {"offset":12582912,"length":4194304,"exists":"true"},
    {"offset":16777216,"length":4194304,"exists":"true"},
    {"offset":20971520,"length":4194304,"exists":"true"},
    {"offset":25165824,"length":4194304,"exists":"true"},
    {"offset":952107008,"length":4194304,"exists":"true"}

.. [1] Except the last block which may vary in length.
