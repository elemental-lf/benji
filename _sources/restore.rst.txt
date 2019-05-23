.. include:: global.rst.inc

.. _restore:

Restore
=======

.. command-output:: benji restore --help

There are two possible restore options with Benji.

.. NOTE:: If you determined the *version* you want to restore it is a good idea to protect this *version* to prevent
    any accidental or automatic removal by any retention policy enforcement that you might have configured.

Full Restore
------------

A full restore either saves the image into a file (i.e. an image file), to a device (e.g. /dev/hda) or to a Ceph RBD
volume.

The target is specified by the URI scheme. Examples::

    $ benji restore --sparse $VERSION_UID file:///var/lib/vms/myvm.qcow2
    $ benji restore --sparse --force $VERSION_UID file:///dev/sda1
    $ benji restore --sparse $VERSION_UID rbd:pool/myvm_restore

If the target already exists, i.e.

- it is a device file
- an existing Ceph RBD volume
- or an existing image file

you need to ``--force`` the restore. Benji will create an Ceph RBD volume or file if it does not exist, yet.

In most cases ``--sparse`` should be used to skip the restore of sparse (empty) blocks. This increases restore
performance and in the case of Ceph RBD or thinly provisioned LVM volumes also decreases space usage. When ``--sparse``
is not specified sparse blocks are written as blocks full of zeros.

.. CAUTION:: If you use ``--sparse`` to restore to an existing device or file, sparse blocks will not be written,
    so whatever random data was in the location of the sparse block before the restore will remain. This is not
    the case with Ceph RBD as ``--sparse`` will discard all currently used blocks before beginning the restore.

.. _database_less_restore:

Restoring without a database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``benji restore`` also supports a mode to restore a *version* even when the database backend is not available. This
mode is activated by passing the ``--database-backend-less`` switch to ``benji restore``. Benji will import the
metadata backup of the specified *version* from the storage into an ad-hoc in-memory database  and then restore the
image normally.

This is for failure scenarios where the database is unavailable and you still need to restore a *version*. But because
of the database unavailability you can't just execute ``benji ls`` to determine the right *version* to restore, you
need to generate some reports beforehand and save them somewhere to be able to chose the right *version*!

NBD Server
----------

.. command-output:: benji nbd --help

Benji comes with its own NBD server which when started exports all known *versions*. These *versions* can then be
mounted on any Linux host. The requirements on the Linux host are:

- loaded ``nbd`` kernel module (``modprobe nbd`` as root)
- installed ``nbd-client`` program (RPM package ``nbd`` on RHEL/CentOS7/Fedora)

The ``nbd-client`` contacts Benji's NBD server and connects an exported *version* to am NBD block device (``/dev/nbd*``)
on the Linux host. If the image contains a filesystem it can be mounted normally. You can then search for the relevant
files and restore them.

There are some known issues with ``nbd-client``:

* Some problems have been reported with ``nbd-client`` 3.18. Please see https://github.com/elemental-lf/benji/issues/12.

* Some versions of ``nbd-client`` use a timeout value of zero which also leads to problems. Please explicit specify
  a timeout with ``-t`` in these cases.

* If the image has a partition table the Linux kernel will have problems parsing the partition table
  when the device block size used by ``nbd-client`` is different from the one used during creation of the partition
  table. Please use the ``-b`` option of ``nbd-client`` to specify the original device block size which normally
  will be 512. Newer versions of ``nbd-client`` already changed the default block size from 1024 to 512 because of
  this. See https://github.com/NetworkBlockDevice/nbd/commit/128fd556286ff5d53c5f2b16c4ae5746b5268a64.


Read-Only Mount
~~~~~~~~~~~~~~~

This command will run the NBD server in read-only mode and wait for incoming connections::

    $ benji nbd -r
        INFO: Starting to serve nbd on 127.0.0.1:10809

Benji's NBD server will serve all available *versions* and it is possible to access each one of them as a block device
by using ``nbd-client`` und the in-kernel ``nbd`` driver::

    # Load the nbd kernel module
    $ sudo modprobe nbd

    # Connect a Benji version to a free NBD block device
    $ sudo nbd-client -N V0000000001 127.0.0.1 -p 10809 -b 512 -t 10 /dev/nbd0
    Negotiation: ..size = 10MB
    bs=512, sz=10485760 bytes

    # Detect partitions
    # (partprobe might throw a few WARNINGs because we're read-only)
    partprobe /dev/nbd0

    # Mount the filesystem
    mount -o ro /dev/nbd0p1 /mnt

If the image just contains a filesystem without a partition table the ``partprobe`` command can be skipped and
the ``/dev/nbd0`` device can be mounted directly. Some filesystem will require additional mount options as they try
to write to the device even when ``ro`` is specified.

The NBD server will signal an incoming connection::

     INFO: Incoming connection from 127.0.0.1:33714
    DEBUG: [127.0.0.1:33714]: opt=7, len=17, data=b'\x00\x00\x00\x0bV0000000001\x00\x00'
    DEBUG: [127.0.0.1:33714]: opt=1, len=11, data=b'V0000000001'
     INFO: [127.0.0.1:33714] Negotiated export: V0000000001
     INFO: nbd is read only.

After you're done using the filesystem you need to unmount it and disconnect the NBD device::

    umount /mnt
    nbd-client -d /dev/nbd0

You can then either reconnect to this or another *version* or terminate Benji's NBD server. The server is also able to
serve multiple *versions* at once.

Benji's NBD server by default listens on 127.0.0.1 (i.e. localhost) for incoming connections. For the server to be
reachable from the outside bind it to 0.0.0.0 or the specific address of another interface::

    benji nbd -a 0.0.0.0 -r


Read-Write Mount
~~~~~~~~~~~~~~~~

In addition to providing read-only access, Benji also allows read-write access in a safe way. This means, the original
*version* **will not be modified**. To access the available *versions* in read-write mode start the NBD server
without the ``-r`` option.

After connecting the NBD device you can initiate any repair procedures required like ``fsck``.  Any writes to the
device will initiate a copy-on-write (COW) of the original blocks to a new *version* which is dynamically created
by Benji.

After disconnecting the NBD device Benji will start to fixate the COW *version*. Depending on how many changes
have been done to the original *version* this will take some time!::

    INFO: [127.0.0.1:46526] disconnecting
    INFO: Fixating version V0000000002 with 1024 blocks, please wait!
    INFO: Fixation done. Deleting temporary data, please wait!
    INFO: Finished.

.. CAUTION:: If you end the NBD server before the last "INFO: Finished." is reported, your copy-on-write clone will not
    be written completely and thus be incomplete. However, the original backup *version*
    **will be untouched in any case**.

The newly created *version* can be seen in the output of ``benji ls``::

    $ benji ls
        INFO: $ benji ls
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot_name                           |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+
    | 2018-06-10T01:00:43 | V0000000001 | test |                                         | 41943040 |    4194304 |  True |   False   |      |
    | 2018-06-10T01:01:16 | V0000000002 | test | nbd-cow-V0000000001-2018-06-10T01:01:16 | 41943040 |    4194304 |  True |    True   |      |
    +---------------------+-------------+------+-----------------------------------------+----------+------------+-------+-----------+------+

The name will be the same as the original *version*. The snapshot_name will start with the prefix *nbd-cow-* followed
by the *version* UID followed by a timestamp.

The COW *version* will automatically be marked as protected by Benji to prevent removal by any automatic retention
policy enforcement configured. This ensures the new *version* won't be destroyed accidentally. To be able to remove
the *version* the protection needs to be lifted with ``benji unprotect``.

.. NOTE:: The new created COW *version* can be restored just like any other *version*. Both the original and the
    COW *version* are independent from each other and each can be removed without affecting the other.

Restoring Invalid *Versions*
----------------------------

During a restore Benji will compare each restored block's checksum to the one stored in the database.
This even happens when the block has been marked as invalid previously. If it encounters a difference, the block
and all *versions* also referencing this block will be marked as invalid and a warning will be given::

   ERROR: Checksum mismatch during restore for block 9 (UID 1-a) (is: e36cee7fd34ae637... should-be: dea186672147e1e3..., block.valid: True). Block restored is invalid.
    INFO: Marked block invalid (UID 1-a, Checksum dea186672147e1e3. Affected versions: V0000000001, V0000000002
    INFO: Marked version invalid (UID V0000000001)
    INFO: Marked version invalid (UID V0000000002)

Even when encountering such an error, Benji will continue the restore.

.. NOTE:: The philosophy behind this is that restores should always succeed, even if there
    is data corruption. Often invalid data is in irrelevant places or can be fixed later.
    You get as much of your data back as possible!
