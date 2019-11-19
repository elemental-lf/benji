.. image:: https://img.shields.io/travis/elemental-lf/benji/master.svg?style=plastic&label=Travis%20CI
    :target: https://travis-ci.org/elemental-lf/benji

.. image:: https://img.shields.io/pypi/l/benji.svg?style=plastic&label=License
    :target: https://pypi.org/project/benji/

.. image:: https://img.shields.io/pypi/v/benji.svg?style=plastic&label=PyPI%20version
    :target: https://pypi.org/project/benji/

.. image:: https://img.shields.io/pypi/pyversions/benji.svg?style=plastic&label=Supported%20Python%20versions
    :target: https://pypi.org/project/benji/

Benji Backup
============

Benji Backup is a block based deduplicating  backup software. It builds on the
excellent foundations and concepts of `backy² <http://backy2.com/>`_ by Daniel Kraft.

While Benji can backup any block device or image file (this includes LVM logical
volumes and snapshots) it excels at backing up Ceph RBD images and it also includes
preliminary support to backup iSCSI targets.

Benji is written in Python and is available in `PyPI <https://pypi.org/project/benji/>`_
for installation with ``pip``. Benji also features a generic container image with all
dependencies included as well as an image and Helm chart to integrate Benji into a
`Kubernetes <https://kubernetes.io/>`_ environment to backup Ceph RBD based persistent
volumes.

The documentation is available `here <https://benji-backup.me/>`_.

Status
------

Benji is slowly nearing beta quality. It passes all included tests. The
documentation isn't completely up-to-date. Please open an issue on GitHub if you have
a usage question that is not or incorrectly covered by the documentation. And have a
look at the CHANGES file for any upgrade notes.

Benji requires **Python 3.6.5 or newer** because older Python versions
have some shortcomings in the ``concurrent.futures`` implementation which lead to an
excessive memory usage.

The ``master`` branch contains the development version of Benji and may be broken at
times and may even destroy your backups.  Please use the latest pre-releases to get
some resemblance of stability and a migration path from one pre-release to the next.

The Kubernetes integration is currently in the process of being completely rewritten
to use an operator based approach. In the meantime the ``benji-k8s`` container
image together with the Helm chart already provides a solid way of backing up
persistent volumes provided by Ceph RBD. Benji will detect both normal RBD
volumes and volumes provisioned by Rook's FlexVolume provisioner.

Main Features
-------------

**Small backups**
    Benji deduplicates all data read and each unique block is only written
    to the storage location once. The deduplication takes into account all
    historic data present on the backup storage and so spans all backups
    and all backup sources.

    In addition Benji supports fast state-of-the-art compression to further
    reduce the storage space requirements.

**Fast backups**
    With the help of snapshots and the ``rbd diff`` command Benji only
    backups blocks that have changed since the last backup when used with
    Ceph RBD images. The same mechanism can be extended to other backup
    sources.

**Fast restores**
    Sparse blocks are be skipped on restore providing fast restores of sparsely
    populated disk images.

**Low bandwidth requirements**
    As only changed and not yet known blocks are written to the backup storage,
    the bandwidth requirements for the network connection between Benji and the
    storage location are usually low. Even with newly created block devices
    the traffic to the backup storage location is generally small as these devices
    mostly contain sparse blocks. Enabling compression further reduces the bandwidth
    requirements.

**Support for a variety of backup storage locations**
    Benji supports AWS S3 as a backup storage location and it has options to
    enable compatibility with other S3 implementations like Google Storage,
    Ceph's RADOS Gateway or `Minio <https://www.minio.io/>`_.

    Benji also supports `Backblaze's <https://www.backblaze.com/>`_ B2 Cloud
    Storage which opens up a very cost effective way to store backups.

    Benji is able to use any file based storage including external hard drives
    and network based storage solutions like NFS, SMB or even CephFS.

    Multiple different storage locations can be used simultaneously and in
    parallel to accomodate different backup strategies.

**Confidentiality**
    Benji supports AES-256 in GCM mode to encrypt all data blocks on the
    backup storage. By using envelope encryption every block is encrypted with
    its own unique random key. This makes plaintext attacks even more difficult.

**Integrity**
    Each data block in Benji is protected by a checksum. This checksum is not
    only used for deduplication but also to ensure the integrity of the whole
    backup. Long-term availability of backups is ensured by regularly checking
    existing backups for bit rot.

**Integrated NBD server**
    Benji brings its own NBD (network block device) server which makes backup
    images directly accessible as a block device - even over the network. The
    block device can be mounted if it contains a filesystem and any individual
    files needed can be easily restored even though Benji is a block based
    backup solution.

    Benji can also provide a writable version of a backup via NBD. This enables
    repair operations like ``fsck``. The original backup is not changed in this
    case. All changes are transparently written to a new backup via copy-on-write
    and this new backup can be restored just like any other backup after the
    repair is complete.

**Concurrency**
    Benji supports running multiple operations simultaneously. Instances can
    be distributed across different hosts or containers without the need
    for a central server.

**Extensibility**
    Benji comes with a module framework to easily add new protocols for
    accessing backup sources or storages. New compression and encryption
    algorithms are also easily integrated into Benji.
