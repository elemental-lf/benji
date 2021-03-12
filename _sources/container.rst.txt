.. include:: global.rst.inc
.. _container:

Containerized Benji
===================

Images
------

The container images are hosted in the GitHub Container Registry:

* ``ghcr.io/elemental-lf/benji:latest``
* ``ghcr.io/elemental-lf/benji-k8s:latest``

The ``latest`` tag always points to the latest released version. Images for all Git branches of the repository are
available under their branch name, i.e. the current development version is available by referring to the ``master`` tag.

.. NOTE:: Older versions of the images are still available on Docker Hub, but no new images will be published there.

benji
~~~~~

The images is based on CentOS 7. Ceph and iSCSI support are present.

The Benji configuration should be put into ``/etc/benji/benji.yaml``. Either by inheriting from this image and
overwriting it or by mounting it directly into the container. By default a minimal test configuration is provided
by the image.

The default entry point is just ``/bin/bash``.

One use case for this image is for testing Benji::

    docker run --interactive --tty --rm ghcr.io/elemental-lf/benji:latest

After that you can directly proceed with step 1 of the instructions in section :ref:`quickstart`.

benji-k8s
~~~~~~~~~

This image is directly derived from the ``benji`` image above. It includes a number of scripts to do backups of
Kubernetes persistent volumes backed by Ceph RBD:

- ``benji-backup-pvc`` for doing backups
- ``benji-restore-pvc`` for restore operations to either existing or new PVCs/PVs
- ``benji-command`` for all other Benji commands
- ``benji-versions-status`` publishes the number of invalid or incomplete versions as Prometheus metrics

The scripts provide support for volumes provisioned by the classic RBD volume provider, by Rook Ceph CSI and by
Ceph CSI.

Example usages::

    benji-backup-pvc --all-namespaces -l 'release in (prod)'
    benji-backup-pvc --namespace staging
    benji-command enforce latest3,hours24,days30,months3 'labels["benji-backup.me/instance"] == "benji-k8s"'
    benji-command cleanup
    benji-versions-status
    benji-command batch-deep-scrub --version-percentage 10 --block-percentage 33 'labels["benji-backup.me/instance"] == "benji-k8s"'

The backup script ``benji-backup-pvc`` first searches for ``PersistemtVolumeClaims`` matching the selector supplied on
the command line. Direct backups of ``PersistentVolumes`` are currently not supported by this script.

.. TIP:: See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
    for possible ways to construct the selector.

``benji-command enforce`` should be called regularly to expire old backup *versions*. Also ``benji-command cleanup``
needs to be executed once in a while to actually remove blocks that are no longer used from the storages.

At the end of each command `Prometheus <https://prometheus.io/>`_ metrics are pushed to the configured
`pushgateway <https://github.com/prometheus/pushgateway>`_. The format of the variable is ``host:port``. If ``host``
part is left blank localhost is assumed. If ``PROM_PUSH_GATEWAY`` is not set, this step is skipped.

The backup script uses Ceph's differential backup features if possible. Normally only the initial backup is a full
backup. RBD snapshots names are generated with a prefix of ``b-``.

Helm Chart
----------

The Helm chart is the preferred way to deploy the ``benji-k8s`` image.

.. NOTE:: The deployed resources create a service account which has the right to *get*,
    *list* and *watch* all PersistentVolume, PersistentVolumeClaim, Storageclasses and Pod resources in all
    namespaces. Additionally it is able to *create* Events and PersistentVolumeClaims.
