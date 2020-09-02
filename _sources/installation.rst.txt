.. include:: global.rst.inc
.. _installation:

Installation
============

Release versions of Benji are available on PyPi and can installed via `pip`. In addition there are two container images:
One generic image and an extended one for the use with Kubernetes. The generic ``elementalnet/benji`` image is the
easiest and fastest way to try out Benji but can also be used in production. It includes all dependencies and extra
features listed below (including RBD and iSCSI support). See section :ref:`container` for more information about the
images.

Fedora 27 and up
----------------

The distribution includes a supported version of Python 3. Make sure to install the latest available update.

openSUSE
--------

The distribution includes a supported version of Python 3. Benji is also available through the official repositories of
openSUSE Tumbleweed. For older, versioned releases of openSUSE you need to add the devel project::

    zypper ar obs://Archiving:Backup ab-benji
    zypper ref

Then you can install it via::

    zypper in benji

RHEL/CentOS 7
-------------

A recent version of Python 3 is included in the EPEL repository::

    yum install -y epel-release
    yum install -y python36-devel python36-pip python36-libs python36-setuptools

Ubuntu 16.04
------------

This version of Ubuntu doesn't have a current Python installation. But Python 3
can be installed via private repository::

    apt-get update
    apt-get install --no-install-recommends software-properties-common python-software-properties
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get install --no-install-recommends python3.6 python3.6-venv python3.6-dev git gcc

.. NOTE:: For more information about this Personal Package Archive (PPA)
    please see https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa.

Common to All Distributions
---------------------------

After installing a recent Python version, it is now time to install Benji and its dependencies::

    # Create new virtual environment
    python3.6 -m venv /usr/local/benji
    # Activate it (your shell prompt should change)
    . /usr/local/benji/bin/activate
    # Alternative A: Install a specific released version from PyPI (0.8.0)
    pip install benji==0.8.0
    # Alternative B: Install the latest released version from PyPI
    pip install benji
    # Alternative C: Install the latest version from the master branch of the Git repository
    pip install git+https://github.com/elemental-lf/benji

For certain features additional dependencies are needed. These are referenced by a symbolic name:

- ``s3``: AWS S3 object storage support
- ``b2``: Backblaze's B2 Cloud object storage support
- ``compression``: Compression support

Specify any extra extra features as a comma delimited list in square brackets after the package URL::

    pip install benji[compression,s3,readcache,b2]==0.8.0


To upgrade an existing installation use the same command line but add the ``--upgrade`` option.

.. NOTE:: It is recommended to install and use the compression feature for almost all use cases as it decreases storage
   space usage significantly.

Ceph RBD Support
----------------

The Ceph RBD support cannot be installed via `pip` like the other dependencies as the Python bindings for `librados`
and `librbd` are not available on PyPi. Depending on the distribution and the used Ceph version pre-built packages
are available:

- For RHEL and CentOS 7 see https://docs.ceph.com/docs/master/install/get-packages/, the packages are named
  `python36-rados` and `python36-rbd`.
- For Ubuntu (Xenial and Bionic) also see https://docs.ceph.com/docs/master/install/get-packages/, the packages are
  named `python3-rados` and `python3-rbd`.
- The Ceph project no longer supplies Debian packages but unofficial packages can be found at
  https://mirror.croit.io/debian-mimic/ (Mimic) and https://mirror.croit.io/debian-nautilus/ (Nautilus). Usage
  instructions are given in this `blog post <https://croit.io/2018/09/23/2018-09-23-debian-mirror>`_.
- Proxmox 6.0 includes packages for Ceph Nautilus (`python3-rados` and `python3-rbd`).
- Some distributions also provide packages for recent versions of Ceph directly via their official
  repositories. For Fedora 30 the packages are named `python3-rados` and `python3-rbd` for example and are based
  on Nautilus. Debian Buster includes packages for Ceph Luminous (`python3-rados` and `python3-rbd`).

.. NOTE:: If Benji is installed in a virtual environment as suggested above, system-wide Python packages are not
   available by default. To access system-wide Python packages like Ceph's Python bindings the virtual environment
   needs to be created with the `--system-site-packages` option.

If all other options fail, it is still possible to directly install the Python binding from the Ceph source code. For
RHEL/CentOS 7 the procedure looks like this::

    cat >/etc/yum.repos.d/ceph.repo <<EOF
    [ceph]
    name=Ceph packages for \$basearch
    baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/\$basearch
    enabled=1
    priority=2
    gpgcheck=1
    gpgkey=https://download.ceph.com/keys/release.asc

    [ceph-noarch]
    name=Ceph noarch packages
    baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/noarch
    enabled=1
    priority=2
    gpgcheck=1
    gpgkey=https://download.ceph.com/keys/release.asc

    [ceph-source]
    name=Ceph source packages
    baseurl=https://download.ceph.com/rpm-{ceph-release}/{distro}/SRPMS
    enabled=0
    priority=2
    gpgcheck=1
    gpgkey=https://download.ceph.com/keys/release.asc
    EOF

    export CEPH_CODENAME="nautilus"
    export CEPH_DISTRO="el7"
    sed -i -e "s/{ceph-release}/$CEPH_CODENAME/" -e "s/{distro}/$CEPH_DISTRO/" /etc/yum.repos.d/ceph.repo

    yum install -y epel-release
    yum install -y git gcc make python36-devel python36-pip python36-libs python36-setuptools librados-devel librbd-devel

    export VENV_DIR=/usr/local/benji
    python3.6 -m venv $VENV_DIR
    . $VENV_DIR/bin/activate
    pip install --upgrade pip

    export CEPH_VERSION=14.2.2
    # The links are necessary as Ceph's setup.py searches for these names
    ln -s /usr/bin/python3.6-config $VENV_DIR/bin/python-config
    ln -s /usr/bin/python3.6m-x86_64-config $VENV_DIR/bin/python3.6m-x86_64-config
    pip install cython
    pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rados"
    pip install "git+https://github.com/ceph/ceph@v$CEPH_VERSION#subdirectory=src/pybind/rbd"

.. NOTE:: Benji has only been tested with Luminous and later versions of Ceph's Python bindings.
