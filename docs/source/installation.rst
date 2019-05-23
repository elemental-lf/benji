.. include:: global.rst.inc
.. _installation:

Installation
============

Currently there are no pre-built packages but you can easily install Benji
with ``pip``. Furthermore there are two Docker images which each contain
a complete Benji installation. The ``benji`` image is the easiest and fastest
way to test Benji but can also be used in production. See section :ref:`container`
for more information about the images.

Ubuntu 16.04
------------

This version of Ubuntu doesn't have a current Python installation. But Python 3
can be installed via private repository::

    apt-get update
    apt-get install --no-install-recommends software-properties-common python-software-properties
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get --no-install-recommends python3.6 python3.6-venv python3.6-dev git gcc

.. NOTE:: For more information about this Personal Package Archive (PPA)
    please see https://launchpad.net/~deadsnakes/+archive/ubuntu/ppa.

RHEL/CentOS 7
-------------

A recent version of Python 3 is included in the EPEL repository::

    yum install -y epel-release
    yum install -y python36-devel python36-pip python36-libs python36-setuptools

Fedora 27 and up
----------------

The distribution includes a supported version of Python 3. Make sure to install the latest available update.

openSUSE
--------

The distribution includes a supported version of Python 3.
Benji is also available through the official repositories of openSUSE Tumbleweed.
For older, versioned releases of openSUSE you need to add the devel project::

    zypper ar obs://Archiving:Backup ab-benji
    zypper ref

Then you can install it via::

    zypper in benji

Common to All Distributions
---------------------------

After installing a recent Python version, it is now time to install
Benji and its dependencies::

    # Create new virtual environment
    python3.6 -m venv /usr/local/benji
    # Activate it (your shell prompt should change)
    . /usr/local/benji/bin/activate
    # Alternative A: Install a specific released version from PyPI (0.4.0)
    pip install benji==0.4.0
    # Alternative B: Install the latest released version from PyPI
    pip install benji
    # Alternative C: Install the latest version from the master branch of the Git repository
    pip install git+https://github.com/elemental-lf/benji

If you want to use certain features of Benji you need to install additional
dependencies:

- ``s3``: AWS S3 object storage support
- ``b2``: Backblaze's B2 Cloud object storage support
- ``compression``: Compression support
- ``readcache``: Disk caching support

You can do this be specifying a comma delimited list of extra features in square brackets after the package URL::

    pip install 'benji==0.2.0[compression,s3,readcache,b2]'

To upgrade an existing installation use the same command line but add the ``--upgrade`` option.

.. NOTE:: It is recommended to install and use the compression feature for
    almost all use cases.
