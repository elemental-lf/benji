.. include:: global.rst.inc

Data Layout
===========

Benji works with two different categories of data: The metadata is stored
in a database backend and describes how the blocks fit together to form
a backup *version*. The actual blocks are saved in one or more object
storages.

Database Backend
----------------

The database backend is responsible for managing all metadata.

The database backend relies on SQLAlchemy, a Python ORM which works with a
huge number of DBMS, e.g. MySQL, PostgreSQL, SQLite3 and Oracle.

Benji has been developed and tested with PostgreSQL and SQLite3, so they are
the recommended database engines and you may encounter problems with other
databases. Patches to support other databases are welcome of course!

For Benji's purposes, you may use either PostgreSQL or SQLite3. For a single
workstation's backup with 10 to 20 versions, SQLite3 is perfectly suitable.
However you will benefit from PostgreSQL's performance and stability when
doing hundreds of versions with terabytes of backup data. A distributed
installation of Benji requires PostgreSQL to work and it is also the
recommended DBMS for production deployments.

You configure the location of your database with the ``databaseEngine``
directive. Please consult the `SQLAlchemy documentation <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_
for options and format.

All timestamps in the database are in UTC.

.. todo:: Document all tables

Object Storages
---------------

The object storage backend is pluggable and there are currently three different
implementations from which you can choose:

- file: File based storage
- s3: S3 compatible storage like AWS S3, Google Storage, Ceph's RADOS Gateway
  or Minio
- b2: Backblaze's B2 Cloud Storage

.. todo:: Document information about the actual data layout, encryption,
    compression and mention metadata accompanying objects.
