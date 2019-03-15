.. include:: global.rst.inc

Cleanup
=======

In order to remove of old backup versions, Benji uses a two step process.

Removing old versions
---------------------

In order to remove an old version, use ``benji rm``:

.. command-output:: benji rm --help

Example::

    $ benji rm -f V1
        INFO: $ benji rm -f V1
        INFO: Removed version V0000000001 metadata from backend storage.
        INFO: Removed backup version V0000000001 with 10 blocks.

Versions can only be removed if they are older than the number of days configured with the ``disallowRemoveWhenYounger``
option::

    $ benji rm V1
           INFO: $ benji rm V1
          ERROR: Version V0000000001 is too young. Will not delete.

It is possible to force the removal of a version by using ``--force``.

``benji rm`` removes the version's metadata and corresponding block list from the database. It also adds the removed
block entries into a deletion candidate list. By default it also removes the backup of the version's metadata on
the storage. If you want to keep this data, you can use the ``-k`` or ``--keep-metadata-backup`` option.

``benji rm`` only affects the database. To actually delete unused blocks on the storages a ``benji cleanup`` is
required.

Cleanup
-------

To free up space on the storage, you need to cleanup.
There are two different cleanup methods, but you'll usually only need the
so-called *fast-cleanup*.

.. command-output:: benji cleanup --help

``benji cleanup`` will go through the list of deletion candidates and check if
there are blocks which aren't referenced from any other version anymore.

These blocks are then deleted from the storage. The still-in-use blocks are removed from the list of candidates.
Due to fact that Benji needs to prevent a race-conditions between removing a block completely and referencing this
block from another version ``benji cleanup`` will only remove data blocks once they're on the list of deletion
candidates for more than one hour.
