.. include:: global.rst.inc

.. _scrubbing:

Scrub
=====

Scrubbing backups is needed to ensure data consistency over time.

Reasons for Scrubbing
---------------------

Benji divides the data you backup into blocks. These blocks are referenced
by the metadata stored in the database backend. When restoring images, these
blocks are read and restored to form the original image. As Benji also does
deduplication, an invalid block can potentially affect multiple versions and
so image backups.

Invalid blocks can occur for the following reasons (probably incomplete):

- Bit rot / data degradation (https://en.wikipedia.org/wiki/Data_degradation)
- Software failure when writing the block for the first time
- OS errors and bugs
- Human error: Deleting or modifying blocks by accident
- Software errors in Benji and other used tools

Scrubbing Methods
-----------------

Benji implements three different scrubbing methods. Each of these methods
accepts the ``--block-percentage`` (short form ``-p``) option. With it you
can limit the scrubbing to a randomly selected percentage of the blocks.

.. ATTENTION:: When using the ``--block-percentage`` option with a value of
    less than 100 percent with any of the deep scrubbing commands, an invalid
    *version* won't be marked as valid again, when it has been marked as
    invalid in the past. Only a full successful deep-scrub will do that.

Consistency and Checksum
~~~~~~~~~~~~~~~~~~~~~~~~

.. command-output:: benji deep-scrub --help

For each block in a version Benji reads the block's metadata (UID and
checksum) from the database backend, reads the actual block by its UID
from the storage, calculates its checksum and compares it to the
originally recorded checksum. If the checksums are not the same the
block is marked as invalid and won't be used for deduplication anymore.
All other versions which reference this block are also marked as invalid
as is the scrubbed version itself.

Using the Backup Source
~~~~~~~~~~~~~~~~~~~~~~~

::

    benji deep-scrub --source <snapshot> <version_uid>


In addition to the consistency and checksum checks Benji can also compare
the backup data to the original backup source by specifying the ``--source``
option. The comparison is done byte by byte. Although this is an additional
safeguard against data corruption it requires that the backup source is still
present and it produces additional load on the backup source.

Consistency Only
~~~~~~~~~~~~~~~~

.. command-output:: benji scrub --help

With this command Benji only checks the metadata consistency between the
metadata saved in the database and the metadata accompanying each block
on the storage. It also checks if the block exists and has the right length
as reported by the storage provider. The actual data is **not** checked in
this case.

This mode of operation can be a useful in addition to deep-scrubs if
you pay for data downloads from the storage provider or your bandwidth
is limited. It is not a replacement for deep-scrubs but you can reduce
their frequency.

Batch scrubbing
---------------

Benji also supports two commands to facilitate batch scrubbing of versions:
``benji batch-scrub`` and ``benji batch-deep-scrub``:

.. command-output:: benji batch-scrub --help
.. command-output:: benji batch-deep-scrub --help

Both can take a list of *version* names. All *versions* matching these
names will be scrubbed. If you don't specify any names all *versions*
will be checked.

If the ``--tag`` (short form ``-t``) is given too, the above  selection is
limited to  *versions* also matching the given tag.  If  multiple ``--tag``
options are given, then they constitute an OR  operation.

By default all matching *versions* will be scrubbed. But you can also
randomly select a certain sample of these *versions* with ``--version-percentage``
(short form``-P``). A *version's* size isn't taken into account when selecting the
sample, every *version* is equally eligible.

The batch scrubbing commands also accepts the ``--block-percentage`` (short
form ``-p``) option.

``benji batch-deep-scrub`` doesn't support the ``--source`` option like
``benji deep-scrub``.

This is a good use cause for tags: You could mark your *versions* with a list of
different tags denoting the importance of the backed up data. Then you could scrub
each class of *versions* differently::

    # 14% of the versions are deep scrubbed for data of high importance
    $ benji batch-deep-scrub --version-percentage 14 'labels["priority"] == "high"'

    # 7% of the versions are deep scrubbed for data of medium importance
    $ benji batch-deep-scrub --version-percentage 7 'labels["priority"] == "medium"'

    # 3% of the versions are deep scrubbed for data of low importance
    $ benji batch-deep-scrub --version-percentage 3 'labels["priority"] == "low"'

    # 3% of the versions are scrubbed when they contain reproducible scratch data or don't have a priority label
    $ benji batch-scrub --version-percentage 3 'labels["priority"] == "scratch" or not labels["priority"]'

If you'd call this schedule every day, you'd scrub the important data completely
about every seven days (statistically), data of medium importance completely every
fourteen days and low priority data completely every month. Scratch data would also
be scrubbed completely every month, but only metadata consistency and block
existence is checked.

Scrubbing Failures
------------------

If scrubbing finds invalid blocks, these blocks are marked as *invalid*
in the metadata store. However, such blocks **will persist** and not be deleted.

Also, the versions affected by such invalid blocks are marked *invalid*.
Such versions cannot be the base (i.e. ``benji backup -f``, see
:ref:`differential_backup`) for differential backups anymore, Benji will throw
an error if you try.

However, invalid versions **can still be restored**. So a single block will not
break the restore process. Instead, you'll get a clear log output that there
is invalid data restored.

You can find invalid versions by looking at the output of ``benji ls``::

    $ benji  ls
        INFO: $ benji ls
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    |         date        |     uid     | name | snapshot_name |     size | block_size | valid | protected | tags |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+
    | 2018-06-07T12:51:19 | V0000000001 | test |               | 41943040 |    4194304 | False |   False   |      |
    +---------------------+-------------+------+---------------+----------+------------+-------+-----------+------+


.. NOTE:: Multiple versions can be affected by a single block as Benji does
    deduplication and one block can belong to multiple versions, even to
    different images.



