.. include:: global.rst.inc

Storage Statistics
==================

Storage Statistics
------------------

.. command-output:: benji storage-stats --help

This command outputs the number of objects and their space usage. It will take some time as it enumerates all
objects in the storage.

Space Usage Statistics
----------------------

.. command-output:: benji storage-usage --help

This command provides space usage statistics over all or a subset of versions::

    INFO: $ /home/lf/src/backy2/venv/bin/benji storage-usage
    +-----------+---------+--------+--------+-----------+------------------------+
    | storage   | virtual | sparse | shared | exclusive | deduplicated_exclusive |
    +-----------+---------+--------+--------+-----------+------------------------+
    | storage-1 | 12.7MiB | 1.0MiB |   0.0B |   11.7MiB |                 6.2MiB |
    +-----------+---------+--------+--------+-----------+------------------------+

Answering the question of how much space a number of versions occupy on the storages is inherently hard to do due
to the storage-wide deduplication that Benji employs. But this command tries to answer this question at least in part.
If called without any arguments the statistics are calculated over all versions. But in general a filter expression
should be used to limit the number of versions. Calculating these statistics is an expensive operation and will take
some time if the number of versions and blocks is large.

* **virtual**: This number is the sum of the sizes of all matching versions. It is virtual in the sense that it does
  not represent the actual space usage on the storage.

* **sparse**: This is the number of bytes that are sparse. Sparse blocks do not take up any space.

* **shared**: This value represents the number of bytes that the matched subset of versions shares with other
  versions which are not part of this set.

* **exclusive**: These are the number of bytes that are exclusive to this subset. If a block is used
  more than once in this subset of versions then it is counted multiple times towards this figure.

* **deduplicated_exclusive**: This is like **exclusive** but only counts each distinct block on the storage once. It
  is an estimate of the space that would be freed if all matching versions would be deleted.

* The sum of **sparse**, **shared** and **exclusive** equals **virtual**.
