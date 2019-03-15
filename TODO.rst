TODOs
=====

Probably soonish
----------------

* Perform fsfreeze as part of script framework
  (see https://gitlab.com/costrouc/kubernetes-rook-backup/blob/master/rook-backup.py#L115)

Unsorted collection
-------------------

* Finish key rotation support
* Reintroduce Debian packaging
* Write more tests
* Readd documentation for development setup
* Add tests for anything where scrub marks blocks as invalid (source changed, bit rot in backup, ...)
* Add tests for CLI frontend
* Support for layering data backends to implement things like mirroring
* Native Google Storage backend
* Better NBD server performance (if possible)
* Deduplication in NBD fixate()?
* Make some more ASCIInema casts for the documentation
* Update and republish website (the thing in the website subdirectory)
* Add script to generate hints from LVM usage bitmaps for classic and thin snapshots
* Write an version metadata overview (listing of all versions) to the storage to facilitate disaster recovery
* Remove ``--include-blocks`` from ``benji ls``, ``benji metdata-export`` already does this
