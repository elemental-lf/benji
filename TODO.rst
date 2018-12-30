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
* Write a new Makefile for build, test and release
* Write more tests
* Readd documentation for development setup
* Add tests for anything where scrub marks blocks as invalid (source changed,
  bit rot in backup, ...
* Add tests for CLI frontend
* io._reader, io.get could return checksum in block (block._replace)...
* Support for layering data backends to implement things like mirroring
* Native Google Storage backend
* Better NBD server performance (if possible)
* Deduplication in NBD fixate()?
* Make some more ASCIInema casts for the documentation
* Update and republish website (the thing in the website subdirectory)
* Add script to generate hints from LVM usage bitmaps for classic and thin snapshots
