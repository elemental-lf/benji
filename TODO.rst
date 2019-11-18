TODOs
=====

Unsorted collection
-------------------

* Finish key rotation support
* Write more tests
* Add tests for anything where scrub marks blocks as invalid (source changed, bit rot in backup, ...)
* Add tests for CLI frontend
* Support for layering data backends to implement things like mirroring
* Native Google Storage backend
* Deduplication in NBD fixate()?
* Make some more ASCIInema casts for the documentation
* Update and republish website (the thing in the website subdirectory)
* Add script to generate hints from LVM usage bitmaps for classic and thin snapshots
* Write an version metadata overview (listing of all versions) to the storage to facilitate disaster recovery
* Kubernetes operator
* Improve sparse block handling by not representing them in the database
* Improve NBD block cache (removal of cache entries, size limit)
