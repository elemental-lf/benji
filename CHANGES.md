## 0.6.0 (Kubecon Barcelona Edition), 22.05.2019

Notable changes:

* URL parsing of I/O resources is now conforming to standards. Especially for the RBD I/O module the two slashes
  directly after the colon are no longer valid and have to be removed (`rbd://pool/image` -> `rbd:pool/image`).

* Added I/O module for iSCSI. It is based on `libiscsi` and requires no elevated permissions. Please see the 
  documentation as Benji requires a special version of the `libiscsi` Python bindings. The module is single-threaded
  and synchronous, so performance will be limited. Contributions are welcome!
  
* The algorithm used by `benji enforce` has seen an overhaul and should be more comprehensible as the time categories
  are based on natural time boundaries (start of the hour, day, week, month, and year) now.
  
* Added a restore helper script (`images/benji-k8s/scripts/benji-restore-pvc`) for Kubernetes. This script is intended
  to be run on a management system with access to the Kubernetes cluster and can restore a version into a new or
  an existing PVC/PV pair.
  
* The container images are now based on the Python 3.6 included in EPEL. The RBD support has been updated to Ceph
  Nautilus. Nautilus also added RADOS and RBD Python bindings for Python 3.6 which are now used instead of building
  them themselves.

## v0.5.0, 02.04.2019

Notable changes:

* Added `fsfreeze` support to the `benji-k8s` Docker image. Just add the `benji-backup.me/fsfreeze: yes` annotation to
  the PVC. Kubernetes hosts are accessed via pods which are deployed by a DaemonSet, see the Helm chart for details.

* Use bulk inserts to speed up backups of images based on a previous version. This also decreases memory usage.
  
* Switched from in-memory block lists to an iterator based approach. This will increase performance and decrease
  memory usage when backing up large images.
   
* Fixed a wrong index on the `blocks` table. This should also increase performance. The database will need to be
  migrated with `benji database-migrate`.

* Laid the foundation for structured logging.

* Removed database table `stats` and assorted code and commands. Statistics are now kept together with the other
  version metadata in the `versions` table. This means they are also removed when the version is removed. If
  you want to keep historic statistics you need to export them beforehand with `benji -m ls` or 
  `benji metadata-export`. This is a breaking change and you might need to adjust your scripts. As statistics
  are now included in a version's metadata the metadata version has changed to `1.1.0`. Old metadata backups
  and exports with a metadata version of `1.0.0` can be imported by the current  version. The statistics will
  be empty in that case. The database will need to be migrated with `benji database-migrate`.
 
* Fixed a bug in the time calculation of `benji enforce` which could lead to a late expiration of versions,
  the timing was a few hours off.

I'd like to thank @olifre and @adambmedent for their testing efforts!

## v0.4.0, 20.03.2019

Notable changes:

* Documentation updates
* Added new CLI command `benji storage-stats` to get storage usage information
* Fixed backup progress reporting on console
* Added `fdatasync()` calls to the `file` module to ensure backup integrity in case of a system crash or a 
  power failure (will impact performance when using the `file` I/O module)
* Speed up command line completion
* Added `benji completion` CLI command

## v0.3.1, 25.02.2019

Fixes a naming problem with the `--override-lock` CLI option of `benji rm` and `benji cleanup`

## v0.3.0, 25.02.2019

Notable changes:

* Restores are now multi-threaded just like backups. This should speed things up quite a bit.

* Multi-threaded removal of blocks was implemented. This should speed up `benji cleanup` with B2. Due to a 
  simplification in the code `benji cleanup` for S3 based storages is probably slower than before. Try increasing 
  `simultaneousRemovals` in your configuration if you're affected by this. If this doesn't help, please 
  open an issue.
  
* The backup scripts got another major overhaul: 

  * It is now possible to hook into strategic points in the backup process with custom `bash` functions. 
  * Prometheus metrics were reimplemented with these hooks and are now specific to the `benji-k8s` Docker image.
  * The foundations for freezing the filesystem before a Ceph snapshot were laid.
  * The scripts in the `benji-k8s` Docker image now generate Kubernetes events about backup failure or
    success. These events are attached to the affected PersistentVolumeClain and can for example be
    viewed with `kubectl describe pvc`.
  * `benji-backup-pvc` was renamed to `benji-pvc-backup`.  
  * Support for the try/catch construct based on the `bash-oo-framework` was removed. It had limitations
    and was causing problems with certain IDEs.
  
* The default for simultaneous read and writes was increased from one to three to get better out-of-the-box
  performance. The default for the simultaneous removals was set to five.  

* Left over locks due to power outages are similar events can now be overridden. This applies to `benji rm` and
  `benji cleanup`.
  
* A typo was fixed in the `metadata-backup` command. It is now correctly spelled `metadata-backup` and
  not `netadata-backup` as before. User action might be required if you're using this command in your scripts.

* A bug was fixed where blocks where not properly cleaned up when multiple storages were in use.
  
* The documentation was updated, but we're still not up-to-date at all fronts. 

## v0.2.0, 01.02.2019

Notable changes:

* Fix a big locking design problem.
* Convert `valid` boolean flag into a `status` field in versions table. This solves a problem where incomplete 
  backups where marked as valid by a `deep-scrub`. The new status field can be queried in filter expressions 
  like so: `status == "valid"`. Third party scripts might need adjustments.
* All binary keys and salts in the configuration file are now BASE64 encoded instead of using the `!!binary` 
  YAML extension. This is for better compatibility with 3rd party tools like Helm. User action needed.
* Implement proper handling of datetime columns  in filter expressions. Things like `date < "1 day ago" ` 
  are now possible.
* Make database migrations work properly again. Database migrations are now an explicit process and not automatic 
  anymore. Use the new `database-migrate` command.
* When restoring to an existing RBD image discard all existing data before the restore so as not to use more 
  space then needed.
* Increase standards conformance of Benji's NBD server (a little bit).
* Convert read cache to use a sharded design to increase scalability.
* Optimize database access pattern when backing up an image with a lot of sparse blocks.
* Optimize database commit handling.
* Implement workaround for SQLite database write access contention problem. Please use PostgreSQL if you're 
  planning on having multiple Benji processes running at the same time, SQLite wasn't designed for this.

A big thank you goes to @olifre for his extensive testing efforts and his valuable feedback!

## v0.1.1, 02.01.2019

## v0.1.0, 02.01.2019
