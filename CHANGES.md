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
