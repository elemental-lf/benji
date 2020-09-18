.. include:: global.rst.inc
.. _configuration:

Configuration
=============

Configuration File Location
---------------------------

Benji will by default search the following locations for configuration files:

* /etc/benji.yaml
* /etc/benji/benji.yaml
* ~/.benji.yaml
* ~/benji.yaml

If multiple of these files exist, only the first file found is read.

In order to explicitly pass a configuration file, use the ``-c`` (or
``--configfile``) parameter.


Top-level configuration directives
----------------------------------

* key: **configurationVersion**
* type: string (integer is also accepted)
* required

Currently this is always ``1``.

* key: **logFile**
* type: string
* default: ``null``

Benji will by default log INFO, WARNING and ERROR to this file. If you also
need DEBUG information, please start Benji with ``--log-level DEBUG``.
Setting this to null disables logging to a file, this might be useful when
running Benji inside a container.

* key: **blockSize**
* type: integer
* unit: bytes
* default: ``4194304`` (4MiB)

The block size can be changed on the command line on a version by version
basis, but be aware that this will affect deduplication and increase the
space usage. One possible use case for different block sizes would be backing
up LVM volumes and Ceph images with the same Benji installation. While for Ceph
4MiB is usually the best size, LVM volume might profit from a smaller block size.

* key: **hashFunction**
* type: string
* default: ``BLAKE2b,digest_bits=256``

Hash function to use for calculating block checksums. There is normally no reason
to change the default. **Do not change this setting when backups already exist.**

* key: **processName**
* type: string
* default : ``benji``

This name will be used to identify a specific instance of Benji in the process list
and  can be used to distinguish several parallel installations.

* key: **disallowRemoveWhenYounger**
* type: integer
* default: ``6``

This settings disallows removal of backup versions if they are younger than the
specified number of days. Set to 0 to disable, i.e. to be able to delete any
backup version regardless of its age.

* key: **databaseEngine**
* type: string
* required

See https://docs.sqlalchemy.org/en/latest/core/engines.html for options.
Only PostgreSQL (dialect psycopg2) and SQLite 3 are tested with during development.


* key: **ios**
* type: list of dictionaries
* required

List of I/O configurations. Backup sources are accessed via these configurations.
They are also used as a destination during restore operations. See below.

* key: **storages**
* type: list of dictionaries
* required

List of storage configurations used for storing backup versions. See below.

* key: **defaultStorage**
* type: string
* required

Default storage for storing backup versions. Reference to a storage name.

* key: **transforms**
* type: list of dictionaries
* default: empty list

List of data transformation configurations. See below.

* key: **nbd**
* type: dictionary
* default: see below

Configuration options pertaining to Benji's NBD server.

List of I/O Configurations
--------------------------

The list of I/O configurations (**ios**) is a list of dictionaries with the
following keys:

* key: **name**
* type: string
* default: none

This sets the name of this I/O configuration entry. It is used as the
scheme in backup source or restore destination specifications.

* key: **module**
* type: string
* default: none

Reference to a I/O module name. See below.

* key: **configuration**
* type: list of dictionaries
* default: ``null``

Module specific configuration for this I/O configuration entry.

List of Transform Configurations
--------------------------------

The list of transform configuration (**transforms**) is a list of
dictionaries with the following keys:

* key: **name**
* type: string
* default: none

This sets the name of this transform configuration entry. It is
referenced in the list of **activeTransforms**.

* key: **module**
* type: string
* default: none

Reference to a transform module name. See below.

* key: **configuration**
* type: list of dictionaries
* default: ``null``

Module specific configuration for this transform configuration entry.

List of Storage Configurations
------------------------------

The list of storage configurations (**storages**) is a list of dictionaries
with the following keys:

* key: **name**
* type: string
* default: none

This sets the name of this storage configuration entry. It is referenced by
the **defaultStorage** top-level configuration directive or it is specified
on the command line.

* key: **module**
* type: string
* default: none

Reference to a storage module name. See below.

* key: **storageId**
* type: integer
* default: none

This sets the internal storage id for this storage configuration. It is no longer necessary to populate
this configuration key as the storage id is now assigned automatically. This option only exists for
backward compatibility with older configurations and should not be used in new configurations.

* key: **configuration**
* type: list of dictionaries
* default: ``null``

Module specific configuration for this storage configuration entry.

I/O Modules
------------

* name: **simultaneousReads**
* type: integer
* default: ``1``

Number of reader threads when reading from a backup source. Also
affects the internal read queue length. It is highly recommended to
increase this number to get better concurrency
and performance.

* name: **simultaneousWrites**
* type: integer
* default: ``1``

Number of writer threads when restoring a version. Also affects the internal write queue length. It is highly
recommended to increase this number to get better concurrency and performance.

I/O Module file
~~~~~~~~~~~~~~~~

The ``file`` I/O module supports the following configuration options:

* name: **simultaneousReads**
* type: integer
* default: ``1``

Number of reader threads when reading from a backup source. Also
affects the internal read queue length. It is highly recommended to
increase this number to get better concurrency and performance.

* name: **simultaneousWrites**
* type: integer
* default: ``1``

Number of writer threads when restoring a version. Also affects the internal write queue length. It is highly
recommended to increase this number to get better concurrency and performance.

I/O Module rbd
~~~~~~~~~~~~~~

The ``rbd`` I/O module requires that Ceph's Python modules ``rados`` and ``rbd`` are installed. With it it is
possible to backup RBD images of a Ceph cluster. The supported URL syntax is::

    <module instance>:<pool>/<image>
    <module instance>:<pool>/<image>@<snapshot>

Additional options can be passed as query parameters:

- ``client_identifier``: Overrides the ``clientIdentfier`` from the module instance's configuration
- ``mon_host``: Comma separated list of monitor hosts, either host names or IP addresses, optionally with a port number
  (``<host>:<port>``)
- ``key``: Verbatim Cephx key
- ``keyring``: Path to a CephX keyring file
- any other Ceph configuration options

Full example::

    rbd:pool/image@snapshot?client_identifier=guest&mon_host=10.0.0.1:6789,10.0.0.2:6789&key=XXXXXXXX

This module supports the following configuration options:

* name: **simultaneousReads**
* type: integer
* default: ``1``

Number of reader threads when reading from a backup source. Also
affects the internal read queue length. It is highly recommended to
increase this number to get better concurrency
and performance.

* name: **simultaneousWrites**
* type: integer
* default: ``1``

Number of writer threads when restoring a version. Also affects the internal write queue length. It is highly
recommended to increase this number to get better concurrency and performance.

* name: **cephConfigFile**
* type: string
* default: ``/etc/ceph/ceph.conf``

Sets the path to the Ceph configuration file used by this I/O configuration.

* name: **clientIdentifier**
* type: string
* default: ``admin``

Sets the name of the client identifier used by this I/O configuration to
access the Ceph RBD service.

* name: **newImageFeatures**
* type: list of strings
* default: none, required

Valid values for this list are extracted from the installed Ceph RBD
libraries. For recent version of Ceph this list of possible image
features applies: ``RBD_FEATURE_LAYERING``, ``RBD_FEATURE_EXCLUSIVE_LOCK``,
``RBD_FEATURE_STRIPINGV2``, ``RBD_FEATURE_OBJECT_MAP``,
``RBD_FEATURE_FAST_DIFF``, ``RBD_FEATURE_DEEP_FLATTEN``.

I/O Module rbdaio
~~~~~~~~~~~~~~~~~

The ``rbdaio`` I/O module requires that Ceph's Python modules ``rados`` and ``rbd`` are installed. The configuration
options are identical to the ``rbd`` module but instead of using multiple threads the asynchronous I/O support of
``librbd`` is used to access the images and snapshots. This module is relatively new and has not seen much testing.
Some non-methodical testing shows that the performance is about 10 to 20 percent better than the ``rbd`` module. It
is planned that this module is going to replace the ``rbd`` module in the future.

I/O Module iscsi
~~~~~~~~~~~~~~~~

This I/O module requires a special version of the ``libiscsi`` Python bindings available at
https://github.com/elemental-lf/libiscsi-python. It currently has some limitations:

* Single-threaded synchronous execution of iSCSI commands limiting achievable performance
* No usage of ``GET_LBA_STATUS`` to detect unmapped regions

The following configuration options are supported:

* name: **username**
* type: string
* default: none

Sets the CHAP username for iSCSI authentication and authorization. If this directive is not set,
no authentication is attempted.

* name: **password**
* type: string
* default: none

Sets the CHAP password for iSCSI authentication and authorization.

* name: **targetUsername**
* type: string
* default: none

Sets the CHAP username for iSCSI target authentication. If this directive is not set, no authentication
of the target is done.

* name: **targetPassword**
* type: string
* default: none

Sets the CHAP password for iSCSI target authentication.

* name: **headerDigest**
* type: string
* default: ``NONE_CRC32C``

Sets the header digest order advertised during negotiation. Possible values are: ``NONE``, ``NONE_CRC32C``,
``CRC32C_NONE``, and ``CRC32C``.

* name: **initiatorName**
* type: string
* default: ``iqn.2019-04.me.benji-backup:benji``

Sets the initiator name.

* name: **timeout**
* type: string
* default: ``0`` meaning unlimited

Sets the iSCSI timeout.

Transform Modules
-----------------

The name given to a transform configuration is saved together with the data
object to the storage. This means that you must be careful when changing the
configuration after a transform has been used by a storage. For example
you can't just change the encryption key because you will lose access to
all data objects already encrypted with the old key. But you can create
a new encryption configuration with a new name and use it instead in
the list of **activeTransforms** of your storage. All newly create data
objects will then use this new transform. Old objects will still be
encrypted with the old key but they can still be decrypted as long as
the old encryption configuration is available under the old name. Key
rollover is not yet implemented.

It is harmless to change the compression level. But this change will only
affect newly create data objects.

Transform Module zstd
~~~~~~~~~~~~~~~~~~~~~

The ``zstd`` module supports the following configuration
options:

* name: **level**
* type: integer
* default: none, required

The compression level used for compressing blocks of data.

* name: **dictDataFile**
* type: string
* default: none

Sets the path to a zstandard dictionary. This option has limited value in the
context of Benji and shouldn't be set.


Transform Module aes_256_gcm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module encrypts each data block with its own randomly generated key
by using AES-256 in GCM mode. The random key is then encrypted with
a master key by using the key wrapping algorithm specified in RFC 3394
and then saved beside the data block.

The ``aes_256_gcm`` module supports the following configuration
options:

* name: **kdfSalt**
* type: binary string encoded with BASE64
* default: none

Sets the salt for the key derivation function.

* name: **kdfIterations**
* type: integer
* default: none

Sets the number of iterations for the key derivation function.

* name: **password**
* type: string
* default: none

Sets the password from which the master key is generated.

* name: **masterKey**
* type: binary string with a length of 32 bytes encoded with BASE64
* default: none

Sets the master key used for encrypting the envelope keys. This key should
have a high entropy. In most cases it is safer and easier to derive
the key from a **password**.

The **masterKey** configuration directive is mutually exclusive
to the other three directives.

When the **masterKey** directive is not set the master key is derived from
the other three configuration directives by using PBKDF2 with SHA-512.

Regarding kdfSalt and kdfIterations: It is highly recommended to generate
your own random salt and chose your own number of iterations. Don't change
the salt and iteration count after writing encrypted data objects,  they
cannot be decrypted anymore.


Transform Module aes_256_gcm_ecc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module encrypts each data block using the ``aes_256_gcm`` module
(see above). Instead of specifying a symmetric master key, the encryption
key is encrypted asymmetrically using elliptic curve cryptography. The
implementation is based on the algorithm outlined in the ebook
`Practical Cryptography for Developers <https://cryptobook.nakov.com/asymmetric-key-ciphers/ecc-encryption-decryption>`_.

The idea of this module is that the configuration used for backup does not
need to include the private key of the ECC key pair and so cannot decrypt any
of the data once it has been stored. For restore and deep-scrubbing operations
the private key is still needed but this key can be confined to a separate
specifically secured installation of Benji.

The ``aes_256_gcm_ecc`` module supports the following configuration options:

* name: **eccKey**
* type: BASE64-encoded DER representation of a ECC key (public or private)
* default: none

Specify the encryption keyfile to encrypt a block's symmetric key.
For backup (encryption-only) this should be the public key only.
For restore (decryption) this must include the private key.

* name: **eccCurve**
* type: string
* default: ``NIST P-384``

The ECC curve to use. Valid values are 'NIST P-256', 'NIST P-384' and 'NIST P-521'.

Example python code to create a valid ECC key for **eccKey** (using ``PyCryptodome``)::

    from base64 import b64encode
    from Crypto.PublicKey import ECC
    key = ECC.generate(curve='NIST P-384')
    public_key = b64encode(key.public_key().export_key(format='DER', compress=True)).decode('ascii')
    private_key = b64encode(key.export_key(format='DER', compress=True)).decode('ascii')


Storage Modules
---------------

All storage modules support the following configuration directives:

* name: **simultaneousReads**
* type: integer
* default: ``1``

Number of reader threads when reading from a storage. Also affects the internal read queue length. It is highly
recommended to increase this number to get better concurrency and performance.

* name: **simultaneousWrites**
* type: integer
* default: ``1``

Number of writer threads when writing to a storage. Also affects the internal write queue length. It is highly
recommended to increase this number to get better concurrency and performance.

* name: **simultaneousRemovals**
* type: integer
* default: ``1``

Number of removal threads when removing blocks from a storage. Also affects the internal queue length. It is highly
recommended to increase this number to get better concurrency and performance.

* name: **bandwidthRead**
* type: integer
* unit: bytes per second
* default: ``0``

This limits the number of bytes read from the storage by second using a token
bucket algorithm. A value of ``0`` disables this feature.

* name: **bandwidthWrite**
* type: integer
* unit: bytes per second
* default: ``0``

This limits the number of bytes written to the storage by second using a token
bucket algorithm.  A value of ``0`` disables this feature.

* name: **activeTransforms**
* type: list of strings
* default: empty list

Sets a list of transformations which are applied to each data object before it is
written to the storage. The transformations are performed in order from first to
last when writing to the storage. On read the list of transformations recorded in
the object's metadata is used in reverse order to decode the data. This makes
it possible to change the list of ``activeTransforms`` and so enable compression or
encryption even when there are data objects present in a storage already. The changed
transformation list will only be applied to new data objects.

* name: **consistencyCheckWrites**
* type: bool
* default: ``false``

When this option is set to ``true`` then each write to the storage is followed
by a read checking the data integrity of the written object data and metadata.
This is intended to by used when developing new storage modules and should be
disabled during normal use as it reduces the performance significantly.

HMAC
~~~~

The metadata for each data object in a storage is written to a separate object 
accompanying it. This metadata as whole is not encrypted. To protect against
metadata corruption or malicious manipulation an object's metadata can be
protected by a HMAC (Hash-based Message Authentication Code). Benji's
implementation conforms to RFC 2104 and uses SHA-256 as the hash algorithm.

* name: **hmac**
* type: dictionary
* default: none

The **hmac** dictionary supports the following keys:

* name: **kdfSalt**
* type: binary string encoded with BASE64
* default: none

Set ths salt for the key derivation function.

* name: **kdfIterations**
* type: integer
* default: none

Sets the number of iterations for the key derivation function.

* name: **password**
* type: string
* default: none

Sets the password from which the key is generated.

* name: **key**
* type: binary string encoded with BASE64
* default: none

Sets the key used for seeding the hash function. In most cases it is safer and
easier to derive the key from a **password**.


The **key** configuration directive is mutually exclusive to the other
three directives.

When the **key** directive is not set the key is derived from the other
three configuration directives by using PBKDF2 with SHA-512.

Read Cache
~~~~~~~~~~

Benji supports a read cache for all storage modules. The read cache can be
be beneficial when frequently restoring images that are mostly identical.

* name: **readCache**
* type: dictionary
* default: none

The **readCache** dictionary supports the following keys:

* name: **directory**
* type: string
* default: none

Sets the directory used by the cache.

* name: **maximumSize**
* type: integer
* unit: bytes
* default: none

Maximum size of the cache in bytes.

* name: **shards**
* type: integer
* default: none

Sets the number of cache shards. Needs to be scaled together with
**simultaneousReads**.

Storage Module file
~~~~~~~~~~~~~~~~~~~

The ``file`` storage module supports the following configuration options:

* name: **path**
* type: string
* required

Sets the path to a directory where backup version data is stored.

Storage Module s3
~~~~~~~~~~~~~~~~~

* name: **awsAccessKeyId**
* type: string
* one of **awsAccessKeyId** or **awsAccessKeyIdFile** required

Sets the access key id. This option and the **awsAccessKeyIdFile**
option are mutually exclusive.

* name: **awsAccessKeyIdFile**
* type: string
* one of **awsAccessKeyId** or **awsAccessKeyIdFile** required

Sets the access key id from a file. This option and the **awsAccessKeyId**
option are mutually exclusive.

* name: **awsSecretAccessKey**
* type: string
* one of **awsSecretAccessKey** or **awsSecretAccessKeyFile** required

Set the access key. This option and the **awsSecretAccessKeyFile**
option are mutually exclusive.

* name: **awsSecretAccessKeyFile**
* type: string
* one of **awsSecretAccessKey** or **awsSecretAccessKeyFile** required

Sets the access key from a file.This option and the **awsSecretAccessKey**
option are mutually exclusive.

* name: **bucketName**
* type: string
* required

Sets the bucket name.

* name: **regionName**
* type: string
* default: from ``boto3`` library, ignored if **endpointUrl** is specified

Sets the region of the bucket.

* name: **useSsl**
* type: bool
* default: from ``boto3`` library, ignored if **endpointUrl** is specified

If not set, the default of the underlying ``boto3`` library is used.
When this option is set to ``true`` then TLS is used to connect to
the S3 API endpoint. When it is set to ``false`` HTTP is used.

* name: **endpointUrl**
* type: string
* default: none

If not set, the default of the underlying ``boto3`` library is used.
This option sets the S3 API endpoint to use in URL format. If it is
specified other options like **regionName** and **useSsl** are ignored
by the underlying ``boto3`` library. This needs to be set to
``https://storage.googleapis.com/`` when connecting to a Google Storage
bucket.

* name: **addressingStyle**
* type: string
* default: none

If not set, the default of the underlying ``boto3`` library is used.
Valid values are ``path`` and ``host``. This needs to be set to ``path``
when connecting to a Google Storage bucket.

* name: **signatureVersion**
* type: string
* default: none

If not set, the default of the underlying ``boto3`` library is used. Valid
values are ``s3`` for version 2 signatures and  ``s3v4`` for version 4
signatures.

* name: **disableEncodingType**
* type: bool
* default: False

Some S3 compatible endpoints generate errors when an encoding type is set
during some operations. Enabling this setting prevents this by not sending
this HTTP header. This needs to be set to ``true`` when connecting
to a Google Storage bucket.

Storage Module b2
~~~~~~~~~~~~~~~~~~

* name: **accountId**
* type: string
* one of **accountId** or **accountIdFile** required

Set the account id. This option and the **accountIdFile**
option are mutually exclusive.

* name: **accountIdFile**
* type: string
* one of **accountId** or **accountIdFile** required

Sets the account id from a file. This option and the **accountId**
option are mutually exclusive.

* name: **applicationKey**
* type: string
* one of **applicationKey** or **applicationKeyFile** required

Sets the application key. This option and the **applicationKeyFile**
option are mutually exclusive.

* name: **applicationKeyFile**
* type: string
* one of **applicationKey** or **applicationKeyFile** required

Sets the application key from a file. This option and the **applicationKey**
option are mutually exclusive.

* name: **accountInfoFile**
* type: string
* default: none

Sets the file for caching the authorization token.  If unset, the `b2` module
will always authorize the user with the provided account id and application
key.  But the latter operation is rate limited by BackBlaze, so if Benji is
invoked repeatably in a short time frame the login will fail. In this case
configure  this option to cache and use the authorization token in subsequent
calls of  Benji. The token will be renewed automatically when it expires.

* name: **bucketName**
* type: string
* required

Sets the bucket name.

* name: **uploadAttempts**
* type: integer
* default: ``5``

Sets the number of upload attempts made by the underlying ``b2`` library.

* name: **writeObjectAttempts**
* type: integer
* default: ``3``

Sometimes the b2 API shows transient errors during object writes. Benji will
retry writes this number of times.

* name: **readObjectAttempts**
* type: integer
* default: ``3``

Sometimes the b2 API shows transient errors during object reads. Benji will
retry reads this number of times.

NBD
---

Configuration options pertaining to Benji's NBD server are located under the top-level key **nbd**.
While the defaults are suitable for testing they are not suitable for a production use of the NBD server
functionality as ``/tmp`` generally is RAM disk today.

The block cache is bounded by the maximum size setting. The copy-on-write store is only used if data
is written to an NBD device and a copy-on-write *version* is created. It holds all the blocks that have
changed and will be cleaned up when the NBD device is detached and all the changed blocks have been
written into the COW *version*. This means that it can grow up to the size of all *versions* that
are attached to an NBD device in read-write mode at a time.

Benji's NBD server is not protected by any form of authentication and the traffic between the NBD server
and client is not encrypted. That is why the NBD server only listens on ``localhost`` by default.

* name: **blockCache.directory**
* type: string
* default: ``/tmp/benji/nbd/block-cache``

Sets the base directory for the block cache

* name: **blockCache.maximumSize**
* type: integer
* default: ``2126512128``

Sets the approximate maximum size of the block cache. The default is 2GB.

* name: **cowStore.directory**
* type: string
* default: ``/tmp/benji/nbd/cow-store``

Sets the base directory for the copy-on-wrote storage area.

Multiple Instance Installations
-------------------------------

You can run Benji multiple times on different machines or in different
containers simultaneously. The configurations will have to match.
This is the responsibility of the user and isn't checked by Benji!  Be
careful to shutdown all instances before making configuration changes that
could affect other instances (like adding an encryption key).

Multiple instances open up the possibility to scale-out Benji for
performance reasons, to put instances where the backup source data is or to
have a dedicated instance for restores for example.

Locking between different instances is done via the database backend.
