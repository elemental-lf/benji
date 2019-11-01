.. include:: global.rst.inc

*Version* Metadata
==================

At this stage Benji has backed up all image data to a (hopefully) safe place. However, the blocks are of no use without
the corresponding metadata. Benji will need this information to get the blocks back in the correct order and
restore the image.

This information is stored in the database. Additionally Benji will backup the metadata itself to the storage
automatically. Should you lose your database backend, you can restore these metadata backups by using
``benji metadata-restore``.

.. command-output:: benji metadata-restore --help

There is currently no mechanism to import the metadata backups of all *versions* from the storage, but you could get a
list of all metadata backups on a specific storage with ``benji metadata-ls``.

.. command-output:: benji metadata-ls --help

.. NOTE:: This metadata backup is compressed and encrypted like the blocks
    if you have these features enabled.

It is also possible to export extra metadata backups with ``benji metadata-export``.

.. command-output:: benji metadata-export --help

By default ``benji metadata-export`` exports to standard output. But it can also write directly into a file by
specifying the ``--output-file`` option.

::

    $ benji --log-level ERROR metadata-export 'uid == "V1"'
    {
      "versions": [
        {
          "uid": 1,
          "date": "2019-09-27T18:05:21.936087Z",
          "name": "test",
          "snapshot": "",
          "size": 692241,
          "block_size": 4194304,
          "storage_id": 1,
          "status": "valid",
          "protected": false,
          "bytes_read": 692241,
          "bytes_written": 692241,
          "bytes_deduplicated": 0,
          "bytes_sparse": 0,
          "duration": 0,
          "labels": {
            "label-1": "bla",
            "label-2": "blub"
          },
          "blocks": [
            {
              "uid": {
                "left": 1,
                "right": 1
              },
              "size": 692241,
              "valid": true,
              "checksum": "d0d2b5d75e846ebfd7bc30784dfc1b727af47833c2d2ff9a2eac398db50dc3e0"
            }
          ]
        }
      ],
      "metadata_version": "2.0.0"

Such a dump can be imported with ``benji metadata-import``.

.. command-output:: benji metadata-import --help

``benji metadata-import`` only imports the metadata of *versions* which do not already exist in the database.
