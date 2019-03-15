.. include:: global.rst.inc

*Version* Metadata
==================

At this stage Benji has backed up all image data to a (hopefully) safe place. However, the blocks are of no use without
the corresponding metadata. Benji will need this information to get the blocks back in the correct order and
restore the image.

This information is stored in the database backend. Additionally Benji will backup the metadata itself to the storage
automatically. Should you lose your database backend, you can restore these metadata backups by using
``benji metadata-restore``.

.. command-output:: benji metadata-restore --help

There is currently no mechanism to import the backup of all version's
metadata from the storage, but you could get a list of all versions
manually from the storage.

.. NOTE:: This metadata backup is compressed and encrypted like the blocks
    if you have these features enabled.

If you want to make your own copies of your metadata you can do so by using
``benji metadata-export``.

.. command-output:: benji metadata-export --help

If you're doing this programmatically and are exporting to STDOUT you should
probably add ``-m`` to your export command to reduce the logging level of Benji.

::

    $ benji -m metadata-export V1
    {
      "metadataVersion": "1.0.0",
      "versions": [
        {
          "uid": 1,
          "date": "2018-06-07T12:51:19",
          "name": "test",
          "snapshot_name": "",
          "size": 41943040,
          "block_size": 4194304,
          "valid": true,
          "protected": false,
          "tags": [],
          "blocks": [
            {
              "uid": {
                "left": 1,
                "right": 1
              },
              "date": "2018-06-07T14:51:20",
              "id": 0,
              "size": 4194304,
              "valid": true,
              "checksum": "aed3116b4e7fad9a3188f5ba7c8e73bf158dabec387ef1a7bca84c58fe72f319"
            },
    [...]

You can import such a dump of a version's metadata with ``benji metadata-import``.

.. command-output:: benji metadata-import --help

You can't import versions that already exist in the database backend.
