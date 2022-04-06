.. include:: global.rst.inc

.. _filter_expressions:

Filter Expressions
==================

A number of CLI command accept a filter expression to select the *versions* to display or act on:

* ``benji batch-scrub``, ``benji batch-deep-scrub``: The expression selects the *versions* to scrub. When no filter is
  specified all *versions* are scrubbed.
* ``benji enforce``: The expression selects the *versions* to which the retention policy is applied.
* ``benji ls``: The filter expression selects which *versions* to list. When no filter is specified all *versions* are
  listed.
* ``benji metadata-export``, ``benji metadata-backup``: The expression selects the *versions* to export or to backup
  respectively.


The filter expression syntax is a subset of Python's expression syntax. The following tokens are recognized:

* Identifiers: These reference version metadata columns and are named ``date``, ``uid``, ``volume``, ``snapshot``,
  ``size``, ``block_size``, ``status``, ``protected`` and ``storage``, ``read``, ``written``, ``dedup``, ``sparse``, and
  ``duration``.

* The ``status`` identifier should be compared to a string representing the status like ``status == "valid"``.

* The ``date`` identifier should be compared to a string representing an absolute or relative time reference. To parse
  this reference `dateparser <https://pypi.org/project/dateparser/>`_ with a locale of ``en`` (english) is used. The
  date and time format output by Benji is also accepted.

* In addition labels can be referenced via a special dictionary named ``labels``. The syntax is ``labels["label-name"]``.
  The string literal between the square brackets can be enclosed in either single or double quotes. Labels only
  supported comparisons with ``==`` and ``!=``. It is possible to test for label existence by using
  ``labels["label-name"]`` as a stand-alone expression.

* Strings literals either enclosed in single or double quotes

* Integers

* Comparison operators: The normal set of operators is supported: ``==``, ``!=``, ``<``, ``>``, ``<=`` and ``>=``. In
  addition a ``like`` operator is implemented and behaves like its SQL counterpart. If an operator is applied to a
  string the comparison is case sensitive. Benji (actually Python and SQLAlchemy) will try to adapt types when possible.

* Logical operators: ``not``, ``and``, and ``or`` (in order of precendence). ``not`` can be applied to other expressions
  and directly to identifiers (``not protected`` for example, but also ``not labels["label-name"]`` to test for label
  absence).

* Boolean constants: ``True`` and ``False``

* Brackets are supported to control precedence.

Examples:

* *Version* with the uid ``test-7o4lpp``: ``uid == "test-7o4lpp"``
* All ``invalid`` *versions*: ``status == "invalid"``
* All *versions* older than one week: ``date < "1 week ago"``
* All *versions* which have a label named ``label-1`` and are ``valid``: ``labels["label-1"] and status == "valid"``
* All *versions* with a volume name of ``database-1`` or a volume name of ``redis-1`` that are older than one month:
  ``(name == "database-1" or name == "redis-1") and date < "1 month ago"``
* All *versions* that have a label named ``label-1`` with value ``example-1``: ``labels["label-1"] == "example-1"``
* All protected *versions*: ``protected == True``
* All *versions* which have a volume name starting with ``alice``: ``volume like "alice%"``
* All *versions* which have a volume name containing ``bob``: ``volume like "%bob%"``
