.. include:: global.rst.inc

Retention Policy Enforcement
============================

.. command-output:: benji enforce --help

Benji supports a simple but flexible mechanism for retention policy enforcement. Enforcement is done per *version* name,
i.e. a specific policy is applied to all *versions* with the same name. Enforcement is not automatic and has to be
initiated with ``benji enforce``.

This Benji command is just for policy enforcement (i.e. thinning out *versions* by removal over time). You have to adjust
your schedule to match your policy. For example, if you want to keep hourly backups for the last 72 hours (hours72)
you have to create a new *version* at least every hour.

Filter expressions (see section :ref:`filter_expressions`) are used to select the *versions* the specified policy
is applied to.

``benji enforce`` supports machine readable output (see section :ref:`machine_output`). All removed versions are listed
and could be aggregated into a report for example.

.. ATTENTION:: Enforcement will remove any *version* which doesn't match the policy. To exclude versions from
    enforcement protect them with ``benji protect``.

Policy Specification
--------------------

The enforcement policy is specified as a comma separated list of time categories:

- latest (always refers to the youngest versions, regardless of how old they are)
- hours
- days
- weeks
- months
- years.

Each time interval is directly followed by a positive number, no whitespace is allowed. The categorization (except for
latest) follows natural time boundaries, i.e. the groups of *versions* formed by the hours category each start at
the full hour. The same goes for months where each group starts at the first of each month at 00:00 and for years
where each group starts at January 1st, 00:00. Each group of the weeks category starts on Monday at 00h00.

.. ATTENTION:: Times are based on the timezone which is set when ``benji enforce`` is called. Ensure that each
    invocation of ``benji enforce`` uses the same timezone. Otherwise some additional *versions* might get deleted
    because of the time shift.

Examples:

- latest3,hours48,days7,weeks4,months12,years3

  * Keep the youngest three versions regardless of their age
  * Then keep one version per hour for the last 48 hours
  * Then keep one version per week for the last four weeks
  * Then keep one version per month for the last twelve months
  * Then keep one version per year for the last three years

- years3,months12,latest2

  * Keep the youngest two versions
  * Then keep one version per month for the last twelve months
  * Then keep one version per year for the last three years

- hours72

  * Keep one version per hour for the last 72 hours

- latest10

  * Keep the youngest ten versions

Even when you specify the time categories in a different order, they are always considered from youngest to oldest
(see the second example).

You can call ``benji enforce`` as many times a day as you want. If no version currently falls into a specified category
the oldest version of the category below it is always kept, so that it has a chance to get old enough. So if in the
third example there is one version which is older than 24 hours but younger than one year it will be kept till it is
old enough even though it falls out of the first specified category.

If there is overlap between two categories the younger category always takes precedence.
