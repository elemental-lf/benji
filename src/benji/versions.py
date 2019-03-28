from collections import namedtuple

import semantic_version

_VersionsTuple = namedtuple('_VersionsTuple', ['configuration', 'database_metadata', 'object_metadata'])
_VersionSpecPair = namedtuple('_VersionTuple', ['current', 'supported'])

VERSIONS = _VersionsTuple(
    configuration=_VersionSpecPair(
        current=semantic_version.Version('1', partial=True), supported=semantic_version.Spec('>=1,<2')),
    database_metadata=_VersionSpecPair(
        current=semantic_version.Version('1.1.0'), supported=semantic_version.Spec('>=1,<2')),
    object_metadata=_VersionSpecPair(
        current=semantic_version.Version('1.0.0'), supported=semantic_version.Spec('>=1,<2')))
