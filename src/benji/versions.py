from collections import namedtuple

import semantic_version

_VersionsTuple = namedtuple('_VersionsTuple', ['configuration', 'database_metadata', 'object_metadata'])
_VersionSpecPair = namedtuple('_VersionTuple', ['current', 'supported'])

# Configuration versions only use the major version part
VERSIONS = _VersionsTuple(configuration=_VersionSpecPair(current=semantic_version.Version('1.0.0'),
                                                         supported=semantic_version.SimpleSpec('>=1,<2')),
                          database_metadata=_VersionSpecPair(current=semantic_version.Version('3.0.0'),
                                                             supported=semantic_version.SimpleSpec('>=1,<4')),
                          object_metadata=_VersionSpecPair(current=semantic_version.Version('2.0.0'),
                                                           supported=semantic_version.SimpleSpec('>=1,<3')))
