from collections import namedtuple
from typing import Dict

import semantic_version

_VersionTuple = namedtuple('_VersionTuple', ['current', 'supported'])

VERSIONS: Dict[str, Dict[str, _VersionTuple]] = {
    'configuration': _VersionTuple(current=semantic_version.Version('1.0.0'), supported=semantic_version.Spec('>=1,<2')),
    'database_metadata': _VersionTuple(current=semantic_version.Version('1.0.0'), supported=semantic_version.Spec('>=1,<2')),
    'object_metadata': _VersionTuple(current=semantic_version.Version('1.0.0'), supported=semantic_version.Spec('>=1,<2')),
}
