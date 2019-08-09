from typing import NamedTuple

OPERATOR_CONFIG_ENV_NAME = 'BENJI_K8S_OPERATOR_CONFIG_NAME'
DEFAULT_OPERATOR_CONFIG_NAME = 'benji'

API_ENDPOINT_ENV_NAME = 'BENJI_API_ENDPOINT'
DEFAULT_API_ENDPOINT = 'http://benji-api:7746/'

# Labels used by the operator to establish a parent/child relationship between CRs and other resources.
LABEL_PARENT_KIND = 'operator.benji-backup.me/parent-kind'
LABEL_PARENT_NAMESPACE = 'operator.benji-backup.me/parent-namespace'
LABEL_PARENT_NAME = 'operator.benji-backup.me/parent-name'

# Constants for field names in the status section of a Job
JOB_STATUS_COMPLETION_TIME = 'completionTime'
JOB_STATUS_START_TIME = 'startTime'
JOB_STATUS_FAILED = 'failed'
JOB_STATUS_SUCCEEDED = 'succeeded'

RESOURCE_STATUS_LIST_OBJECT_REFERENCE = 'reference'

RESOURCE_STATUS_CHILDREN = 'children'
RESOURCE_STATUS_CHILDREN_HANDLER_NAME = 'handlerName'

RESOURCE_STATUS_DEPENDANT_JOBS = 'dependantJobs'
RESOURCE_STATUS_DEPENDANT_JOBS_STATUS = 'status'

RESOURCE_STATUS_CHILD_CHANGED = 'childChanged'

# Name of our status section tracking job status'
RESOURCE_STATUS_JOB_STATUS = 'jobStatus'

# Possible job
RESOURCE_JOB_STATUS_PENDING = 'Pending'
RESOURCE_JOB_STATUS_RUNNING = 'Running'
RESOURCE_JOB_STATUS_SUCCEEDED = 'Succeeded'
RESOURCE_JOB_STATUS_FAILED = 'Failed'

K8S_RESTORE_SPEC_PERSISTENT_VOLUME_CLAIM_NAME = 'persistentVolumeClaimName'
K8S_RESTORE_SPEC_VERSION_NAME = 'versionName'
K8S_RESTORE_SPEC_OVERWRITE = 'overwrite'
K8S_RESTORE_SPEC_STORAGE_CLASS_NAME = 'storageClassName'


class CRD(NamedTuple):
    api_group: str
    api_version: str
    name: str
    plural: str
    namespaced: bool


API_GROUP = 'benji-backup.me'
API_VERSION = 'v1alpha1'

# yapf: disable
CRD_VERSION = CRD(api_group=API_GROUP, api_version=API_VERSION, name='BenjiVersion', plural='benjiversions', namespaced=True)

CRD_RESTORE = CRD(api_group=API_GROUP, api_version=API_VERSION, name='BenjiRestore', plural='benjirestores', namespaced=True)

CRD_BACKUP_SCHEDULE = CRD(api_group=API_GROUP, api_version=API_VERSION, name='BenjiBackupSchedule', plural='benjibackupschedules', namespaced=True)
CRD_CLUSTER_BACKUP_SCHEDULE = CRD(api_group=API_GROUP, api_version=API_VERSION, name='ClusterBenjiBackupSchedule', plural='clusterbenjibackupschedules', namespaced=False)

CRD_RETENTION_SCHEDULE = CRD(api_group=API_GROUP, api_version=API_VERSION, name='BenjiRetentionSchedule', plural='benjiretentionschedules', namespaced=True)
CRD_CLUSTER_RETENTION_SCHEDULE = CRD(api_group=API_GROUP, api_version=API_VERSION, name='ClusterBenjiRetentionSchedule', plural='clusterbenjiretentionschedules', namespaced=False)

CRD_OPERATOR_CONFIG = CRD(api_group=API_GROUP, api_version=API_VERSION, name='BenjiOperatorConfig', plural='benjioperatorconfigs', namespaced=False)
# yapf: enable
