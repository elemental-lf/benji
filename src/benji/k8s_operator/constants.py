# Labels used by the operator to establish a parent/child relationship between CRs and other resources.
LABEL_PARENT_KIND = 'operator.benji-backup.me/parent-kind'
LABEL_PARENT_NAMESPACE = 'operator.benji-backup.me/parent-namespace'
LABEL_PARENT_NAME = 'operator.benji-backup.me/parent-name'

# Version labels (in Benji)
LABEL_INSTANCE = 'benji-backup.me/instance'
LABEL_K8S_PVC_NAMESPACE = 'benji-backup.me/k8s-pvc-namespace'
LABEL_K8S_PVC_NAME = 'benji-backup.me/k8s-pvc-name'
LABEL_K8S_PV_NAME = 'benji-backup.me/k8s-pv-name'

# Constants for field names in the status section of a Job
JOB_STATUS_COMPLETION_TIME = 'completionTime'
JOB_STATUS_START_TIME = 'startTime'
JOB_STATUS_FAILED = 'failed'
JOB_STATUS_SUCCEEDED = 'succeeded'

RESOURCE_STATUS_LIST_OBJECT_REFERENCE = 'reference'

RESOURCE_STATUS_JOBS = 'jobs'
RESOURCE_STATUS_DEPENDANT_JOBS_STATUS = 'status'

# Possible job status values
RESOURCE_JOB_STATUS_PENDING = 'Pending'
RESOURCE_JOB_STATUS_RUNNING = 'Running'
RESOURCE_JOB_STATUS_SUCCEEDED = 'Succeeded'
RESOURCE_JOB_STATUS_FAILED = 'Failed'

API_GROUP = 'benji-backup.me'
API_VERSION = 'v1alpha1'
