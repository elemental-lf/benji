import kopf
import kubernetes
import yaml

kubernetes.config.load_incluster_config()


@kopf.on.create('benji-backup.me', 'v1alpha1', 'benjirestores')
def benji_restore_pvc(body, **kwargs):

    cr_namespace = body['metadata']['namespace']
    cr_name = body['metadata']['name']
    pvc_name = body['spec']['persistentVolumeClaim']['claimName']
    version_name = body['spec']['version']['versionName']

    # Render the pod yaml with some spec fields used in the template.
    job_manifest = yaml.safe_load(f'''
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: { cr_name }
        spec:
          template:
            spec:
              containers:
              - command:
                - benji-restore-pvc
                - { version_name }
                - { cr_namespace }
                - { pvc_name }
                env:
                - name: POD_NAME
                  valueFrom:
                    fieldRef:
                      apiVersion: v1
                      fieldPath: metadata.name
                - name: POD_NAMESPACE
                  valueFrom:
                    fieldRef:
                      apiVersion: v1
                      fieldPath: metadata.namespace
                - name: PROM_PUSH_GATEWAY
                  value: benji-pushgateway:9091
                image: docker.io/elementalnet/benji-k8s:k8s-operator
                imagePullPolicy: Always
                name: benji
                resources: {'{}'}
                securityContext:
                  privileged: true
                  procMount: Default
                terminationMessagePath: /dev/termination-log
                terminationMessagePolicy: File
                volumeMounts:
                - mountPath: /benji/etc
                  name: benji-config
                - mountPath: /etc/localtime
                  name: tz-config
                - mountPath: /etc/ceph/ceph.conf
                  name: ceph-etc
                  readOnly: true
                  subPath: ceph.conf
                - mountPath: /etc/ceph/ceph.client.admin.keyring
                  name: ceph-client-admin-keyring
                  readOnly: true
                  subPath: ceph.client.admin.keyring
              dnsPolicy: ClusterFirstWithHostNet
              restartPolicy: Never
              schedulerName: default-scheduler
              securityContext: {'{}'}
              serviceAccountName: benji
              terminationGracePeriodSeconds: 30
              volumes:
              - configMap:
                  defaultMode: 420
                  name: benji
                name: benji-config
              - hostPath:
                  path: /usr/share/zoneinfo/Europe/Berlin
                  type: ""
                name: tz-config
              - configMap:
                  defaultMode: 292
                  name: ceph-etc
                name: ceph-etc
              - name: ceph-client-admin-keyring
                secret:
                  defaultMode: 292
                  secretName: ceph-client-admin-keyring
    ''')

    # Make it our child: assign the namespace, name, labels, owner references, etc.
    kopf.adopt(job_manifest, owner=body)

    # Actually create an object by requesting the Kubernetes API.
    batch_v1_api = kubernetes.client.BatchV1Api()
    job = batch_v1_api.create_namespaced_job(namespace=cr_namespace, body=job_manifest)

    # Update the parent's status.
    return {
        'associatedJobs': [{
            'namespace': job.metadata.namespace,
            'name': job.metadata.name,
            'uid': job.metadata.uid,
        }]
    }
