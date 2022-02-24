#!/usr/bin/env python3
import argparse
import logging
import random
import string
import sys
import time
from typing import Any, Dict, Optional, Tuple

import kubernetes
import kubernetes.stream

import benji.helpers.ceph as ceph
import benji.helpers.prometheus as prometheus
import benji.helpers.settings as settings
import benji.helpers.utils as utils
import benji.k8s_tools.kubernetes

FSFREEZE_TIMEOUT = 15
FSFREEZE_UNFREEZE_TRIES = (0, 1, 1, 1, 15, 30)
FSFREEZE_ANNOTATION = 'benji-backup.me/fsfreeze'
FSFREEZE_POD_LABEL_SELECTOR = 'benji-backup.me/component=fsfreeze'
FSFREEZE_CONTAINER_NAME = 'fsfreeze'

utils.setup_logging()
logger = logging.getLogger()


def _random_string(length: int, characters: str = string.ascii_lowercase + string.digits) -> str:
    return ''.join(random.choice(characters) for _ in range(length))


def _determine_fsfreeze_info(pvc_namespace: str, pvc_name: str, image: str) -> Tuple[bool, Optional[str], Optional[str]]:
    pv_fsfreeze = False
    pv_host_ip = None
    pv_fsfreeze_pod = None

    core_v1_api = kubernetes.client.CoreV1Api()
    pvc = core_v1_api.read_namespaced_persistent_volume_claim(pvc_name, pvc_namespace)
    service_account_namespace = benji.k8s_tools.kubernetes.service_account_namespace()
    if hasattr(pvc.metadata,
               'annotations') and FSFREEZE_ANNOTATION in pvc.metadata.annotations and pvc.metadata.annotations[FSFREEZE_ANNOTATION] == 'yes':
        pods = core_v1_api.list_namespaced_pod(service_account_namespace, watch=False).items
        for pod in pods:
            if pv_fsfreeze:
                break
            if not hasattr(pod.spec, 'volumes'):
                continue
            for volume in pod.spec.volumes:
                if not hasattr(volume, 'persistent_volume_claim') or not hasattr(
                        volume.persistent_volume_claim, 'claim_name') or volume.persistent_volume_claim.claim_name != pvc_name:
                    continue
                if hasattr(pod.status, 'host_ip') and pod.status.host_ip != '':
                    pv_fsfreeze = True
                    pv_host_ip = pod.status.host_ip
                break

        if pv_fsfreeze:
            pods = core_v1_api.list_namespaced_pod(benji.k8s_tools.kubernetes.service_account_namespace(),
                                                   label_selector=FSFREEZE_POD_LABEL_SELECTOR).items

            if not pods:
                logger.error('No fsfreeze pods found (label selector {FSFREEZE_POD_LABEL_SELECTOR}).')

            for pod in pods:
                if not hasattr(pod.status, 'host_ip') or not hasattr(pod.status, 'phase'):
                    continue

                if pod.status.host_ip == pv_host_ip and pod.status.phase == 'Running':
                    pv_fsfreeze_pod = pod.metadata.name
                    break
            else:
                pv_fsfreeze = False

    return pv_fsfreeze, pv_host_ip, pv_fsfreeze_pod


@ceph.signal_snapshot_create_pre.connect
def ceph_snapshot_create_pre(sender: str, volume: str, pool: str, namespace: str, image: str, snapshot: str,
                             context: Dict[str, Any]) -> None:
    assert isinstance(context, dict)
    assert 'pvc' in context
    pvc_namespace = context['pvc'].metadata.namespace
    pvc_name = context['pvc'].metadata.name
    pv_mount_point = context['pv-mount-point']

    if pv_mount_point is None:
        logger.warning(f'Mount path of PV is not known, skipping fsfreeze.')

    pv_fsfreeze, pv_host_ip, pv_fsfreeze_pod = _determine_fsfreeze_info(pvc_namespace, pvc_name, image)

    # Record for use in post signals
    context['pv-fsfreeze'] = pv_fsfreeze
    context['pv-host-ip'] = pv_host_ip
    context['pv-fsfreeze-pod'] = pv_fsfreeze_pod

    if not pv_fsfreeze:
        return
    if pv_host_ip is None:
        logger.info(f'PV is not mounted anywhere, skipping fsfreeze.')
        return
    if pv_fsfreeze_pod is None:
        logger.warning(f'No fsfreeze pod found for host {pv_host_ip}, skipping fsfreeze for this PV.')
        return

    logger.info(f'Freezing filesystem {pv_mount_point} on host {pv_host_ip} (pod {pv_fsfreeze_pod}).')

    service_account_namespace = benji.k8s_tools.kubernetes.service_account_namespace()
    try:
        benji.k8s_tools.kubernetes.pod_exec(['fsfreeze', '--freeze', pv_mount_point],
                                            name=pv_fsfreeze_pod,
                                            namespace=service_account_namespace,
                                            container=FSFREEZE_CONTAINER_NAME,
                                            timeout=FSFREEZE_TIMEOUT)
    except Exception as exception:
        # Try to unfreeze in any case
        try:
            benji.k8s_tools.kubernetes.pod_exec(['fsfreeze', '--unfreeze', pv_mount_point],
                                                name=pv_fsfreeze_pod,
                                                namespace=service_account_namespace,
                                                container=FSFREEZE_CONTAINER_NAME,
                                                timeout=FSFREEZE_TIMEOUT)
        except Exception as exception_2:
            raise exception_2 from exception
        else:
            raise exception

    logger.debug(f'Freezing filesystem succeeded.')


@ceph.signal_snapshot_create_post_success.connect
def ceph_snapshot_create_post_success(sender: str, volume: str, pool: str, namespace: str, image: str, snapshot: str,
                                      context: Dict[str, Any]) -> None:
    assert isinstance(context, dict)
    pv_fsfreeze = context['pv-fsfreeze']
    if not pv_fsfreeze:
        return

    pv_host_ip = context['pv-host-ip']
    pv_fsfreeze_pod = context['pv-fsfreeze-pod']
    pv_mount_point = context['pv-mount-point']

    logger.info(f'Unfreezing filesystem {pv_mount_point} on host {pv_host_ip}.')

    service_account_namespace = benji.k8s_tools.kubernetes.service_account_namespace()
    for delay in FSFREEZE_UNFREEZE_TRIES:
        if delay > 0:
            time.sleep(delay)

        try:
            benji.k8s_tools.kubernetes.pod_exec(['fsfreeze', '--unfreeze', pv_mount_point],
                                                name=pv_fsfreeze_pod,
                                                namespace=service_account_namespace,
                                                container=FSFREEZE_CONTAINER_NAME,
                                                timeout=FSFREEZE_TIMEOUT)
        except Exception:
            pass
        else:
            logger.debug(f'Unfreezing filesystem succeeded.')
            break
    else:
        logger.error(f'Giving up on unfreezing filesystem {pv_mount_point} on host {pv_host_ip}.')


@ceph.signal_snapshot_create_post_error.connect
def ceph_snapshot_create_post_error(sender: str, volume: str, pool: str, namespace: str, image: str, snapshot: str,
                                    context: Dict[str, Any], exception: Exception) -> None:
    ceph_snapshot_create_post_success(sender, volume, pool, image, snapshot, context)
    raise exception


@ceph.signal_backup_pre.connect
def ceph_backup_pre(sender: str, volume: str, pool: str, namespace: str, image: str, version_labels: Dict[str, str],
                    context: Dict[str, Any]):
    assert isinstance(context, dict)
    context['backup-start-time'] = start_time = time.time()
    prometheus.backup_start_time.labels(volume=volume).set(start_time)


def _k8s_create_pvc_event(type: str, reason: str, message: str, context: Dict[str, Any]):
    assert isinstance(context, dict)
    assert 'pvc' in context
    pvc_namespace = context['pvc'].metadata.namespace
    pvc_name = context['pvc'].metadata.name
    pvc_uid = context['pvc'].metadata.uid

    try:
        benji.k8s_tools.kubernetes.create_pvc_event(type=type,
                                                    reason=reason,
                                                    message=message,
                                                    pvc_namespace=pvc_namespace,
                                                    pvc_name=pvc_name,
                                                    pvc_uid=pvc_uid)
    except Exception as exception:
        logger.error(f'Creating Kubernetes event for {pvc_namespace}/{pvc_name} failed with a {exception.__class__.__name__} exception: {str(exception)}')
        pass


@ceph.signal_backup_post_success.connect
def ceph_backup_post_success(sender: str, volume: str, pool: str, namespace: str, image: str,
                             version_labels: Dict[str, str], context: Dict[str, Any], version: Optional[Dict]):
    assert isinstance(context, dict)
    assert version is not None

    pvc_namespace = context['pvc'].metadata.namespace
    pvc_name = context['pvc'].metadata.name
    pvc_uid = context['pvc'].metadata.uid
    start_time = context['backup-start-time']

    completion_time = time.time()
    prometheus.backup_completion_time.labels(volume=volume).set(completion_time)
    prometheus.backup_runtime_seconds.labels(volume=volume).set(completion_time - start_time)
    prometheus.backup_status_succeeded.labels(volume=volume).set(1)
    prometheus.push(prometheus.backup_registry, grouping_key={'pvc_namespace': pvc_namespace, 'pvc_name': pvc_name})
    try:
        benji.k8s_tools.kubernetes.create_pvc_event(
            type='Normal',
            reason='SuccessfulBackup',
            message=f'Backup to {version["uid"]} completed successfully (took {completion_time - start_time:.0f} seconds).',
            pvc_namespace=pvc_namespace,
            pvc_name=pvc_name,
            pvc_uid=pvc_uid)
    except Exception as exception:
        logger.error(f'Creating Kubernetes event for {pvc_namespace}/{pvc_name} failed with a {exception.__class__.__name__} exception: {str(exception)}')
        pass


@ceph.signal_backup_post_error.connect
def ceph_backup_post_error(sender: str, volume: str, pool: str, namespace: str, image: str, version_labels: Dict[str,
                                                                                                                 str],
                           context: Dict[str, Any], version: Optional[Dict], exception: Exception):
    assert isinstance(context, dict)
    pvc_namespace = context['pvc'].metadata.namespace
    pvc_name = context['pvc'].metadata.name
    pvc_uid = context['pvc'].metadata.uid

    start_time = context['backup-start-time']
    completion_time = time.time()

    prometheus.backup_completion_time.labels(volume=volume).set(completion_time)
    prometheus.backup_runtime_seconds.labels(volume=volume).set(completion_time - start_time)
    prometheus.backup_status_failed.labels(volume=volume).set(1)
    prometheus.push(prometheus.backup_registry, grouping_key={'pvc_namespace': pvc_namespace, 'pvc_name': pvc_name})

    benji.k8s_tools.kubernetes.create_pvc_event(type='Warning',
                                                reason='FailedBackup',
                                                message=f'Backup failed: {exception.__class__.__name__} {str(exception)}',
                                                pvc_namespace=pvc_namespace,
                                                pvc_name=pvc_name,
                                                pvc_uid=pvc_uid)

    raise exception


def main():
    # This arguments parser tries to mimic kubectl
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, allow_abbrev=False)

    parser.add_argument('-n',
                        '--namespace',
                        metavar='namespace',
                        dest='namespace',
                        default=None,
                        help='Filter on namespace')
    parser.add_argument('-l',
                        '--selector',
                        metavar='label-selector',
                        dest='labels',
                        action='append',
                        default=[],
                        help='Filter PVCs on label selector')
    parser.add_argument('--field-selector',
                        metavar='field-selector',
                        dest='fields',
                        action='append',
                        default=[],
                        help='Filter PVCs on field selector')
    parser.add_argument('--source-compare',
                        dest='source_compare',
                        action='store_true',
                        default=False,
                        help='Compare version to source after backup')

    args = parser.parse_args()

    benji.k8s_tools.kubernetes.load_config()
    core_v1_api = kubernetes.client.CoreV1Api()

    labels = ','.join(args.labels)
    fields = ','.join(args.fields)

    if args.namespace is not None:
        logger.info(f'Backing up all PVCs in namespace {args.namespace}.')
    else:
        logger.info(f'Backing up all PVCs in all namespaces.')
    if labels != '':
        logger.info(f'Matching label(s) {labels}.')
    if fields != '':
        logger.info(f'Matching field(s) {fields}.')

    if args.namespace is not None:
        pvcs = core_v1_api.list_namespaced_persistent_volume_claim(args.namespace,
                                                                   watch=False,
                                                                   label_selector=labels,
                                                                   field_selector=fields).items
    else:
        pvcs = core_v1_api.list_persistent_volume_claim_for_all_namespaces(watch=False,
                                                                           label_selector=labels,
                                                                           field_selector=fields).items
    if len(pvcs) == 0:
        logger.info('Not matching PVCs found.')
        sys.exit(0)

    for pvc in pvcs:
        if not hasattr(pvc.spec, 'volume_name') or pvc.spec.volume_name in (None, ''):
            continue

        pv = core_v1_api.read_persistent_volume(pvc.spec.volume_name)
        rbd_info = benji.k8s_tools.kubernetes.determine_rbd_info_from_pv(pv)
        if rbd_info is None:
            logger.debug(f'PersistentVolume {pv.metadata.name} is not an RBD backed volume '
                         f'or the volume format is unknown to us.')
            continue

        volume = f'{pvc.metadata.namespace}/{pvc.metadata.name}'
        # Limit the version_uid to 253 characters so that it is a compatible Kubernetes resource name.
        version_uid = '{}-{}'.format(f'{pvc.metadata.namespace}-{pvc.metadata.name}'[:246], _random_string(6))

        version_labels = {
            'benji-backup.me/instance': settings.benji_instance,
            'benji-backup.me/ceph-pool': rbd_info.pool,
            'benji-backup.me/ceph-namespace': rbd_info.namespace,
            'benji-backup.me/ceph-rbd-image': rbd_info.image,
            'benji-backup.me/k8s-pvc-namespace': pvc.metadata.namespace,
            'benji-backup.me/k8s-pvc': pvc.metadata.name,
            'benji-backup.me/k8s-pv': pv.metadata.name
        }

        context = {
            'pvc': pvc,
            'pv': pv,
            'pv-mount-point': rbd_info.mount_point,
        }
        ceph.backup(volume=volume,
                    pool=rbd_info.pool,
                    namespace=rbd_info.namespace,
                    image=rbd_info.image,
                    version_uid=version_uid,
                    version_labels=version_labels,
                    source_compare=args.source_compare,
                    context=context)

    sys.exit(0)
