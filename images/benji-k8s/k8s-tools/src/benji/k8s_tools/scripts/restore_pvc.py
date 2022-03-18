#!/usr/bin/env python3
import argparse
import logging
import sys
import time

import kubernetes
from kubernetes.client.rest import ApiException

import benji.helpers.settings as settings
import benji.helpers.utils as utils
import benji.k8s_tools.kubernetes

utils.setup_logging()
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, allow_abbrev=False)

    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='Overwrite content of existing persistent volumes')
    parser.add_argument('--pvc-storage-class',
                        metavar='pvc_storage_class',
                        dest='pvc_storage_class',
                        default='rbd',
                        help='PVC storage class (only takes effect if the PVC does not exist yet)')
    parser.add_argument('--restore-url-template',
                        metavar='restore_url_template',
                        dest='restore_url_template',
                        help='Template to use for constructing URL for benji restore call',
                        default='rbd:{pool}/{namespace}/{image}')
    parser.add_argument(metavar='version_uid', dest='version_uid', help='Version uid')
    parser.add_argument(metavar='pvc_namespace', dest='pvc_namespace', help='PVC namespace')
    parser.add_argument(metavar='pvc_name', dest='pvc_name', help='PVC name')

    args = parser.parse_args()

    benji.k8s_tools.kubernetes.load_config()

    logger.info(f'Restoring version {args.version_uid} to PVC {args.pvc_namespace}/{args.pvc_name}.')

    benji_ls = utils.subprocess_run(
        ['benji', '--machine-output', '--log-level', settings.benji_log_level, 'ls', f'uid == "{args.version_uid}"'],
        decode_json=True)
    assert isinstance(benji_ls, dict)
    assert 'versions' in benji_ls
    assert isinstance(benji_ls['versions'], list)

    if len(benji_ls['versions']) == 0:
        raise RuntimeError(f'Size of {args.version_uid} could not be determined.')

    assert isinstance(benji_ls['versions'][0], dict)
    assert isinstance(benji_ls['versions'][0]['size'], int)
    version_size = benji_ls['versions'][0]['size']

    # This assumes that the Kubernetes client has already been initialized
    core_v1_api = kubernetes.client.CoreV1Api()
    pvc = None
    try:
        pvc = core_v1_api.read_namespaced_persistent_volume_claim(args.pvc_name, args.pvc_namespace)
    except ApiException as exception:
        if exception.status != 404:
            raise RuntimeError(f'Unexpected Kubernetes API exception: {str(exception)}')

    if pvc is None:
        pvc = benji.k8s_tools.kubernetes.create_pvc(args.pvc_name, args.pvc_namespace, version_size,
                                                    args.pvc_storage_class)
    else:
        if not args.force:
            raise RuntimeError('PVC already exists. Will not overwrite it unless forced.')

        # I don't really understand why capacity is a regular dict and not an object. Oh, well.
        pvc_size = int(benji.k8s_tools.kubernetes.parse_quantity(pvc.status.capacity['storage']))
        if pvc_size < version_size:
            raise RuntimeError(f'Existing PVC is too small to hold version {args.version_uid} ({pvc_size} < {version_size}).')
        elif pvc_size > version_size:
            logger.warning(f'Existing PVC is {pvc_size - version_size} bytes bigger than version {args.version_uid}.')

    while True:
        pvc = core_v1_api.read_namespaced_persistent_volume_claim(args.pvc_name, args.pvc_namespace)
        if pvc.status.phase == 'Bound':
            break
        logger.info('Waiting for persistent volume creation.')
        time.sleep(1)

    pv = core_v1_api.read_persistent_volume(pvc.spec.volume_name)
    rbd_info = benji.k8s_tools.kubernetes.determine_rbd_info_from_pv(pv)
    if rbd_info is None:
        raise RuntimeError(f'Unable to determine RBD information for {pv.metadata.name}')

    utils.subprocess_run([
        'benji',
        '--log-level',
        settings.benji_log_level,
        'restore',
        '--sparse',
        '--force',
        args.version_uid,
        args.restore_url_template.format(pool=rbd_info.pool, namespace=rbd_info.namespace, image=rbd_info.image),
    ])
    sys.exit(0)
