load('ext://restart_process', 'docker_build_with_restart')

analytics_settings(enable=False)
version_settings(check_updates=False)
disable_snapshots()
docker_prune_settings(disable=False, num_builds=1, max_age_mins=1)

config.define_string_list('vcs-ref')
config.define_string_list('build-date')
config.define_string_list('vcs-url')
config.define_string_list('version')
config.define_string_list('args', args=True)
cfg = config.parse()
config.set_enabled_resources(cfg.get('args', []))

allow_k8s_contexts('kind-kind')

docker_build(
    'elementalnet/benji',
    '.',
    dockerfile='images/benji/Dockerfile',
    build_args={
        'VCS_REF': cfg.get('vcs-ref', 'unknown'),
        'BUILD_DATE': cfg.get('build-date', 'unknown'),
        'VCS_URL': cfg.get('vcs-url', 'unknown'),
        'VERSION': cfg.get('version', 'unknown'),
    },
    # live_update=[
    #     sync('src/benji', '/benji/lib/python3.6/site-packages/benji'),
    #     restart_container(),
    # ],
    ignore=['*', '!src', '!etc', '!setup.py', '!README.rst', 'src/benji/k8s_operator', '!images/benji'])

docker_build_with_restart('elementalnet/benji-k8s-operator',
                          '.',
                          dockerfile='images/benji-k8s-operator/Dockerfile',
                          entrypoint=["/bin/sh", "-c", "kopf run -mbenji.k8s_operator"],
                          build_args={
                              'VCS_REF': cfg.get('vcs-ref', 'unknown'),
                              'BUILD_DATE': cfg.get('build-date', 'unknown'),
                              'VCS_URL': cfg.get('vcs-url', 'unknown'),
                              'VERSION': cfg.get('version', 'unknown'),
                          },
                          live_update=[
                              sync('src/benji', '/usr/local/lib/python3.7/site-packages/benji'),
                          ],
                          ignore=['*', '!src', '!etc', '!setup.py', '!README.rst', '!images/benji'])

k8s_resource('benji-operator', resource_deps=['benji-api'])
k8s_resource('benji', extra_pod_selectors=[{'app.kubernetes.io/managed-by': 'benji-operator'}])

k8s_kind('BenjiOperatorConfig', image_json_path=['{.spec.jobTemplate.spec.template.spec.containers[0].image}'])

helm_template = helm(
    'charts/benji',
    namespace='rook-ceph',
    name='benji',
    values=['../../dual/dual/addons/values/global/benji.yaml', '../../dual/dual/addons/values/dev/benji.yaml'])
k8s_yaml(helm_template)

k8s_yaml(listdir('charts/benji/crds'))
