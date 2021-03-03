from setuptools import setup, find_packages

setup(name='benji-k8s-tools',
      version='0.1',
      description='Small tools for using Benji with Kubernetes',
      url='https://github.com/elemental-lf/benji',
      author='Lars Fenneberg',
      author_email='lf@elemental.net',
      license='LGPG-3',
      python_requires='~=3.6',
      packages=find_packages('src'),
      package_dir={
          '': 'src',
      },
      install_requires=['benji', 'kubernetes>=10.0.0,<11'],
      entry_points="""
        [console_scripts]
            benji-backup-pvc = benji.k8s_tools.scripts.backup_pvc:main
            benji-command = benji.k8s_tools.scripts.command:main
            benji-restore-pvc = benji.k8s_tools.scripts.restore_pvc:main
            benji-versions-status = benji.k8s_tools.scripts.versions_status:main
    """)
