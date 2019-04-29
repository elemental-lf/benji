# -*- encoding: utf-8 -*-
try:
    from setuptools import setup, Extension, find_packages
except ImportError:
    from distutils.core import setup, Extentsion, find_packages

with open('README.rst', 'r', encoding='utf-8') as fh:
    long_description = fh.read()


def get_version_and_cmdclass(package_path):
    import os
    from importlib.util import module_from_spec, spec_from_file_location
    spec = spec_from_file_location('version', os.path.join('src', package_path, '_version.py'))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.__version__, module.cmdclass


version, cmdclass = get_version_and_cmdclass('benji')

setup(
    name='benji',
    version=version,
    cmdclass=cmdclass,
    description='A block based deduplicating backup software for Ceph RBD, image files and devices ',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    classifiers="""Development Status :: 3 - Alpha
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)
Operating System :: POSIX
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Topic :: System :: Archiving :: Backup
""" [:-1].split('\n'),
    keywords='backup',
    author='Lars Fenneberg <lf@elemental.net>, Daniel Kraft <daniel.kraft@d9t.de>',
    author_email='lf@elemental.net, daniel.kraft@d9t.de',
    url='https://benji-backup.me/',
    license='LGPL-3',
    packages=find_packages('src', exclude=['*.tests', '*.tests.*']),
    package_dir={
        '': 'src',
    },
    package_data={
        'benji': ['schemas/*/*.yaml', 'sql_migrations/alembic.ini'],
    },
    zip_safe=False,  # ONLY because of alembic.ini. The rest is zip-safe.
    install_requires=[
        'PrettyTable>=0.7.2,<1',
        'sqlalchemy>=1.2.6,<2',
        'setproctitle>=1.1.8,<2',
        'python-dateutil>=2.6.0,<3',
        'alembic>=1.0.5,<2',
        'ruamel.yaml>0.15,<0.16',
        'psycopg2-binary>=2.7.4,<3',
        'argcomplete>=1.9.4,<2',
        'sparsebitfield>=0.2.2,<1',
        'cerberus>=1.2,<2',
        'pycryptodome>=3.6.1,<4',
        'pyparsing>=2.3.0,<3',
        'semantic_version>=2.6.0,<3',
        'dateparser>=0.7.0,<1',
        'structlog>=19.1.0',
        'colorama>=0.4.1,<1',
        'diskcache>=3.0.6',
    ],
    extras_require={
        's3': ['boto3>=1.7.28'],
        'b2': ['b2>=1.3.2'],
        'compression': ['zstandard>=0.9.0'],
        # For RBD support the packages supplied by the Linux distribution or the Ceph team should be used,
        # possible packages names include: python-rados, python-rbd or python3-rados, python3-rbd
        #'RBD support': ['rados', 'rbd'],
        'dev': ['parameterized'],
        'doc': ['sphinx', 'sphinx_rtd_theme', 'sphinxcontrib-programoutput'],
    },
    python_requires='~=3.6',
    entry_points="""
        [console_scripts]
            benji = benji.scripts.benji:main
    """,
)
