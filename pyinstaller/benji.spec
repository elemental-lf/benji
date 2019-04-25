# -*- mode: python -*-

from PyInstaller.utils.hooks import collect_submodules, collect_data_files
from tempfile import NamedTemporaryFile
from itertools import chain
import sys
import os
from os.path import abspath

def write_version(fname):
    import os
    from importlib.util import module_from_spec, spec_from_file_location
    spec = spec_from_file_location('version', os.path.join('src', 'benji', '_version.py'))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    module._write_version(fname)


sys.path.insert(0, abspath('./src'))
hiddenimports_packages = ['benji.io', 'benji.storage', 'benji.sql_migrations.alembic', 'Crypto.Hash']
hiddenimports = ['sortedcontainers']
for package in hiddenimports_packages:
    hiddenimports += collect_submodules(package)

# Paths are relative to the spec file
a = Analysis(['../src/benji/scripts/benji.py'],
             binaries=[],
             datas=[('../src/benji/schemas', 'benji/schemas'),
                    ('../src/benji/sql_migrations/alembic.ini', 'benji/sql_migrations'),
                    ('../src/benji/sql_migrations/alembic/', 'benji/sql_migrations/alembic/')],
             hiddenimports=hiddenimports,
             excludes=['rados', 'rbd', 'libiscsi'],
             hookspath=[],
             runtime_hooks=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=None,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

with NamedTemporaryFile(suffix='.py') as version_file:
    write_version(version_file.name)
    a.datas.append(('benji/_static_version.py', version_file.name, 'DATA'))
    exe = EXE(
        pyz,
        a.scripts,
        a.binaries,
        a.zipfiles,
        a.datas, [],
        name='benji',
        debug=False,
        bootloader_ignore_signals=False,
        strip=False,
        upx=True,
        runtime_tmpdir=None,
        console=True)
