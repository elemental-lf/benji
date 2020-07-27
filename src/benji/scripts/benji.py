#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK

import argparse
import os
import sys
from functools import partial
from typing import NamedTuple, Type, Optional

import argcomplete

from benji.exception import InternalError
from benji.io.factory import IOFactory
from benji.storage.factory import StorageFactory


class _ExceptionMapping(NamedTuple):
    exception: Type[BaseException]
    exit_code: int
    include_stacktrace: bool


def completion(shell: str) -> None:
    print(argcomplete.shellcode(sys.argv[0], shell=shell))


def integer_range(minimum: int, maximum: int, arg: str) -> Optional[int]:
    if arg is None:
        return None

    try:
        value = int(arg)
    except ValueError as err:
        raise argparse.ArgumentTypeError(str(err))

    if value < minimum or (maximum is not None and value > maximum):
        raise argparse.ArgumentTypeError('Expected a value between {} and {}, got {}.'.format(minimum, maximum, value))

    return value


def main():
    if sys.hexversion < 0x030605F0:
        # We're using features introduced with Python 3.6. In addition Python versions before 3.6.5 have some
        # shortcomings in the concurrent.futures implementation which lead to an excessive memory usage.
        raise InternalError('Benji only supports Python 3.6.5 or above.')

    enable_experimental = os.getenv('BENJI_EXPERIMENTAL', default='0') == '1'

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter, allow_abbrev=False)

    parser.add_argument('-c', '--config-file', default=None, type=str, help='Specify a non-default configuration file')
    parser.add_argument('-m',
                        '--machine-output',
                        action='store_true',
                        default=False,
                        help='Enable machine-readable JSON output')
    parser.add_argument('--log-level',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO',
                        help='Only log messages of this level or above on the console')
    parser.add_argument('--no-color',
                        action='store_true',
                        default=False,
                        help='Disable colorization of console logging')

    subparsers_root = parser.add_subparsers(title='commands')

    # BACKUP
    p = subparsers_root.add_parser('backup', help='Perform a backup')
    p.add_argument('-u',
                   '--uid',
                   dest='version_uid',
                   default=None,
                   help='Unique ID of created version (will be generated automatically if not specified)')
    p.add_argument('-s', '--snapshot', default='', help='Snapshot name (e.g. the name of the RBD snapshot)')
    p.add_argument('-r', '--rbd-hints', default=None, help='Hints in rbd diff JSON format')
    p.add_argument('-f', '--base-version', dest='base_version_uid', default=None, help='Base version UID')
    p.add_argument('-b', '--block-size', type=int, default=None, help='Block size in bytes')
    p.add_argument('-l',
                   '--label',
                   action='append',
                   dest='labels',
                   metavar='label',
                   default=None,
                   help='Labels for this version (can be repeated)')
    p.add_argument('-S', '--storage', default='', help='Destination storage (if unspecified the default is used)')
    p.add_argument('source', help='Source URL')
    p.add_argument('volume', help='Volume name')
    p.set_defaults(func='backup')

    # BATCH-DEEP-SCRUB
    p = subparsers_root.add_parser('batch-deep-scrub',
                                   help='Check data and metadata integrity of multiple versions at once',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-p',
                   '--block-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of blocks')
    p.add_argument('-P',
                   '--version-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of versions')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='batch_deep_scrub')

    # BATCH-SCRUB
    p = subparsers_root.add_parser('batch-scrub',
                                   help='Check block existence and metadata integrity of multiple versions at once',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-p',
                   '--block-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of blocks')
    p.add_argument('-P',
                   '--version-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of versions')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='batch_scrub')

    # CLEANUP
    p = subparsers_root.add_parser('cleanup', help='Cleanup no longer referenced blocks')
    p.add_argument('--override-lock', action='store_true', help='Override and release any held lock (dangerous)')
    p.set_defaults(func='cleanup')

    # COMPLETION
    p = subparsers_root.add_parser('completion', help='Emit autocompletion script')
    p.add_argument('shell', choices=['bash', 'tcsh'], help='Shell')
    p.set_defaults(func='completion')

    # DATABASE-INIT
    p = subparsers_root.add_parser('database-init',
                                   help='Initialize the database (will not delete existing tables or data)')
    p.set_defaults(func='database_init')

    # DATABASE-MIGRATE
    p = subparsers_root.add_parser('database-migrate', help='Migrate an existing database to a new schema revision')
    p.set_defaults(func='database_migrate')

    # DEEP-SCRUB
    p = subparsers_root.add_parser('deep-scrub',
                                   help='Check a version\'s data and metadata integrity',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-s', '--source', default=None, help='Additionally compare version against source URL')
    p.add_argument('-p',
                   '--block-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of blocks')
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='deep_scrub')

    # ENFORCE
    p = subparsers_root.add_parser('enforce', help="Enforce a retention policy ")
    p.add_argument('--dry-run', action='store_true', help='Only show which versions would be removed')
    p.add_argument('-k', '--keep-metadata-backup', action='store_true', help='Keep version metadata backup')
    p.add_argument('-g', '--group_label', default=None, help='Label to find related versions to remove')
    p.add_argument('rules_spec', help='Retention rules specification')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='enforce_retention_policy')

    # LABEL
    p = subparsers_root.add_parser('label', help='Add labels to a version')
    p.add_argument('version_uid')
    p.add_argument('labels', nargs='+')
    p.set_defaults(func='label')

    # LS
    p = subparsers_root.add_parser('ls', help='List versions')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.add_argument('-l', '--include-labels', action='store_true', help='Include labels in output')
    p.add_argument('-s', '--include-stats', action='store_true', help='Include statistics in output')
    p.set_defaults(func='ls')

    # METADATA-BACKUP
    p = subparsers_root.add_parser('metadata-backup', help='Back up the metadata of one or more versions')
    p.add_argument('filter_expression', help="Version filter expression")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite existing metadata backups')
    p.set_defaults(func='metadata_backup')

    # METADATA EXPORT
    p = subparsers_root.add_parser('metadata-export',
                                   help='Export the metadata of one or more versions to a file or standard output')
    p.add_argument('filter_expression', nargs='?', default=None, help="Version filter expression")
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing output file')
    p.add_argument('-o', '--output-file', default=None, help='Output file (standard output if missing)')
    p.set_defaults(func='metadata_export')

    # METADATA-IMPORT
    p = subparsers_root.add_parser('metadata-import',
                                   help='Import the metadata of one or more versions from a file or standard input')
    p.add_argument('-i', '--input-file', default=None, help='Input file (standard input if missing)')
    p.set_defaults(func='metadata_import')

    # METADATA-LS
    p = subparsers_root.add_parser('metadata-ls', help='List the version metadata backup')
    p.add_argument('-S', '--storage', default=None, help='Source storage (if unspecified the default is used)')
    p.set_defaults(func='metadata_ls')

    # METADATA-RESTORE
    p = subparsers_root.add_parser('metadata-restore', help='Restore the metadata of one ore more versions')
    p.add_argument('-S', '--storage', default=None, help='Source storage (if unspecified the default is used)')
    p.add_argument('version_uids', metavar='VERSION_UID', nargs='+', help="Version UID")
    p.set_defaults(func='metadata_restore')

    # NBD
    p = subparsers_root.add_parser('nbd',
                                   help='Start an NBD server',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-a', '--bind-address', default='127.0.0.1', help='Bind to the specified IP address')
    p.add_argument('-p', '--bind-port', default=10809, help='Bind to the specified port')
    p.add_argument('-r', '--read-only', action='store_true', default=False, help='NBD device is read-only')
    p.set_defaults(func='nbd')

    # PROTECT
    p = subparsers_root.add_parser('protect', help='Protect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help="Version UID")
    p.set_defaults(func='protect')

    # RESTORE
    p = subparsers_root.add_parser('restore', help='Restore a backup')
    p.add_argument('-s', '--sparse', action='store_true', help='Restore only existing blocks')
    p.add_argument('-f', '--force', action='store_true', help='Overwrite an existing file, device or image')
    p.add_argument('-d', '--database-less', action='store_true', help='Restore without requiring the database')
    p.add_argument('-S', '--storage', default=None, help='Source storage (if unspecified the default is used)')
    p.add_argument('version_uid', help='Version UID to restore')
    p.add_argument('destination', help='Destination URL')
    p.set_defaults(func='restore')

    # RM
    p = subparsers_root.add_parser('rm', help='Remove one or more versions')
    p.add_argument('-f', '--force', action='store_true', help='Force removal (overrides protection of recent versions)')
    p.add_argument('-k', '--keep-metadata-backup', action='store_true', help='Keep version metadata backup')
    p.add_argument('--override-lock', action='store_true', help='Override and release any held locks (dangerous)')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='rm')

    # SCRUB
    p = subparsers_root.add_parser('scrub',
                                   help='Check a version\'s block existence and metadata integrity',
                                   formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-p',
                   '--block-percentage',
                   type=partial(integer_range, 1, 100),
                   default=100,
                   help='Check only a certain percentage of blocks')
    p.add_argument('version_uid', help='Version UID')
    p.set_defaults(func='scrub')

    # STORAGE-STATS
    p = subparsers_root.add_parser('storage-stats', help='Show storage statistics')
    p.add_argument('storage_name', nargs='?', default=None, help='Storage')
    p.set_defaults(func='storage_stats')

    # UNPROTECT
    p = subparsers_root.add_parser('unprotect', help='Unprotect one or more versions')
    p.add_argument('version_uids', metavar='version_uid', nargs='+', help='Version UID')
    p.set_defaults(func='unprotect')

    # VERSION-INFO
    p = subparsers_root.add_parser('version-info', help='Program version information')
    p.set_defaults(func='version_info')

    # REST-API
    if enable_experimental:
        p = subparsers_root.add_parser('rest-api', help='Start REST API server')
        p.set_defaults(func='rest_api')
        p.add_argument('-a', '--bind-address', default='127.0.0.1', help='Bind to the specified IP address')
        p.add_argument('-p', '--bind-port', default=8080, type=int, help='Bind to the specified port')
        p.add_argument('--threads', default=1, type=int, help='Number of worker threads')

    # DU
    p = subparsers_root.add_parser('storage-usage', help='Provide storage usage statistics')
    p.add_argument('filter_expression', nargs='?', default=None, help='Version filter expression')
    p.set_defaults(func='storage_usage')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_usage()
        sys.exit(os.EX_USAGE)

    if args.func == 'completion':
        completion(args.shell)
        sys.exit(os.EX_OK)

    from benji.config import Config
    from benji.logging import logger, init_logging
    if args.config_file is not None and args.config_file != '':
        try:
            cfg = open(args.config_file, 'r', encoding='utf-8').read()
        except FileNotFoundError:
            logger.error('File {} not found.'.format(args.config_file))
            sys.exit(os.EX_USAGE)
        config = Config(ad_hoc_config=cfg)
    else:
        config = Config()

    console_formatter = 'console-colored'
    if args.machine_output:
        console_formatter = 'json'
    elif args.no_color:
        console_formatter = 'console-plain'

    init_logging(logfile=config.get('logFile', types=(str, type(None))),
                 console_level=args.log_level,
                 console_formatter=console_formatter)

    IOFactory.initialize(config)
    StorageFactory.initialize(config)

    import benji.commands
    commands = benji.commands.Commands(args.machine_output, config)
    func = getattr(commands, args.func)

    # Pass over to function
    func_args = dict(args._get_kwargs())
    del func_args['config_file']
    del func_args['func']
    del func_args['log_level']
    del func_args['machine_output']
    del func_args['no_color']

    # From most specific to least specific
    # yapf: disable
    exception_mappings = [
        _ExceptionMapping(exception=benji.exception.UsageError, exit_code=os.EX_USAGE, include_stacktrace=False),
        _ExceptionMapping(exception=benji.exception.AlreadyLocked, exit_code=os.EX_NOPERM, include_stacktrace=False),
        _ExceptionMapping(exception=benji.exception.InternalError, exit_code=os.EX_SOFTWARE, include_stacktrace=True),
        _ExceptionMapping(exception=benji.exception.ConfigurationError, exit_code=os.EX_CONFIG, include_stacktrace=False),
        _ExceptionMapping(exception=benji.exception.InputDataError, exit_code=os.EX_DATAERR, include_stacktrace=False),
        _ExceptionMapping(exception=benji.exception.ScrubbingError, exit_code=os.EX_DATAERR, include_stacktrace=False),
        _ExceptionMapping(exception=PermissionError, exit_code=os.EX_NOPERM, include_stacktrace=False),
        _ExceptionMapping(exception=FileExistsError, exit_code=os.EX_CANTCREAT, include_stacktrace=False),
        _ExceptionMapping(exception=FileNotFoundError, exit_code=os.EX_NOINPUT, include_stacktrace=False),
        _ExceptionMapping(exception=EOFError, exit_code=os.EX_IOERR, include_stacktrace=True),
        _ExceptionMapping(exception=IOError, exit_code=os.EX_IOERR, include_stacktrace=True),
        _ExceptionMapping(exception=OSError, exit_code=os.EX_OSERR, include_stacktrace=True),
        _ExceptionMapping(exception=ConnectionError, exit_code=os.EX_IOERR, include_stacktrace=True),
        _ExceptionMapping(exception=LookupError, exit_code=os.EX_NOINPUT, include_stacktrace=True),
        _ExceptionMapping(exception=KeyboardInterrupt, exit_code=os.EX_NOINPUT, include_stacktrace=False),
        _ExceptionMapping(exception=BaseException, exit_code=os.EX_SOFTWARE, include_stacktrace=True),
    ]
    # yapf: enable

    try:
        logger.debug('commands.{0}(**{1!r})'.format(args.func, func_args))
        func(**func_args)
        sys.exit(os.EX_OK)
    except SystemExit:
        raise
    except BaseException as exception:
        for case in exception_mappings:
            if isinstance(exception, case.exception):
                message = str(exception)
                if message:
                    message = '{}: {}'.format(exception.__class__.__name__, message)
                else:
                    message = '{} exception occurred.'.format(exception.__class__.__name__)
                if case.include_stacktrace:
                    logger.error(message, exc_info=True)
                else:
                    logger.debug(message, exc_info=True)
                    logger.error(message)
                sys.exit(case.exit_code)


if __name__ == '__main__':
    main()
