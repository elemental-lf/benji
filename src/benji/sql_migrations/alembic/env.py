import sqlite3

from alembic import context
from alembic.operations import ops
from sqlalchemy import create_engine

from benji.config import Config as BenjiConfig
from benji.database import Base

config = context.config

# Only load configuration when we're running standalone
if config.attributes.get('connection', None) is None:
    benji_config = BenjiConfig()
    database_engine = benji_config.get('databaseEngine', types=str)
else:
    database_engine = None

target_metadata = Base.metadata


def run_migrations_online():
    # Sources: https://alembic.sqlalchemy.org/en/latest/cookbook.html#don-t-emit-drop-index-when-the-table-is-to-be-dropped-as-well,
    #          https://alembic.sqlalchemy.org/en/latest/cookbook.html#don-t-generate-empty-migrations-with-autogenerate
    def process_revision_directives(context, revision, directives):
        if config.cmd_opts.autogenerate:
            script = directives[0]
            if not script.upgrade_ops.is_empty():
                # process both "def upgrade()", "def downgrade()"
                for directive in (script.upgrade_ops, script.downgrade_ops):

                    # make a set of tables that are being dropped within
                    # the migration function
                    tables_dropped = set()
                    for op in directive.ops:
                        if isinstance(op, ops.DropTableOp):
                            tables_dropped.add((op.table_name, op.schema))

                    # now rewrite the list of "ops" such that DropIndexOp
                    # is removed for those tables.   Needs a recursive function.
                    directive.ops = list(_filter_drop_indexes(directive.ops, tables_dropped))
            else:
                directives[:] = []

    def _filter_drop_indexes(directives, tables_dropped):
        # given a set of (tablename, schemaname) to be dropped, filter
        # out DropIndexOp from the list of directives and yield the result.

        for directive in directives:
            # ModifyTableOps is a container of ALTER TABLE types of
            # commands.  process those in place recursively.
            if isinstance(directive, ops.ModifyTableOps) and \
                    (directive.table_name, directive.schema) in tables_dropped:
                directive.ops = list(_filter_drop_indexes(directive.ops, tables_dropped))

                # if we emptied out the directives, then skip the
                # container altogether.
                if not directive.ops:
                    continue
            elif isinstance(directive, ops.DropIndexOp) and \
                    (directive.table_name, directive.schema) in tables_dropped:
                # we found a target DropIndexOp.   keep looping
                continue

            # otherwise if not filtered, yield out the directive
            yield directive

    connectable = config.attributes.get('connection', None)

    if connectable is None:
        # only create Engine if we don't have a Connection
        # from the outside
        connectable = create_engine(database_engine)

    with connectable.connect() as connection:
        if isinstance(connection.connection, sqlite3.Connection):
            connection.execute('PRAGMA foreign_keys=OFF')

        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            process_revision_directives=process_revision_directives,
            render_as_batch=True)

        with context.begin_transaction():
            context.run_migrations()

        if isinstance(connection.connection, sqlite3.Connection):
            connection.execute('PRAGMA foreign_keys=ON')


if not context.is_offline_mode():
    run_migrations_online()
else:
    raise RuntimeError('Offline migrations are not supported.')
