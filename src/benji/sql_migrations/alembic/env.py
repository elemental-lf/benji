from alembic import context
from sqlalchemy import create_engine

from benji.database import Base
from benji.config import Config as BenjiConfig

config = context.config

# Only load configuration when we're running standalone
if config.attributes.get('connection', None) is None:
    benji_config = BenjiConfig()
    database_engine = benji_config.get('databaseEngine', types=str)
else:
    database_engine = None

target_metadata = Base.metadata


def run_migrations_offline():
    if database_engine is None:
        raise RuntimeError('Offline migrations only work when directly running Alembic.')

    context.configure(url=database_engine, target_metadata=target_metadata, literal_binds=True)

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    connectable = config.attributes.get('connection', None)

    if connectable is None:
        # only create Engine if we don't have a Connection
        # from the outside
        connectable = create_engine(database_engine)

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
