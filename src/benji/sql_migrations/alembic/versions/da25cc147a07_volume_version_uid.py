"""volume_version_uid

Revision ID: da25cc147a07
Revises: dd844d630d49
Create Date: 2019-10-28 15:20:15.455215

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'da25cc147a07'
down_revision = 'dd844d630d49'
branch_labels = None
depends_on = None


def upgrade():
    # Constraint names need to be globally unique with PostgreSQL (and they share the same namespace as tables)
    # For the benefit of PostgreSQL drop the primary key constraints before recreating them below to avoid collisions.
    # Requires PostgreSQL 9.2 or newer.
    if (op.get_context().dialect.name == 'postgresql'):
        op.execute('ALTER TABLE versions RENAME CONSTRAINT "pk_versions" TO "pk_versions_old"')
        op.execute('ALTER TABLE blocks RENAME CONSTRAINT "pk_blocks" TO "pk_blocks_old"')
        op.execute('ALTER TABLE labels RENAME CONSTRAINT "pk_labels" TO "pk_labels_old"')

    op.create_table('versions_new',
                    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('uid', sa.String(length=255), nullable=False),
                    sa.Column('date', sa.DateTime(), nullable=False),
                    sa.Column('volume', sa.String(length=255), nullable=False),
                    sa.Column('snapshot', sa.String(length=255), nullable=False),
                    sa.Column('size', sa.BigInteger(), nullable=False),
                    sa.Column('block_size', sa.Integer(), nullable=False),
                    sa.Column('storage_id', sa.Integer(), nullable=False),
                    sa.Column('status', sa.Integer(), nullable=False),
                    sa.Column('protected', sa.Boolean(name=op.f('ck_versions_protected')), nullable=False),
                    sa.Column('bytes_read', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_written', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_dedup', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_sparse', sa.BigInteger(), nullable=True),
                    sa.Column('duration', sa.BigInteger(), nullable=True),
                    sa.ForeignKeyConstraint(['storage_id'], ['storages.id'],
                                            name=op.f('fk_versions_storage_id_storages')),
                    sa.PrimaryKeyConstraint('id', name=op.f('pk_versions')),
                    sa.UniqueConstraint('uid', name=op.f('uq_versions_uid')),
                    sqlite_autoincrement=True)
    with op.batch_alter_table('versions_new', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_versions_volume'), ['volume'], unique=False)

    conn = op.get_bind()
    metadata = sa.MetaData()
    versions = sa.Table('versions', metadata, autoload_with=conn)
    versions_new = sa.Table('versions_new', metadata, autoload_with=conn)

    for version in conn.execute(versions.select()):
        conn.execute(versions_new.insert().values(id=version.uid,
                                                  uid=f'V{version.uid:010d}',
                                                  date=version.date,
                                                  volume=version.name,
                                                  snapshot=version.snapshot,
                                                  size=version.size,
                                                  block_size=version.block_size,
                                                  storage_id=version.storage_id,
                                                  status=version.status,
                                                  protected=version.protected,
                                                  bytes_read=version.bytes_read,
                                                  bytes_written=version.bytes_written,
                                                  bytes_dedup=version.bytes_dedup,
                                                  bytes_sparse=version.bytes_sparse,
                                                  duration=version.duration))

    # Reset sequence for PostgreSQL
    # Source: https://stackoverflow.com/questions/244243/how-to-reset-postgres-primary-key-sequence-when-it-falls-out-of-sync
    if (op.get_context().dialect.name == 'postgresql'):
        op.execute('SELECT setval(pg_get_serial_sequence(\'versions_new\', \'id\'), COALESCE(MAX(id), 0) + 1, false) FROM versions_new')

    with op.batch_alter_table('blocks', schema=None) as batch_op:
        batch_op.drop_constraint('fk_blocks_version_uid_versions', type_='foreignkey')

    with op.batch_alter_table('labels', schema=None) as batch_op:
        batch_op.drop_constraint('fk_labels_version_uid_versions', type_='foreignkey')

    op.drop_table('versions')
    op.rename_table('versions_new', 'versions')
    # Remove old table schema from metadata and reload it (keep_existing=False didn't work for some reason)
    metadata.remove(versions)
    versions = sa.Table('versions', metadata, autoload_with=conn)

    # Blocks

    op.create_table(
        'blocks_new', sa.Column('idx', sa.Integer(), nullable=False), sa.Column('uid_right',
                                                                                sa.Integer(),
                                                                                nullable=True),
        sa.Column('uid_left', sa.Integer(), nullable=True), sa.Column('size', sa.Integer(), nullable=True),
        sa.Column('version_id', sa.Integer(), nullable=False),
        sa.Column('valid', sa.Boolean(name=op.f('ck_blocks_valid')), nullable=False),
        sa.Column('checksum', sa.LargeBinary(length=64), nullable=True),
        sa.ForeignKeyConstraint(['version_id'], ['versions.id'],
                                name=op.f('fk_blocks_version_id_versions'),
                                ondelete='CASCADE'), sa.PrimaryKeyConstraint('version_id',
                                                                             'idx',
                                                                             name=op.f('pk_blocks')))

    with op.batch_alter_table('blocks', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_blocks_checksum'))
        batch_op.drop_index(batch_op.f('ix_blocks_uid_left'))
    with op.batch_alter_table('blocks_new', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_blocks_checksum'), ['checksum'], unique=False)
        batch_op.create_index(batch_op.f('ix_blocks_uid_left'), ['uid_left', 'uid_right'], unique=False)

    blocks = sa.Table('blocks', metadata, autoload_with=conn)
    blocks_new = sa.Table('blocks_new', metadata, autoload_with=conn)

    for block in conn.execute(blocks.select()):
        conn.execute(blocks_new.insert().values(idx=block.idx,
                                                uid_left=block.uid_left,
                                                uid_right=block.uid_right,
                                                size=block.size,
                                                version_id=block.version_uid,
                                                valid=block.valid,
                                                checksum=block.checksum))

    op.drop_table('blocks')
    op.rename_table('blocks_new', 'blocks')

    # Labels

    op.create_table(
        'labels_new', sa.Column('version_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=255), nullable=False),
        sa.Column('value', sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(['version_id'], ['versions.id'],
                                name=op.f('fk_labels_version_id_versions'),
                                ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('version_id', 'name', name=op.f('pk_labels')))
    with op.batch_alter_table('labels', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_labels_value'))
    with op.batch_alter_table('labels_new', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_labels_name'), ['name'], unique=False)
        batch_op.create_index(batch_op.f('ix_labels_value'), ['value'], unique=False)

    labels = sa.Table('labels', metadata, autoload_with=conn)
    labels_new = sa.Table('labels_new', metadata, autoload_with=conn)

    for label in conn.execute(labels.select()):
        conn.execute(labels_new.insert().values(name=label.name, value=label.value, version_id=label.version_uid))

    op.drop_table('labels')
    op.rename_table('labels_new', 'labels')


def downgrade():
    pass
