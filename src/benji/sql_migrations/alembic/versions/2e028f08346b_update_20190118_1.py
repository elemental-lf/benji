"""update_20190118-1

Revision ID: 2e028f08346b
Revises: 
Create Date: 2019-01-18 16:54:13.525607

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '2e028f08346b'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('blocks', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_blocks_uid_left'), ['uid_left', 'uid_right'], unique=False)
        batch_op.drop_index('ix_blocks_uid_left_uid_right')

    with op.batch_alter_table('deleted_blocks', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_deleted_blocks_uid_left'), ['uid_left', 'uid_right'], unique=False)
        batch_op.drop_index('ix_blocks_uid_left_uid_right_2')

    with op.batch_alter_table('labels', schema=None) as batch_op:
        batch_op.alter_column('name', existing_type=sa.VARCHAR(), type_=sa.String(length=255))
        batch_op.alter_column('value', existing_type=sa.VARCHAR(), type_=sa.String(length=255), existing_nullable=False)
        batch_op.create_unique_constraint(batch_op.f('uq_labels_version_uid'), ['version_uid', 'name'])

    with op.batch_alter_table('locks', schema=None) as batch_op:
        batch_op.alter_column('host', existing_type=sa.VARCHAR(), type_=sa.String(length=255))
        batch_op.alter_column('lock_name', existing_type=sa.VARCHAR(), type_=sa.String(length=255))
        batch_op.alter_column('process_id', existing_type=sa.VARCHAR(), type_=sa.String(length=255))
        batch_op.alter_column(
            'reason', existing_type=sa.VARCHAR(), type_=sa.String(length=255), existing_nullable=False)

    with op.batch_alter_table('version_statistics', schema=None) as batch_op:
        batch_op.alter_column('name', existing_type=sa.VARCHAR(), type_=sa.String(length=255), existing_nullable=False)
        batch_op.alter_column(
            'snapshot_name', existing_type=sa.VARCHAR(), type_=sa.String(length=255), existing_nullable=False)

    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.alter_column('name', existing_type=sa.VARCHAR(), type_=sa.String(length=255), existing_nullable=False)
        batch_op.alter_column(
            'snapshot_name',
            existing_type=sa.VARCHAR(),
            server_default=None,
            type_=sa.String(length=255),
            existing_nullable=False)


def downgrade():
    pass
