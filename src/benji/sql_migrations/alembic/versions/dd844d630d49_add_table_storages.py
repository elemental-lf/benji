"""Add table storages

Revision ID: dd844d630d49
Revises: 2bb97229fe36
Create Date: 2019-10-01 00:06:35.657495

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'dd844d630d49'
down_revision = '2bb97229fe36'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('storages', sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('name', sa.String(length=255), nullable=False),
                    sa.PrimaryKeyConstraint('id', name=op.f('pk_storages')),
                    sa.UniqueConstraint('name', name=op.f('uq_storages_name')))
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.create_foreign_key(batch_op.f('fk_versions_storage_id_storages'), 'storages', ['storage_id'], ['id'])
    with op.batch_alter_table('deleted_blocks', schema=None) as batch_op:
        batch_op.create_foreign_key(batch_op.f('fk_deleted_blocks_storage_id_storages'), 'storages', ['storage_id'], ['id'])


def downgrade():
    pass
