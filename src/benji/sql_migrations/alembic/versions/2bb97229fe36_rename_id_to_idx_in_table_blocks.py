"""Rename id to idx in table blocks

Revision ID: 2bb97229fe36
Revises: 013dd9461e2c
Create Date: 2019-09-30 18:17:39.449984

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '2bb97229fe36'
down_revision = '013dd9461e2c'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('blocks', schema=None) as batch_op:
        batch_op.alter_column('id', new_column_name='idx', existing_type=sa.Integer, existing_nullable=False)


def downgrade():
    pass
