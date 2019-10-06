"""Rename snapshot_name to snapshot in table versions

Revision ID: 013dd9461e2c
Revises: 368014edd88c
Create Date: 2019-09-27 21:28:50.744771

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '013dd9461e2c'
down_revision = '368014edd88c'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.alter_column('snapshot_name',
                              new_column_name='snapshot',
                              existing_type=sa.String(255),
                              existing_nullable=False)


def downgrade():
    pass
