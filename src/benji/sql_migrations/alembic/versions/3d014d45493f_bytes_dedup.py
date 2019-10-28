"""bytes_dedup

Revision ID: 3d014d45493f
Revises: da25cc147a07
Create Date: 2019-10-28 19:52:25.464697

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '3d014d45493f'
down_revision = 'da25cc147a07'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.alter_column('bytes_dedup',
                              new_column_name='bytes_deduplicated',
                              existing_type=sa.BigInteger(),
                              existing_nullable=True)


def downgrade():
    pass
