"""Fix blocks primary key

Revision ID: 368014edd88c
Revises: 151248f94062
Create Date: 2019-04-01 11:27:18.183463

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '368014edd88c'
down_revision = '151248f94062'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('blocks', schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f('pk_blocks'))
        batch_op.create_primary_key(batch_op.f('pk_blocks'), ['version_uid', 'id'])


def downgrade():
    pass
