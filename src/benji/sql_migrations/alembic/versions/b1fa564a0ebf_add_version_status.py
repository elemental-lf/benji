"""Add version status

Revision ID: b1fa564a0ebf
Revises: 2e028f08346b
Create Date: 2019-01-21 19:25:42.193956

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b1fa564a0ebf'
down_revision = '2e028f08346b'
branch_labels = None
depends_on = None

versions = sa.table('versions', sa.column('valid', sa.Boolean), sa.column('status', sa.Integer))


def upgrade():
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.add_column(sa.Column('status', sa.Integer, nullable=False, server_default=sa.text('2')))
        batch_op.create_check_constraint('status', 'status >= 1 AND status <= 3')

    op.execute(versions.update().where(versions.c.valid == op.inline_literal(False, type_=sa.Boolean)).values({
        'status':
        op.inline_literal(3)
    }))

    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.drop_column('valid')


def downgrade():
    pass
