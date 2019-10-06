"""Fix locking design

Revision ID: fe79ce75cefa
Revises: b1fa564a0ebf
Create Date: 2019-01-28 23:19:47.958399

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = 'fe79ce75cefa'
down_revision = 'b1fa564a0ebf'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table('locks')
    op.create_table('locks', sa.Column('lock_name', sa.String(255), nullable=False, primary_key=True),
                    sa.Column('host', sa.String(255), nullable=False),
                    sa.Column('process_id', sa.String(255), nullable=False),
                    sa.Column('reason', sa.String(255), nullable=False), sa.Column('date', sa.DateTime, nullable=False))


def downgrade():
    pass
