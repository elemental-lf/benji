"""Remove stats table

Revision ID: 151248f94062
Revises: fe79ce75cefa
Create Date: 2019-03-28 13:08:27.087076

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '151248f94062'
down_revision = 'fe79ce75cefa'
branch_labels = None
depends_on = None

version_statistics = sa.table('version_statistics', sa.Column('uid', sa.Integer(), nullable=False),
                              sa.Column('bytes_dedup', sa.BigInteger(), nullable=True),
                              sa.Column('bytes_read', sa.BigInteger(), nullable=True),
                              sa.Column('bytes_sparse', sa.BigInteger(), nullable=True),
                              sa.Column('bytes_written', sa.BigInteger(), nullable=True),
                              sa.Column('duration', sa.BigInteger(), nullable=True))

versions = sa.table('versions', sa.Column('uid', sa.Integer(), nullable=False),
                    sa.Column('bytes_dedup', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_read', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_sparse', sa.BigInteger(), nullable=True),
                    sa.Column('bytes_written', sa.BigInteger(), nullable=True),
                    sa.Column('duration', sa.BigInteger(), nullable=True))


def upgrade():
    with op.batch_alter_table('versions', schema=None) as batch_op:
        batch_op.add_column(sa.Column('bytes_dedup', sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column('bytes_read', sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column('bytes_sparse', sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column('bytes_written', sa.BigInteger(), nullable=True))
        batch_op.add_column(sa.Column('duration', sa.BigInteger(), nullable=True))

    op.execute(versions.update().values({
        versions.c.bytes_dedup:
        sa.sql.select(columns=[version_statistics.c.bytes_dedup]).where(versions.c.uid == version_statistics.c.uid),
        versions.c.bytes_read:
        sa.sql.select(columns=[version_statistics.c.bytes_read]).where(versions.c.uid == version_statistics.c.uid),
        versions.c.bytes_sparse:
        sa.sql.select(columns=[version_statistics.c.bytes_sparse]).where(versions.c.uid == version_statistics.c.uid),
        versions.c.bytes_written:
        sa.sql.select(columns=[version_statistics.c.bytes_written]).where(versions.c.uid == version_statistics.c.uid),
        versions.c.duration:
        sa.sql.select(columns=[version_statistics.c.duration]).where(versions.c.uid == version_statistics.c.uid),
    }))

    op.drop_table('version_statistics')


def downgrade():
    pass
