"""config

Revision ID: 6b860291073e
Revises: 376615bb5319
Create Date: 2024-10-21 10:36:57.994360

"""
from alembic import op
from ckan.model import meta
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6b860291073e'
down_revision = '376615bb5319'
branch_labels = None
depends_on = None


TABLE_NAME = "falkor_config"


def upgrade():
    op.create_table(
        TABLE_NAME,
        meta.MetaData(),
        sa.Column("initialised", sa.Boolean, nullable=False, primary_key=True),
    )
    op.execute("""
        INSERT INTO falkor_config(initialised)
        VALUES (false)
    """)


def downgrade():
    op.drop_table(TABLE_NAME)
