"""INIT

Revision ID: 376615bb5319
Revises:
Create Date: 2024-10-07 13:20:12.171995

"""
from alembic import op
from ckan.model import meta
from sqlalchemy import orm
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '376615bb5319'
down_revision = None
branch_labels = None
depends_on = None

package_table = sa.Table(
    "package",
    meta.MetaData(),
    sa.Column("id", sa.types.UnicodeText,
              primary_key=True),
)

falkor_dataset_sync_table = sa.Table(
    "falkor_dataset_sync",
    meta.MetaData(),
    sa.Column("id", sa.TEXT, sa.ForeignKey(
        "package.id"), primary_key=True)
)


def upgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)

    op.create_table(
        "falkor_dataset_sync",
        sa.Column("id", sa.TEXT, sa.ForeignKey(
            "package.id"), primary_key=True)
    )

    for package in session.query(package_table):
        session.execute(
            falkor_dataset_sync_table.insert().values(id=package[0]))

    session.commit()

    # model.package.package_table


def downgrade():
    op.drop_table("falkor_dataset_sync")
