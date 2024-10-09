"""INIT

Revision ID: 376615bb5319
Revises:
Create Date: 2024-10-07 13:20:12.171995

"""
from alembic import op
from ckan.model import meta
import sqlalchemy as sa

import logging

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '376615bb5319'
down_revision = None
branch_labels = None
depends_on = None

# package_table = sa.Table(
#     "package",
#     meta.MetaData(),
#     sa.Column("id", sa.types.UnicodeText,
#               primary_key=True),
# )


def upgrade():
    # bind = op.get_bind()
    #
    # session = orm.Session(bind=bind)
    # try:
    # falkor_dataset_sync_table =
    op.create_table(
        "falkor_event",
        meta.MetaData(),
        sa.Column("id", sa.TEXT, primary_key=True, nullable=False),
        sa.Column("object_id", sa.TEXT, nullable=False),
        sa.Column("object_type", sa.TEXT, nullable=False),
        sa.Column("status", sa.TEXT, default="NOT_SYNCED"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("synced_at", sa.DateTime(), nullable=True, default=True)
    )

    # for package in session.query(package_table):
    #     session.execute(
    #   falkor_dataset_sync_table.insert().values(id=package[0]))
    #
    #    session.commit()
    #    except Exception as e:
    #    log.error(e)
    #    session.rollback()
    #    finally:
    #    session.close()

    # model.package.package_table


def downgrade():
    op.drop_table(
        "falkor_event"
    )
