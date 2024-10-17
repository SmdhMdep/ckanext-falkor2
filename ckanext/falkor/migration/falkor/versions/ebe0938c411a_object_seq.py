"""object_seq

Revision ID: ebe0938c411a
Revises: 376615bb5319
Create Date: 2024-10-15 14:30:36.471133

"""
import sqlalchemy as sa

from alembic import op
from ckan.model import meta


# revision identifiers, used by Alembic.
revision = 'ebe0938c411a'
down_revision = '376615bb5319'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "falkor_object_event_sequence",
        meta.MetaData(),
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("sequence", sa.INTEGER, nullable=False)
    )

    op.create_foreign_key(
        "fk_falkor_event_object_id_falkor_object_event_sequence",
        "falkor_event",
        "falkor_object_event_sequence",
        ["object_id"],
        ["id"],
    )

    op.add_column("falkor_event", sa.Column(
        "sequence", sa.INTEGER, nullable=False))


def downgrade():
    op.drop_constraint(
        "fk_falkor_event_object_id_falkor_object_event_sequence",
        "falkor_event",
        type_="foreignkey"
    )
    op.drop_table("falkor_object_event_sequence")
    op.drop_column("falkor_event", "sequence")
