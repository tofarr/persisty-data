"""Initial Configuration

Revision ID: 68631f5bb825
Revises: 
Create Date: 2023-10-20 11:51:14.409535

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "68631f5bb825"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "data_chunk",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("upload_id", sa.Text(), nullable=False),
        sa.Column("part_number", sa.Integer(), nullable=False),
        sa.Column("chunk_number", sa.Integer(), nullable=False),
        sa.Column("sort_key", sa.Integer(), nullable=False),
        sa.Column("data", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx__data_chunk__upload_id__sort_key",
        "data_chunk",
        ["upload_id", "sort_key"],
        unique=True,
    )
    op.create_table(
        "message",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("message_text", sa.Text(), nullable=False),
        sa.Column(
            "message_state", sa.Enum("FEATURED", name="messagestate"), nullable=True
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("author_id", sa.String(length=36), nullable=False),
        sa.Column("message_image_key", sa.Text(), server_default="NULL", nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "persisty_file_handle",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("store_name", sa.Text(), nullable=False),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column(
            "upload_id", sa.String(length=36), server_default="NULL", nullable=True
        ),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("etag", sa.Text(), nullable=False),
        sa.Column("size_in_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx__persisty_file_handle__store_name__file_name",
        "persisty_file_handle",
        ["store_name", "file_name"],
        unique=True,
    )
    op.create_table(
        "persisty_upload_handle",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("store_name", sa.Text(), nullable=False),
        sa.Column("file_name", sa.Text(), nullable=False),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("expire_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx__persisty_upload_handle__store_name__file_name",
        "persisty_upload_handle",
        ["store_name", "file_name"],
        unique=True,
    )
    op.create_table(
        "persisty_upload_part",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("upload_id", sa.Text(), nullable=False),
        sa.Column("part_number", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx__persisty_upload_part__upload_id__part_number",
        "persisty_upload_part",
        ["upload_id", "part_number"],
        unique=True,
    )
    op.create_table(
        "user",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column(
            "full_name", sa.String(length=255), server_default="NULL", nullable=False
        ),
        sa.Column(
            "email_address",
            sa.String(length=255),
            server_default="NULL",
            nullable=False,
        ),
        sa.Column("password_digest", sa.Text(), nullable=False),
        sa.Column("admin", sa.Boolean(), server_default="False", nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx__user__username", "user", ["username"], unique=True)
    # ### end Alembic commands ###
    from persisty.migration.alembic import add_seed_data

    add_seed_data(op)


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("idx__user__username", table_name="user")
    op.drop_table("user")
    op.drop_index(
        "idx__persisty_upload_part__upload_id__part_number",
        table_name="persisty_upload_part",
    )
    op.drop_table("persisty_upload_part")
    op.drop_index(
        "idx__persisty_upload_handle__store_name__file_name",
        table_name="persisty_upload_handle",
    )
    op.drop_table("persisty_upload_handle")
    op.drop_index(
        "idx__persisty_file_handle__store_name__file_name",
        table_name="persisty_file_handle",
    )
    op.drop_table("persisty_file_handle")
    op.drop_table("message")
    op.drop_index("idx__data_chunk__upload_id__sort_key", table_name="data_chunk")
    op.drop_table("data_chunk")
    # ### end Alembic commands ###
