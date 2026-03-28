"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from typing import Sequence, Union
<<<<<<< HEAD
=======

>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

<<<<<<< HEAD
=======
# revision identifiers, used by Alembic.
>>>>>>> 0869b5537c8feab5210ece8b099d72c680234530
revision: str = ${repr(up_revision)}
down_revision: Union[str, None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
