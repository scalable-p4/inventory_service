from databases import Database
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import UUID

from src.config import settings
from src.constants import DB_NAMING_CONVENTION

DATABASE_URL_INVENTORY = settings.DATABASE_URL_INVENTORY

engine = create_engine(DATABASE_URL_INVENTORY)
metadata = MetaData(naming_convention=DB_NAMING_CONVENTION)

database = Database(DATABASE_URL_INVENTORY, force_rollback=settings.ENVIRONMENT.is_testing)

token_record = Table(
    "token_record",
    metadata,
    Column("uuid", Integer, Identity(), primary_key=True),
    Column("amount_available", Integer, nullable=False),
    Column("amount_taken", Integer, nullable=False),
    Column("amount_left", Integer, nullable=False),
    Column("username", String, nullable=False),
)

metadata.create_all(engine)


