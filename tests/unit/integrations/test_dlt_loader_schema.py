from __future__ import annotations

from typing import Optional

from sqlmodel import Field, SQLModel

from consist.integrations.dlt_loader import _sqlmodel_to_dlt_columns


class _SchemaTypes(SQLModel, table=True):
    __tablename__ = "schema_types"

    id: Optional[int] = Field(default=None, primary_key=True)
    score: Optional[float] = None
    flag: Optional[bool] = None
    note: Optional[str] = None


def test_sqlmodel_to_dlt_columns_optional_types() -> None:
    columns = _sqlmodel_to_dlt_columns(_SchemaTypes)

    assert columns["id"]["data_type"] == "bigint"
    assert columns["score"]["data_type"] == "double"
    assert columns["flag"]["data_type"] == "bool"
    assert columns["note"]["data_type"] == "text"
