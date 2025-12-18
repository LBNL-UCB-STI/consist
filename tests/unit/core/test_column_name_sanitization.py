from __future__ import annotations

from sqlalchemy import Column, String, text
from sqlmodel import Field, SQLModel


def test_typed_empty_view_uses_original_column_names(tracker):
    class WeirdTable(SQLModel, table=True):
        __tablename__ = "weird_table"
        __table_args__ = {"extend_existing": True}

        mass_kg: float | None = Field(
            default=None, sa_column=Column("Mass(kg)", String, nullable=True)
        )

    view_cls = tracker.view(WeirdTable)
    assert view_cls.__tablename__ == "v_weird_table"

    with tracker.engine.begin() as conn:
        cols = conn.exec_driver_sql("PRAGMA table_info('v_weird_table')").fetchall()
    col_names = {str(row[1]) for row in cols}
    assert "Mass(kg)" in col_names

    # Should be queryable even when empty.
    with tracker.engine.begin() as conn:
        conn.execute(text('SELECT "Mass(kg)" FROM v_weird_table WHERE 1=0'))
