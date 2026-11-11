from __future__ import annotations

from sqlalchemy import Column, String
from sqlmodel import Field, SQLModel

from consist.core.views import ViewFactory


def test_typed_empty_view_uses_original_column_names(tracker):
    class WeirdTable(SQLModel, table=True):
        __tablename__ = "weird_table"
        __table_args__ = {"extend_existing": True}

        id: int | None = Field(default=None, primary_key=True)
        mass_kg: float | None = Field(
            default=None, sa_column=Column("Mass(kg)", String, nullable=True)
        )

    assert ViewFactory(tracker).create_hybrid_view(
        "v_weird_table",
        "weird_table",
        schema_model=WeirdTable,
    )

    assert tracker.ledger is not None
    with tracker.ledger.exec_cursor() as cur:
        cols = cur.execute("PRAGMA table_info('v_weird_table')").fetchall()
    col_names = {str(row[1]) for row in cols}
    assert "Mass(kg)" in col_names

    # Should be queryable even when empty.
    df = tracker.query_df('SELECT "Mass(kg)" FROM v_weird_table WHERE 1=0')
    assert list(df.columns) == ["Mass(kg)"]
