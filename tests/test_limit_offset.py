__author__ = "James Waterworth"
"""
Tests to valdiate that LIMIT and OFFSET values are cached
at the appropriate stage of query generation
"""

import sqlalchemy as sa
from packaging.version import Version

from rs_sqla_test_utils.utils import clean, compile_query

sa_version = Version(sa.__version__)

meta = sa.MetaData()

items = sa.Table(
    "items",
    meta,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
    sa.Column("order_id", sa.Integer),
    sa.Column("product_id", sa.Integer),
    sa.Column("name", sa.String(255)),
    sa.Column("qty", sa.Numeric(12, 4)),
    sa.Column("price", sa.Numeric(12, 4)),
    sa.Column("total_invoiced", sa.Numeric(12, 4)),
    sa.Column("discount_invoiced", sa.Numeric(12, 4)),
    sa.Column("grandtotal_invoiced", sa.Numeric(12, 4)),
    sa.Column("created_at", sa.DateTime),
    sa.Column("updated_at", sa.DateTime),
)


def test_select_1_item(stub_redshift_dialect):
    query = sa.select(items.c.id).select_from(items).limit(1).offset(0)

    assert (
        clean(compile_query(query, stub_redshift_dialect))
        == "SELECT items.id FROM items LIMIT 1 OFFSET 0"
    )


def test_limit_offset_values_not_cached(stub_redshift_dialect):
    query = sa.select(items.c.id).select_from(items).limit(1).offset(0)

    # compile our first query to get it cached
    compile_query(query, stub_redshift_dialect)

    second_query = sa.select(items.c.id).select_from(items).limit(2).offset(1)

    # perform the same query, it should use the cached version but add the
    # limit, offset values seperately
    assert (
        clean(compile_query(second_query, stub_redshift_dialect))
        == "SELECT items.id FROM items LIMIT 2 OFFSET 1"
    )
