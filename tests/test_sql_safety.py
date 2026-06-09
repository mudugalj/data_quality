import pytest

from dq_engine.sql_safety import validate_read_only, validate_filter, SqlSafetyError


@pytest.mark.parametrize("sql", [
    "SELECT * FROM customers WHERE email IS NULL",
    "select avg(age) as v from customers",
    "WITH x AS (SELECT * FROM t) SELECT * FROM x",
    "SELECT * FROM customers WHERE email IS NULL;",  # single trailing semicolon ok
])
def test_accepts_read_only_queries(sql):
    assert validate_read_only(sql)


@pytest.mark.parametrize("sql", [
    "INSERT INTO customers VALUES (1)",
    "UPDATE customers SET age = 0",
    "DELETE FROM customers",
    "DROP TABLE customers",
    "CREATE TABLE t (a int)",
    "SELECT 1; DROP TABLE customers",          # multi-statement
    "SELECT * FROM t; SELECT * FROM u",        # multi-statement
    "",                                         # empty
    "   ",                                      # blank
])
def test_rejects_unsafe_sql(sql):
    with pytest.raises(SqlSafetyError):
        validate_read_only(sql)


def test_rejects_dml_hidden_in_comment_casing():
    # keyword-based denylist still catches this for the MVP
    with pytest.raises(SqlSafetyError):
        validate_read_only("SELECT * FROM t WHERE 1=1 /* */ ; DELETE FROM t")


def test_filter_accepts_predicate():
    assert validate_filter("status = 'active'") == "status = 'active'"


def test_filter_rejects_semicolon_and_dml():
    with pytest.raises(SqlSafetyError):
        validate_filter("status = 'active'; drop table t")
