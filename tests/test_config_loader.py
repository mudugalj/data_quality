import csv

from dq_engine import config_loader

CHECK_HEADER = config_loader.CHECK_FIELDS
SOURCE_HEADER = config_loader.SOURCE_FIELDS


def _write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def test_load_checks_valid_and_optional_fields(tmp_path):
    p = tmp_path / "checks.csv"
    _write_csv(p, CHECK_HEADER, [
        ["c1", "t", "col", "", "completeness", "", "", "", "SELECT * FROM t", "violation", ""],
    ])
    checks, rejects = config_loader.load_checks(str(p))
    assert rejects == []
    assert len(checks) == 1
    c = checks[0]
    assert c.app_code is None              # blank -> None
    assert c.deviation_tolerance == 0.0    # default
    assert c.data_type is None


def test_load_checks_rejects_missing_required(tmp_path):
    p = tmp_path / "checks.csv"
    _write_csv(p, CHECK_HEADER, [
        ["", "t", "col", "", "completeness", "", "", "", "SELECT 1", "violation", ""],
    ])
    checks, rejects = config_loader.load_checks(str(p))
    assert checks == []
    assert any("check_id" in r for r in rejects)


def test_load_checks_rejects_bad_enums_and_duplicates(tmp_path):
    p = tmp_path / "checks.csv"
    _write_csv(p, CHECK_HEADER, [
        ["c1", "t", "col", "", "bogus", "", "", "", "SELECT 1", "violation", ""],
        ["c2", "t", "col", "", "validity", "", "", "", "SELECT 1", "consistency_delta", ""],
        ["c3", "t", "col", "", "validity", "", "", "", "SELECT 1", "violation", ""],
        ["c3", "t", "col", "", "validity", "", "", "", "SELECT 1", "violation", ""],
    ])
    checks, rejects = config_loader.load_checks(str(p))
    ids = {c.check_id for c in checks}
    assert ids == {"c3"} or "c3" in ids  # first c3 kept, duplicate rejected
    assert any("check_type" in r for r in rejects)
    assert any("evaluation_mode" in r for r in rejects)
    assert any("duplicate" in r for r in rejects)


def test_load_catalog_validates_parquet_fields(tmp_path):
    p = tmp_path / "cat.csv"
    _write_csv(p, SOURCE_HEADER, [
        ["customers", "parquet", "data/customers", "date"],
        ["bad", "mariadb_table", "", ""],          # unsupported source type (MVP)
        ["nofields", "parquet", "", ""],           # parquet missing fields
    ])
    catalog, rejects = config_loader.load_catalog(str(p))
    assert "customers" in catalog
    assert "bad" not in catalog
    assert "nofields" not in catalog
    assert any("source_type" in r for r in rejects)
    assert any("parquet_location" in r for r in rejects)
