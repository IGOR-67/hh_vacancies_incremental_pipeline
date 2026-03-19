from pathlib import Path

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def load_sql(relative_path: str) -> str:
    path = SQL_DIR / relative_path
    return path.read_text(encoding="utf-8")