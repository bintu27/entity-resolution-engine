import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent


def get_engine(env_var: str, fallback: Optional[str] = None) -> Engine:
    url = os.getenv(env_var, fallback)
    if not url:
        raise RuntimeError(f"Database URL for {env_var} is not configured")
    return create_engine(url, echo=False, future=True)


def init_db(engine: Engine, schema_file: str) -> None:
    path = BASE_DIR / schema_file
    sql = path.read_text()
    with engine.connect() as conn:
        for statement in sql.split(';'):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()
