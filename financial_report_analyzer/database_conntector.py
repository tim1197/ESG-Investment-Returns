import pandas as pd
from sqlalchemy import create_engine


DB_PASSWORD = "test"
DB_PATH = f"postgresql://postgres:{DB_PASSWORD}@localhost:5432/esg"
DEFAULT_TABLE = "scores"

class DatabaseConnector:
    def __init__(self, db_path=DB_PATH, db_password=DB_PASSWORD, default_table=DEFAULT_TABLE):
        self.engine = create_engine(db_path)
        self.table_names = (
            pd.read_sql(
                """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            """,
                self.engine,
            ),
        )
        self.default_table = default_table

    def fetch_data(self, table=DEFAULT_TABLE) -> pd.DataFrame:
        return pd.read_sql_table(table, self.engine)

    def store_data(self, df, table=DEFAULT_TABLE) -> None:
        df.to_sql(table, self.engine, if_exists="replace", index=False, chunksize=100)
        return None
