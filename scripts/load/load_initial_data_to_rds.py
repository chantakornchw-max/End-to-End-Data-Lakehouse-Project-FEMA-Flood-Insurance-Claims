import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import String, Integer, DateTime
from datetime import datetime
import os
import io
import csv
from dotenv import load_dotenv
import logging


load_dotenv()


def psql_insert_copy(table, conn, keys, data_iter):
    dbapi_conn = conn.connection

    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join([f'"{k}"' for k in keys])

        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'

        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)


def migrate_csv_to_rds():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    rds_endpoint = os.getenv("DB_ENDPOINT")
    db_port = os.getenv("DB_PORT")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")

    if not all([rds_endpoint, db_user, db_password, db_name]):
        raise ValueError(
            "Required database environment variables are missing."
        )

    db_url = (
        f"postgresql://{db_user}:{db_password}"
        f"@{rds_endpoint}:{db_port}/{db_name}"
    )

    engine = create_engine(db_url)

    csv_file_path = "data/_FimaNfipClaimsV2.csv"
    table_name = "raw_fema_claims"
    chunk_size = 50000

    # Baseline timestamp for the entire initial load
    initial_source_updated_at = datetime(2026, 9, 1, 10, 0, 0)

    logging.info("==================================================")
    logging.info("Starting migration: Local CSV -> AWS RDS")
    logging.info(f"Initial source_updated_at = {initial_source_updated_at}")
    logging.info("==================================================")

    try:
        for i, chunk_df in enumerate(
            pd.read_csv(
                csv_file_path,
                chunksize=chunk_size,
                low_memory=False
            )
        ):

            start_row = (i * chunk_size) + 1

            chunk_df["row_num"] = range(
                start_row,
                start_row + len(chunk_df)
            )

            # Add synthetic change-tracking timestamp
            chunk_df["source_updated_at"] = initial_source_updated_at

            type_mapping = {
                col: String
                for col in chunk_df.columns
            }

            type_mapping["row_num"] = Integer
            type_mapping["source_updated_at"] = DateTime

            logging.info(
                f"Processing chunk {i + 1}: "
                f"rows {start_row:,} to "
                f"{start_row + len(chunk_df) - 1:,}"
            )

            if i == 0:

                chunk_df.to_sql(
                    name=table_name,
                    con=engine,
                    schema="public",
                    if_exists="replace",
                    index=False,
                    dtype=type_mapping,
                    method=psql_insert_copy
                )

                logging.info(
                    f"Created table '{table_name}' "
                    f"and loaded first chunk successfully."
                )

            else:

                chunk_df.to_sql(
                    name=table_name,
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                    dtype=type_mapping,
                    method=psql_insert_copy
                )

                logging.info(
                    f"Loaded chunk {i + 1} successfully."
                )

        logging.info("==================================================")
        logging.info("Initial migration completed successfully.")
        logging.info("==================================================")

    except Exception as e:
        logging.error(f"Migration failed: {e}")
        raise


if __name__ == "__main__":
    migrate_csv_to_rds()