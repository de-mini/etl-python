import pandas as pd
import datetime
import snowflake.connector
from dotenv import load_dotenv
import os
import time

load_dotenv()

def main(
    grocery_path,
    extra_data_path,
    account: str,
    user: str,
    password: str,
    role: str,
    warehouse: str,
    db: str,
    schema: str,
):
    merged_df = extract(grocery_path, extra_data_path)
    filename = clean_transform(merged_df)
    load_to_snowflake(account, user, password, role, warehouse, db, schema, filename)


def extract(grocery_path, extra_data_path):
    extra_df = pd.read_parquet(extra_data_path, engine="pyarrow")
    grocery_df = pd.read_csv(grocery_path)
    merged_df = grocery_df.merge(extra_df, on="index")

    return merged_df


def clean_transform(df):
    # change date to datetime
    df["Date"] = pd.to_datetime(df["Date"])

    # fill null values in Unemployment with mean
    df["Unemployment"].fillna(df["Unemployment"].mean(), inplace=True)

    # fill null values in weekly sales with 0
    df["Weekly_Sales"].fillna(0, inplace=True)

    filename = f"groceries_cleaned_{int(time.time())}.csv"
    df.to_csv(filename, index=False)
    return filename


def load_to_snowflake(
    account: str,
    user: str,
    password: str,
    role: str,
    warehouse: str,
    db: str,
    schema: str,
    file,
):
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=db,
        schema=schema,
    )
    cur = conn.cursor()
    # cur.execute("CREATE stage retail;")
    cur.execute(f"put file://{file} @retail")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS grocery_sales(
            index INT,
            store_id INT,
            date DATE,
            dept INT,
            weekly_sales REAL,
            isHoliday BOOLEAN,
            temperature REAL,
            fuel_price REAL,
            markdown1 REAL,
            markdown2 REAL,
            markdown3 REAL,
            markdown4 REAL,
            markdown5 REAL,
            cpi REAL,
            unemployment REAL,
            store_type REAL,
            store_size REAL
        );
    """
    )
    cur.execute(
        f"""
        COPY INTO grocery_sales
        FROM @retail/{file}
        file_format=(type='csv' skip_header=1)
        on_error='continue';
    """
    )

if __name__ == "__main__":
    grocery_path = "/home/brianoyollo/Desktop/extras/luxdevhq/retail-pipeline/GROCERY_SALES.csv"
    extra_data_path = "/home/brianoyollo/Desktop/extras/luxdevhq/retail-pipeline/extra_data1.parquet"
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    db = os.getenv("SNOWFLAKE_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    main(grocery_path, extra_data_path, account, user, password, role, warehouse, db, schema)



