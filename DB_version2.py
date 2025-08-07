import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
import json
import re

# === Step 1: Fetch Data from GTech API ===
url = "https://shafferapi.gtech.com.pk/api/post/SalesReport?dateFrom=01/01/2024&DateTo=/31/2024&api=qTpq3bVFho"
response = requests.get(url)

# === Step 2: Decode JSON safely and robustly ===
try:
    data = response.json()
except json.JSONDecodeError:
    # Escape ONLY invalid backslashes (not \n, \t, etc.)
    bad_json = response.text
    cleaned_text = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', bad_json)

    try:
        data = json.loads(cleaned_text)
    except Exception as e:
        print("❌ Still failed to parse JSON:", e)
        print("⚠️ Response preview:", cleaned_text[:500])
        raise e
    
# === Step 3: Convert JSON to DataFrame ===
df = pd.json_normalize(data)

# === Step 4: Clean column names ===
postgres_reserved_keywords = {"group", "order", "select", "user", "where", "table", "from", "join", "by"}

def clean_column(col):
    col_clean = re.sub(r'[^\w]', '_', col)
    col_clean = re.sub(r'_+', '_', col_clean)
    col_clean = col_clean.strip('_')
    if col_clean.lower() in postgres_reserved_keywords:
        col_clean += "_"
    return col_clean

cleaned_columns = []
seen = set()
for col in df.columns:
    clean_col = clean_column(col)
    if clean_col in seen:
        suffix = 1
        while f"{clean_col}_{suffix}" in seen:
            suffix += 1
        clean_col = f"{clean_col}_{suffix}"
    cleaned_columns.append(clean_col)
    seen.add(clean_col)

df.columns = cleaned_columns
print("✅ Cleaned Columns:", cleaned_columns)

# === Step 5: Connect to PostgreSQL ===
conn = psycopg2.connect(
    host="localhost",
    database="Sales_DB",
    user="postgres",
    password="0342",
    port="5432"
)
cur = conn.cursor()

# === Step 6: Infer PostgreSQL data types ===
def infer_pg_type(column_name):
    col_lower = column_name.lower()
    if "date" in col_lower and col_lower != "billmonth":
        return "DATE"
    elif "month" in col_lower:
        return "TEXT"  # Month is usually not a date format
    elif any(x in col_lower for x in ["qty", "points"]):
        return "INTEGER"
    elif any(x in col_lower for x in ["amount", "price", "gst", "disc", "charges", "percent", "volume"]):
        return "NUMERIC(10,2)"
    else:
        return "TEXT"

table_name = 'sales_data'
columns_with_types = [sql.SQL(f'"{col}" {infer_pg_type(col)}') for col in df.columns]

create_table_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table} (
        {fields}
    )
""").format(
    table=sql.Identifier(table_name),
    fields=sql.SQL(', ').join(columns_with_types)
)

cur.execute(create_table_query)
conn.commit()
print(f"✅ Table '{table_name}' created successfully.")

# === Step 7: Clean BillDate column ===
if 'BillDate' in df.columns:
    df['BillDate'] = pd.to_datetime(df['BillDate'], errors='coerce').dt.date

# === Step 8: Clean values before insertion ===
def clean_value(value, expected_type="text"):
    if pd.isna(value) or (isinstance(value, str) and value.strip().upper() in ["N/A", "", "NA"]):
        return None
    if expected_type == "numeric":
        try:
            return float(value)
        except:
            return None
    elif expected_type == "integer":
        try:
            return int(float(value))
        except:
            return None
    elif expected_type == "date":
        try:
            return pd.to_datetime(value).date()
        except:
            return None
    else:
        return str(value).strip()

# Infer type map for each column
column_types = {col: infer_pg_type(col) for col in df.columns}

# === Step 9: Insert cleaned rows ===
for row in df.itertuples(index=False, name=None):
    cleaned_row = [
        clean_value(val, column_types[df.columns[idx]].split('(')[0].lower())
        for idx, val in enumerate(row)
    ]
    insert_query = sql.SQL("""
        INSERT INTO {table} ({columns}) VALUES ({values})
    """).format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(', ').join(map(sql.Identifier, df.columns)),
        values=sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
    )
    try:
        cur.execute(insert_query, cleaned_row)
    except Exception as e:
        print(f"❌ Error inserting row: {cleaned_row}\nError: {e}")

conn.commit()

# === Step 10: Close connection ===
cur.close()
conn.close()

print("✅ Data successfully inserted into PostgreSQL!")
