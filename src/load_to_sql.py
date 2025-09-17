from sqlalchemy import create_engine, text
import pandas as pd

# ------------------------------
# 1️⃣ Connect to PostgreSQL
# ------------------------------
engine = create_engine('postgresql+psycopg2://postgres:nihad@localhost:5432/commercedb')

# ------------------------------
# 2️⃣ Drop old tables
# ------------------------------
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS staging_sales CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS fact_sales CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS dim_product CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS customer_dim CASCADE"))
    conn.execute(text("DROP TABLE IF EXISTS dim_date CASCADE"))
    conn.commit()
print("Old tables dropped.")

# ------------------------------
# 3️⃣ Create staging table = same as CSV
# ------------------------------
create_staging_sql = """
CREATE TABLE staging_sales (
    invoice_no text,
    stockcode text,
    description text,
    quantity int,
    invoicedate timestamp,
    unitprice numeric,
    customerid bigint,
    country text,
    totalprice numeric
);
"""
with engine.connect() as conn:
    conn.execute(text(create_staging_sql))
    conn.commit()
print("Staging table created.")

# ------------------------------
# 4️⃣ Load CSV into staging_sales
# ------------------------------
df = pd.read_csv('data/clean/cleaned_data.csv', parse_dates=['InvoiceDate'])

# normalize column names to match staging
df.rename(columns={
    'InvoiceNo': 'invoice_no',
    'StockCode': 'stockcode',
    'Description': 'description',
    'Quantity': 'quantity',
    'InvoiceDate': 'invoicedate',
    'UnitPrice': 'unitprice',
    'CustomerID': 'customerid',
    'Country': 'country',
    'TotalPrice': 'totalprice'
}, inplace=True)

df.to_sql('staging_sales', engine, if_exists='append', index=False, method='multi', chunksize=5000)
print("CSV data loaded into staging table.")

# ------------------------------
# 5️⃣ Create star schema tables
# ------------------------------
create_star_sql = """
CREATE TABLE dim_product (
    product_id text PRIMARY KEY,
    description text,
    stock_code text
);

CREATE TABLE customer_dim (
    customer_id bigint PRIMARY KEY,
    country text
);

CREATE TABLE dim_date (
    date_id date PRIMARY KEY,
    year int,
    month int,
    day int,
    weekday int
);

CREATE TABLE fact_sales (
    invoice_no text,
    date_id date,
    customer_id bigint,
    product_id text,
    quantity int,
    unit_price numeric,
    total_price numeric,
    FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);
"""
with engine.connect() as conn:
    conn.execute(text(create_star_sql))
    conn.commit()
print("Star schema tables created.")

# ------------------------------
# 6️⃣ Populate dimensions from staging
# ------------------------------

# dim_product
with engine.connect() as conn:
    conn.execute(text("""
       INSERT INTO dim_product (product_id, description, stock_code)
SELECT stockcode,
       MIN(description) AS description,
       stockcode
FROM staging_sales
WHERE stockcode IS NOT NULL
GROUP BY stockcode;

    """))
    conn.commit()

# customer_dim
# customer_dim
with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO customer_dim (customer_id, country)
        SELECT customerid,
               MIN(country) AS country
        FROM staging_sales
        WHERE customerid IS NOT NULL
        GROUP BY customerid;
    """))
    conn.commit()


# dim_date
# dim_date
with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO dim_date (date_id, year, month, day, weekday)
        SELECT CAST(invoicedate AS date) AS date_id,
               EXTRACT(YEAR FROM invoicedate)::int,
               EXTRACT(MONTH FROM invoicedate)::int,
               EXTRACT(DAY FROM invoicedate)::int,
               EXTRACT(DOW FROM invoicedate)::int
        FROM staging_sales
        WHERE invoicedate IS NOT NULL
        GROUP BY CAST(invoicedate AS date),
                 EXTRACT(YEAR FROM invoicedate),
                 EXTRACT(MONTH FROM invoicedate),
                 EXTRACT(DAY FROM invoicedate),
                 EXTRACT(DOW FROM invoicedate);
    """))
    conn.commit()


print("Dimensions populated.")

# ------------------------------
# 7️⃣ Populate fact_sales
# ------------------------------
# fact_sales
with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO fact_sales (invoice_no, date_id, customer_id, product_id, quantity, unit_price, total_price)
        SELECT invoice_no,
               CAST(invoicedate AS date),
               customerid,
               stockcode,
               quantity,
               unitprice,
               totalprice
        FROM staging_sales
        WHERE stockcode IS NOT NULL 
          AND customerid IS NOT NULL;
    """))
    conn.commit()


print("Fact table populated successfully.")
