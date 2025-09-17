-- Dimension Produit
CREATE TABLE IF NOT EXISTS dim_product (
    product_id text PRIMARY KEY,
    description text,
    stock_code text
);

-- Dimension Client
CREATE TABLE IF NOT EXISTS customer_dim (
    customer_id bigint PRIMARY KEY,
    country text
);

-- Dimension Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id date PRIMARY KEY,
    year int,
    month int,
    day int,
    weekday int
);

-- Table des faits
CREATE TABLE IF NOT EXISTS fact_sales (
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
