-- Populate product_dim (using only product_id for now, since description is not in fact_sales)
INSERT INTO dim_product (product_id, description, stock_code)
SELECT DISTINCT product_id, NULL AS description, product_id AS stock_code
FROM fact_sales
WHERE product_id IS NOT NULL;


-- Populate customer_dim
INSERT INTO customer_dim (customer_id, country)
SELECT DISTINCT customer_id, 'Unknown'
FROM fact_sales
WHERE customer_id IS NOT NULL;

SELECT date_trunc('month', invoice_date)::date AS month,
       SUM(total_price) AS revenue,
       COUNT(DISTINCT invoice_no) AS orders
FROM fact_sales
GROUP BY 1
ORDER BY 1;

/* Top 20 products by revenue */
SELECT f.product_id, d.description,
       SUM(f.quantity) AS qty_sold,
       SUM(f.total_price) AS revenue
FROM fact_sales f
LEFT JOIN dim_product d ON d.product_id = f.product_id
GROUP BY f.product_id, d.description
ORDER BY revenue DESC
LIMIT 20;


/* Top 50 customers by lifetime value */
SELECT customer_id, SUM(total_price) AS lifetime_value
FROM fact_sales
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 50;


/* RFM Analysis */
WITH cust AS (
  SELECT customer_id,
         MAX(invoice_date) AS last_purchase,
         COUNT(DISTINCT invoice_no) AS frequency,
         SUM(total_price) AS monetary
  FROM fact_sales
  GROUP BY customer_id
)
SELECT customer_id,
       (CURRENT_DATE - last_purchase::date) AS recency_days,
       frequency,
       monetary,
       ntile(5) OVER (ORDER BY (CURRENT_DATE - last_purchase::date)) AS recency_score,
       ntile(5) OVER (ORDER BY frequency DESC) AS frequency_score,
       ntile(5) OVER (ORDER BY monetary DESC) AS monetary_score
FROM cust;



