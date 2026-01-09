/*
------------------------------------------------------------------------------------
Module Name: create_retail_sales_tables
------------------------------------------------------------------------------------
This script defines the **warehouse schema** for the Retail Sales Analytics system.

It creates all **dimension and fact tables** required to support analytical queries,
with fully materialized derived columns produced during Transform Phase-2 (Model).

------------------------------------------------------------------------------------
Schema Coverage
------------------------------------------------------------------------------------
Dimension Tables:
- customers_dim   : Customer master data with tenure and classification attributes
- products_dim    : Product master data with pricing and category attributes
- stores_dim      : Store master data with geographic attributes
- date_dim        : Calendar dimension derived from sales activity

Fact Tables:
- sales_fact      : Transaction-level sales facts

------------------------------------------------------------------------------------
Design Notes
------------------------------------------------------------------------------------
- Designed for SQLite-based analytical warehouse
- All tables use explicit PRIMARY KEY definitions
- Foreign keys enforce dimensional integrity
- Derived columns are stored physically (not computed at query time)
- This script is idempotent and safe to re-run
------------------------------------------------------------------------------------
*/

-- =================================================================================
-- Dimension Table: customers_dim
-- Grain: One row per customer
-- =================================================================================
CREATE TABLE IF NOT EXISTS customers_dim (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    city TEXT,
    signup_date TEXT NOT NULL,

    customer_full_name TEXT NOT NULL,
    customer_tenure_days INTEGER NOT NULL,
    customer_tenure_bucket TEXT NOT NULL,
    email_domain TEXT
);

-- =================================================================================
-- Dimension Table: products_dim
-- Grain: One row per product
-- =================================================================================
CREATE TABLE IF NOT EXISTS products_dim (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL,

    price_band TEXT NOT NULL,
    is_premium_product BOOLEAN NOT NULL,
    category_normalized TEXT NOT NULL
);

-- =================================================================================
-- Dimension Table: stores_dim
-- Grain: One row per store
-- =================================================================================
CREATE TABLE IF NOT EXISTS stores_dim (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,

    store_region TEXT NOT NULL,
    is_metro_store BOOLEAN NOT NULL
);

-- =================================================================================
-- Dimension Table: date_dim
-- Grain: One row per calendar date
-- =================================================================================
CREATE TABLE IF NOT EXISTS date_dim (
    date TEXT PRIMARY KEY,

    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year_month TEXT NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    quarter INTEGER NOT NULL
);

-- =================================================================================
-- Fact Table: sales_fact
-- Grain: One row per sales transaction
-- =================================================================================
CREATE TABLE IF NOT EXISTS sales_fact (
    sale_id TEXT PRIMARY KEY,
    sale_date TEXT NOT NULL,

    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    store_id TEXT NOT NULL,

    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount_pct REAL NOT NULL,

    gross_amount REAL NOT NULL,
    discount_amount REAL NOT NULL,
    net_amount REAL NOT NULL,
    is_discounted BOOLEAN NOT NULL,

    order_year INTEGER NOT NULL,
    order_month TEXT NOT NULL,

    FOREIGN KEY (sale_date) REFERENCES date_dim(date),
    FOREIGN KEY (customer_id) REFERENCES customers_dim(customer_id),
    FOREIGN KEY (product_id) REFERENCES products_dim(product_id),
    FOREIGN KEY (store_id) REFERENCES stores_dim(store_id)
);
