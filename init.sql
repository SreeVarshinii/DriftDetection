CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(50),
    emitted_at TIMESTAMP,
    schema_version INT,
    _rescued_data JSONB,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _schema_version INT
);

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_state VARCHAR(50),
    customer_city VARCHAR(255),
    emitted_at TIMESTAMP,
    schema_version INT,
    _rescued_data JSONB,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _schema_version INT
);

CREATE TABLE IF NOT EXISTS bronze.items (
    order_item_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255),
    price NUMERIC,
    freight_value NUMERIC,
    emitted_at TIMESTAMP,
    schema_version INT,
    _rescued_data JSONB,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _schema_version INT
);

CREATE TABLE IF NOT EXISTS bronze.payments (
    payment_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255),
    payment_type VARCHAR(50),
    payment_installments INT,
    emitted_at TIMESTAMP,
    schema_version INT,
    _rescued_data JSONB,
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _schema_version INT
);

-- SILVER LAYER
CREATE OR REPLACE VIEW silver.cleaned_orders AS
WITH order_items AS (
    SELECT 
        order_id,
        COUNT(order_item_id) AS item_count,
        SUM(COALESCE(price, (_rescued_data->>'item_price')::NUMERIC)) AS total_value,
        SUM(freight_value) AS freight_value,
        BOOL_OR(COALESCE((_rescued_data->>'burst_flag')::BOOLEAN, FALSE)) AS is_burst_event
    FROM bronze.items
    GROUP BY order_id
),
order_payments AS (
    SELECT 
        order_id,
        MAX(payment_type) AS payment_type,
        MAX(payment_installments) AS payment_installments
    FROM bronze.payments
    GROUP BY order_id
)
SELECT
    o.order_id,
    c.customer_state,
    c.customer_city,
    o.order_status,
    COALESCE(i.item_count, 0) AS item_count,
    COALESCE(i.total_value, 0) AS total_value,
    COALESCE(i.freight_value, 0) AS freight_value,
    p.payment_type,
    p.payment_installments,
    COALESCE(
        (o._rescued_data->>'original_timestamp')::TIMESTAMP,
        o.emitted_at
    ) AS event_timestamp,
    (o._rescued_data->>'loyalty_tier')::VARCHAR AS loyalty_tier,
    COALESCE(i.is_burst_event, FALSE) AS is_burst_event,
    o.schema_version,
    o._ingested_at
FROM bronze.orders o
LEFT JOIN bronze.customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN order_payments p ON o.order_id = p.order_id;

-- GOLD LAYER
CREATE OR REPLACE VIEW gold.state_revenue_features AS
SELECT
    customer_state,
    COUNT(order_id) AS total_orders,
    SUM(total_value) AS total_revenue,
    ROUND(AVG(total_value), 2) AS avg_order_value,
    SUM(CASE WHEN is_burst_event THEN 1 ELSE 0 END) AS suspicious_burst_count,
    MAX(event_timestamp) AS latest_activity_at
FROM silver.cleaned_orders
GROUP BY customer_state;

CREATE OR REPLACE VIEW gold.loyalty_tier_metrics AS
SELECT
    COALESCE(loyalty_tier, 'unassigned') AS tier,
    COUNT(order_id) AS order_count,
    SUM(total_value) AS total_revenue
FROM silver.cleaned_orders
GROUP BY COALESCE(loyalty_tier, 'unassigned');
