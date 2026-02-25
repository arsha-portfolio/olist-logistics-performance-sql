/*
  PROJECT: Logistics Performance Analysis (Olist Dataset)
  OBJECTIVE: Calculate delivery latency and delay flags for fulfilled orders.
  STRATEGY: Pre-aggregate items to minimize Join load + Filter at source.
*/

CREATE TABLE FCT_Logistics_Performance AS
WITH 
-- 1. 預處理訂單項：先聚合金額，減少與主表 JOIN 時的資料筆數
aggregated_items AS (
    SELECT 
        order_id,
        SUM(price) AS total_goods_value,
        SUM(freight_value) AS total_freight_cost
    FROM olist_order_items_dataset
    GROUP BY order_id
),

-- 2. 清洗並篩選有效訂單
cleaned_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_purchase_timestamp,
        order_estimated_delivery_date,
        order_delivered_customer_date
    FROM olist_orders_dataset
    WHERE order_status = 'delivered' 
      AND order_delivered_customer_date IS NOT NULL
      AND order_purchase_timestamp IS NOT NULL
)

-- 3. 最終關聯與指標運算
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_purchase_timestamp,
    o.order_estimated_delivery_date,
    o.order_delivered_customer_date,
    
    -- 指標 A: 實際配送時效 (Actual Delivery Days)
    CAST(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp) AS INTEGER) AS actual_delivery_days,
    
    -- 指標 B: 延遲天數 (負值代表提前送達)
    CAST(julianday(o.order_delivered_customer_date) - julianday(o.order_estimated_delivery_date) AS INTEGER) AS delay_days,
    
    -- 指標 C: 是否延遲 (Binary Flag)
    CASE 
        WHEN julianday(o.order_delivered_customer_date) > julianday(o.order_estimated_delivery_date) THEN 1 
        ELSE 0 
    END AS is_delayed,
    
    oi.total_goods_value,
    oi.total_freight_cost

FROM cleaned_orders o
INNER JOIN olist_customers_dataset c ON o.customer_id = c.customer_id
LEFT JOIN aggregated_items oi ON o.order_id = oi.order_id;
