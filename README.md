# E-Commerce Logistics Performance Pipeline

## Project Overview
This project builds an optimised SQL data pipeline to analyse logistics performance, delivery latency, and SLA compliance for over 100,000+ e-commerce orders (based on the Olist dataset). The core objective is to identify fulfilment bottlenecks and calculate precise delivery metrics while minimising database compute costs.

## Tech Stack & Skills Demonstrated
* **Language:** SQL (SQLite / PostgreSQL compatible)
* **Techniques:** Common Table Expressions (CTEs), Filter Pushdown, Pre-aggregation, Window/Date Functions.
* **Domain:** Supply Chain Operations, Logistics Latency, E-commerce Analytics.

## Performance Optimisation (A/B Analysis)
To handle large-scale transaction data efficiently, the query logic was refactored from a traditional "Join-First, Group-Later" approach to a "Pre-aggregate & Filter" CTE architecture.

| Optimization Strategy | Legacy Approach (Suboptimal) | Optimized Approach (CTE Architecture) | ROI / Business Impact |
| :--- | :--- | :--- | :--- |
| **Data Volume Processing** | Joining full tables before filtering. | Filtering `delivered` status at the source (`cleaned_orders`). | **Significantly reduces Memory & I/O load** during JOIN operations. |
| **Aggregation Strategy** | Heavy `GROUP BY` at the final layer across millions of joined rows. | Pre-aggregating line-item costs at the `order_id` level first. | **Eliminates redundant Shuffle/Sort costs**, reducing query execution time. |
| **Maintainability** | Nested subqueries; difficult to debug. | Modular CTEs (Step 1 -> Step 2 -> Step 3). | **Lower maintenance cost**; logic can be easily updated or isolated for testing. |

## Key Business Metrics Extracted
The pipeline generates a denormalised fact table (`FCT_Logistics_Performance`) ready for Tableau ingestion, outputting the following KPIs:
1. **`actual_delivery_days`**: The end-to-end lead time from customer purchase to package arrival.
2. **`delay_days`**: Variance analysis against the Estimated Delivery Date (SLA). Negative values indicate early delivery.
3. **`is_delayed`**: A binary flag (1/0) used to calculate the overall On-Time Delivery (OTD) rate.
4. **`total_goods_value` & `total_freight_cost`**: Financial metrics tied directly to logistic nodes.

## Repository Structure
```text
├── scripts/
│   └── 01_logistics_performance.sql  # Core ETL/Transformation logic
├── README.md                         # Project documentation
