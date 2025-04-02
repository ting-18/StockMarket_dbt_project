# StockMarket_dbt_project

This is a side project for a Data Engineering project: Stock Market Insights https://github.com/ting-18/DEProject_StockMarketInsights


A standard dbt project (__dbt-SQL-Transformation__) focuses on transforming and modeling raw data into analytics-ready tables, i.e. defining clear layers of transformations that clean, aggregate, and structure stock market data for analytics and reporting.
#### Design the dbt Models
dbt project Structure: dbt follows a staging → intermediate → marts structure.
```
stock_analysis/
│── models/
│   │── staging/
│   │   ├── stg_sp500_stockdata.sql
│   │── intermediate/
│   │   ├── int_7d_returns.sql
│   │   ├── int_30d_returns.sql
│   │   ├── int_1y_returns.sql
│   │   ├── int_3y_returns.sql
│   │   ├── int_ytd_returns.sql
│   │   ├── int_yearly_returns.sql
│   │── marts/
│   │   ├── mart_sector_performance_30d.sql
│   │   ├── mart_7d_tops.sql
│   │   ├── mart_30d_tops.sql
│   │   ├── mart_1y_tops.sql
│   │   ├── mart_sp500_performance_trend.sql
│   │   ├── mart_30d_top_price_trend.sql
│── dbt_project.yml
│── packages.yml
│── seeds/
│   ├── sp500_stocks_info
│── snapshots/
│── tests/
│── macros/

```
staging/ → Cleans and standardizes raw data (1:1 with source tables).\
intermediate/ → Derived tables for calculations (e.g., returns, price change).\
marts/ → Final analytics tables for reporting.\
seeds/ → Static reference data (e.g., stocks info).\
tests/ → Data quality tests (e.g., null checks, uniqueness). Ensure data quality with tests.

#### 1. Staging Layer (stg_*) - Cleans and standardizes raw data.
- Load raw data into staging tables, ensuring clean and structured formats.
- Normalization: \
	This layer removes duplicates, renames columns, fill in missing values, and standardizes data types to ensure the data format matches your target schema. Ensures one row per stock per day.

#### 2. Intermediate Layer (int_*) - Transformations: Performs calculations and aggregations.
Create aggregated or derived calculations such as moving averages, stock returns, volatility, etc.
Derived Tables: used to feed into final reporting tables.

#### 3. Marts Layer (dim_* and fact_*) - Analytics: Provides final tables for dashboarding. 
Create Fact & Dimension tables for business insights.
Star Schema → Fact Tables & Dimension Tables.
	Fact tables: Contain numerical values for analytics. (aggregated fact table)
	Dimension tables: Contain descriptive information.

#### dbt Lineage: 
![img](images/dbt_lineage.png)
