{{
    config(
        materialized='table'       
    )
}}


WITH latest_date AS (
    SELECT trade_date 
    FROM {{ ref('int_7d_returns') }}
    ORDER BY trade_date DESC 
    LIMIT 1
),
last_7d_top_gianer AS (
    SELECT 
        ticker,
        seven_days_return        
    FROM {{ ref('int_7d_returns') }}
    WHERE trade_date = (SELECT trade_date FROM latest_date)  
    ORDER BY seven_days_return DESC LIMIT 1 
)

SELECT ticker, trade_date, opening_price, closing_price, lowest_price, highest_price, volume
FROM {{ ref('stg_sp500_stockdata') }}
WHERE ticker = (SELECT ticker FROM last_7d_top_gianer) 
