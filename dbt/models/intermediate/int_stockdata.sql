{{
    config(
        materialized='table'        
    )
}}

with stockdata as 
(
  select *
  from {{ ref('stg_sp500_stockdata') }}
)

select *    
from stockdata



