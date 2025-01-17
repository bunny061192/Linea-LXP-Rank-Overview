WITH
  raw_tx_tbl AS (
    SELECT
      DATE(DATE_TRUNC('day', block_time)) AS tx_date,
      hash,
      "from" AS from_address, -- sender
      "to" as to_address, -- reciever
      success,
      p.price * (gas_price * gas_used) / 1e18 AS tx_fee_usd, 
      (gas_price * gas_used) / 1e18 AS tx_fee_eth -- ADD
    FROM linea.transactions
    LEFT JOIN prices.usd p ON p.minute = DATE_TRUNC('minute', block_time) 
        AND p.blockchain = 'ethereum'
        AND p.symbol = 'WETH'
  -- 0xd3d7DBe73BbdD5A5C7a49Ca322763c4d400fC240 -- OKEX wallet
  
  )
  
 , transactions_tbl AS (
        SELECT
            tx_date,
            COUNT(hash) AS total_tx,
            COUNT(CASE WHEN success = true THEN 1 END) AS successful_tx,
            AVG(tx_fee_usd) AS tx_fee_usd_average,
            APPROX_PERCENTILE(tx_fee_usd, 0.5) AS tx_fee_usd_median,
        --   APPROX_PERCENTILE(tx_fee_usd, 0.1) AS tx_fee_usd_percentile_10, -- minimal transaction cost USD
        --   APPROX_PERCENTILE(tx_fee_usd, 0.9) AS tx_fee_usd_percentile_90 -- 90 percentile
        --  AVG(tx_fee_eth) AS tx_fee_eth_average,
            APPROX_PERCENTILE(tx_fee_eth, 0.5) AS tx_fee_eth_median
        FROM raw_tx_tbl
        GROUP BY 1    
  )
  
  , new_addresses_tbl AS ( -- NEW ADDRESSES BY DAY
        SELECT
            tx_date,
            COUNT(DISTINCT from_address) AS new_addresses,
            COUNT(DISTINCT from_address) FILTER (WHERE to_address = 0xd3d7DBe73BbdD5A5C7a49Ca322763c4d400fC240) AS okx_addresses,
            COUNT(DISTINCT from_address) FILTER (WHERE to_address = 0xf89d7b9c864f589bbF53a82105107622B35EaA40) AS bybit_addresses, 
            COUNT(DISTINCT from_address) FILTER (WHERE to_address = 0x0D0707963952f2fBA59dD06f2b425ace40b492Fe) AS gate_addresses 
        FROM (
                SELECT -- find the first transaction date
                    tx_date,
                    from_address,
                    to_address, -- ADDED
                    MIN(tx_date) OVER (PARTITION BY from_address) AS first_tx_date
                FROM raw_tx_tbl
             )
        WHERE tx_date = first_tx_date
        GROUP BY 1
  ),
  
  cumulative_addresses_tbl AS (
    SELECT
      tx_date,
      SUM(new_addresses) OVER (ORDER BY tx_date ASC) AS cumulative_addresses
    FROM new_addresses_tbl
  )

SELECT
    n.tx_date,
    n.new_addresses as total_new_wallets,
    n.okx_addresses as okx_wallets,
    n.bybit_addresses as bybit_wallets,
    n.gate_addresses as gate_wallets,
    n.new_addresses - n.okx_addresses - n.bybit_addresses - n.gate_addresses as new_wallets,
    c.cumulative_addresses as total_wallets, -- cumulative
    -- t.total_tx, 
    t.successful_tx,
    round(t.tx_fee_usd_median, 3) as tx_fee_usd_median,
    round(t.tx_fee_usd_average, 3) as tx_fee_usd_average,
    t.tx_fee_eth_median
    
FROM new_addresses_tbl n
LEFT JOIN cumulative_addresses_tbl c ON n.tx_date=c.tx_date
LEFT JOIN transactions_tbl t ON n.tx_date=t.tx_date
order by 1 desc
