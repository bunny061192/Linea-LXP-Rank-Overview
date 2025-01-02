WITH lxp AS ( -- LXP transactions table from Linea blockchain
  SELECT
    block_date,
    block_time,
    block_number,
    CASE
      WHEN contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a')
      THEN 'LXP'
      ELSE TRY_CAST(contract_address AS VARCHAR)
    END AS contract_name,
    contract_address,
    BYTEARRAY_LTRIM(topic1) AS from_wallet,
    BYTEARRAY_LTRIM(topic2) AS to_user_wallet,
    BYTEARRAY_TO_UINT256(data) / 1000000000000000000 AS lxp_quantity,
    case
        when BYTEARRAY_LTRIM(topic1) = FROM_HEX('0x') then 'Airdrop'
        else null end as event_name
  FROM linea.logs
  WHERE
        tx_from = FROM_HEX('97643dd2dfe4dd0b64d43504bac4adb2923fdf7a')
    AND tx_to = FROM_HEX('3886a948ea7b4053312c3ae31a13776144aa6239')
    AND contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a') -- contract addres LXP
  ORDER BY 2 DESC  
)
, calendar_months as ( -- CREATE CALENDAR MONTHS SECUENCE
    WITH date_bounds AS (
        SELECT
            MIN(block_date) AS min_date,
            GREATEST(MAX(block_date), DATE_TRUNC('month', CURRENT_DATE)) AS max_date
            -- MAX(block_date) AS max_date
        FROM linea.logs
        WHERE contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a')
    )
    SELECT
            DATE_TRUNC('month', sequence_element) AS calendar_month
    FROM date_bounds
    CROSS JOIN UNNEST(SEQUENCE(min_date, max_date + INTERVAL '1' MONTH, INTERVAL '1' MONTH)) AS t(sequence_element)
)
, aggregations as ( -- BASIC AGGREGATIONS
    SELECT
        DATE_TRUNC('month', block_date) as date_time,
        sum(lxp_quantity) as lxp_quantity,
        count(distinct to_user_wallet) as wallets
    FROM lxp
    GROUP BY 1
)
, all_wallets AS ( -- ALL WALLETS LIST BY MONTH
    SELECT
        DATE_TRUNC('month', block_date) as date_time,
        to_user_wallet as wallet
    FROM lxp
)
, filtered_wallets AS ( -- NEW WALLETS COUNT BY MONTH
    SELECT
        a1.date_time,
        count(distinct a1.wallet) as wallets_by_month
    FROM all_wallets a1
    WHERE NOT EXISTS (  SELECT 1
                        FROM all_wallets a2
                        WHERE a2.wallet = a1.wallet
                          AND a2.date_time < a1.date_time)
    group by 1
)
SELECT
    c.calendar_month,
    -- a.date_time,
    COALESCE(a.wallets, 0) as wallets,
    COALESCE(f.wallets_by_month, 0) as new_wallets,
    SUM(COALESCE(f.wallets_by_month, 0)) OVER (ORDER BY c.calendar_month) AS wallets_cumulative,
    COALESCE(a.wallets, 0) - COALESCE(f.wallets_by_month, 0) as old_wallets,
    COALESCE(a.lxp_quantity, 0) as lxp_quantity,
    SUM(COALESCE(a.lxp_quantity, 0)) OVER (ORDER BY c.calendar_month) AS lxp_cumulative
FROM calendar_months c
LEFT JOIN aggregations a ON c.calendar_month=a.date_time
LEFT JOIN filtered_wallets f ON c.calendar_month=f.date_time
ORDER BY 1 desc
