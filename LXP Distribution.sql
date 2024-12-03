WITH lxp AS (
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
    -- topic0,
    BYTEARRAY_LTRIM(topic1) AS from_wallet,
    BYTEARRAY_LTRIM(topic2) AS to_user_wallet,
    -- topic3,
    BYTEARRAY_TO_UINT256(data) / 1000000000000000000 AS lxp_quantity,
    case
        when BYTEARRAY_LTRIM(topic1) = FROM_HEX('0x') then 'Airdrop'
        else null end as event_name,
    index,
    tx_hash,
    -- tx_index,
    -- TRY_CAST(tx_from AS VARCHAR) AS tx_from,
    -- TRY_CAST(tx_to AS VARCHAR) AS tx_to,
    block_hash,
    sum(BYTEARRAY_TO_UINT256(data) / 1000000000000000000) over (partition by BYTEARRAY_LTRIM(topic2)) as lxp_holding
  FROM linea.logs
  WHERE
        tx_from = FROM_HEX('97643dd2dfe4dd0b64d43504bac4adb2923fdf7a')
    AND tx_to = FROM_HEX('3886a948ea7b4053312c3ae31a13776144aa6239')
    AND contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a') -- contract addres LXP
  ORDER BY 2 DESC  
)
, status as (
SELECT
    distinct to_user_wallet,
    lxp_holding,
    case
        when lxp_holding > 0 and lxp_holding <= 1500 then '1 - 1500 LXP'
        -- when lxp_holding <= 1500 then '1001 - 1500 LXP'
        when lxp_holding <= 2250 then '1501 - 2250 LXP'
        when lxp_holding <= 2800 then '2251 - 2800 LXP'
        when lxp_holding <= 3500 then '2801 - 3500 LXP'
        when lxp_holding <= 5000 then '3501 - 5000 LXP'
        when lxp_holding > 5000 then '5000+ LXP'
        else null end as LXP_status
FROM lxp
order by 2 desc
)
SELECT
    LXP_status,
    count(distinct to_user_wallet) as wallets
from status
group by 1
order by 1 desc
