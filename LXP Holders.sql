WITH lxp AS (
  SELECT
    block_date,
    block_time,
    block_number,
    CASE
      WHEN contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a')
      THEN 'LXP'
      ELSE TRY_CAST(contract_address AS VARCHAR) END AS contract_name,
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
    block_hash
  FROM linea.logs
  WHERE
    tx_from = FROM_HEX('97643dd2dfe4dd0b64d43504bac4adb2923fdf7a') -- contract deployer
    AND tx_to = FROM_HEX('3886a948ea7b4053312c3ae31a13776144aa6239') -- drop executor
    AND contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a') -- contract addres LXP
  ORDER BY 2 DESC  
)
SELECT
    sum(lxp_quantity) as total_lxp,
    count(distinct to_user_wallet) as total_wallets
FROM lxp
