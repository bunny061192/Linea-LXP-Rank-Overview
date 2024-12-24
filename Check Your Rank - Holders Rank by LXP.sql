-- select *--sum(NFTs), count(wallet) filter(where NFTs > 0) 
-- from (

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
    block_hash
  FROM linea.logs
  WHERE
    tx_from = FROM_HEX('97643dd2dfe4dd0b64d43504bac4adb2923fdf7a')
    AND tx_to = FROM_HEX('3886a948ea7b4053312c3ae31a13776144aa6239')
    AND contract_address = FROM_HEX('d83af4fbd77f3ab65c3b1dc4b38d7e67aecf599a') -- contract addres LXP
  ORDER BY 2 DESC  
),
lxp2 as (
SELECT
  to_user_wallet as wallet,
  sum(lxp_quantity) as total_lxp
FROM lxp
group by 1
order by 2 desc
),
nft as (
    WITH transfers AS ( -- Table normalization for NFT Transfers
            SELECT
                COALESCE(to, CAST('' AS varbinary)) AS wallet,
                id,
                value AS amount
            FROM erc1155_linea.evt_transfersingle
            WHERE contract_address = 0x0872ec4426103482a50f26ffc32acefcec61b3c9

            UNION ALL

            SELECT
                COALESCE("from", CAST('' AS varbinary)) AS wallet,
                id,
                -value AS amount
            FROM erc1155_linea.evt_transfersingle
            WHERE contract_address = 0x0872ec4426103482a50f26ffc32acefcec61b3c9
        ),

    batch_transfers AS (
            SELECT
                COALESCE(to, CAST('' AS varbinary)) AS wallet,
                t.id,
                t.amount
            FROM erc1155_linea.evt_transferbatch
            CROSS JOIN UNNEST("ids", "values") AS t(id, amount)
            WHERE contract_address = 0x0872ec4426103482a50f26ffc32acefcec61b3c9

            UNION ALL

            SELECT
                COALESCE("from", CAST('' AS varbinary)) AS wallet,
                t.id,
                -t.amount AS amount
            FROM erc1155_linea.evt_transferbatch
            CROSS JOIN UNNEST("ids", "values") AS t(id, amount)
            WHERE contract_address = 0x0872ec4426103482a50f26ffc32acefcec61b3c9
        )

    SELECT
        wallet,
        sum(amount) as NFTs
    FROM (SELECT * FROM transfers
        UNION ALL
          SELECT * FROM batch_transfers) t
    GROUP BY wallet
    HAVING sum(amount) > 0
)

SELECT
    RANK() over(order by lxp2.total_lxp desc, COALESCE(nft.NFTs, 0) desc) as "rank",
    lxp2.wallet,
    lxp2.total_lxp as LXP,
    COALESCE(nft.NFTs, 0) as NFTs
FROM lxp2
LEFT JOIN nft on lxp2.wallet=nft.wallet
    

-- ) a 
-- where LXP > 750 and LXP < 1500
