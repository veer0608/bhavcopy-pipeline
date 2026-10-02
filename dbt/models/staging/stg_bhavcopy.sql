-- Rename the UDiFF abbreviations and type every column. Nothing is filtered here.

select
    _source                                as exchange,
    _trade_date                            as trade_date,
    "FinInstrmId"                          as instrument_id,
    "TckrSymb"                             as symbol,
    "SctySrs"                              as series,
    "ISIN"                                 as isin,
    "FinInstrmNm"                          as security_name,
    "FinInstrmTp"                          as instrument_type,
    cast("OpnPric" as decimal(18, 4))      as open_price,
    cast("HghPric" as decimal(18, 4))      as high_price,
    cast("LwPric" as decimal(18, 4))       as low_price,
    cast("ClsPric" as decimal(18, 4))      as close_price,
    cast("LastPric" as decimal(18, 4))     as last_price,
    cast("PrvsClsgPric" as decimal(18, 4)) as prev_close_price,
    cast("TtlTradgVol" as bigint)          as volume,
    cast("TtlTrfVal" as decimal(24, 2))    as turnover,
    cast("TtlNbOfTxsExctd" as bigint)      as trades,
    cast("TradDt" as date)                 as file_trade_date,
    _sha256                                as file_sha256,
    _loaded_at                             as loaded_at
from {{ source('raw', 'bhavcopy') }}
