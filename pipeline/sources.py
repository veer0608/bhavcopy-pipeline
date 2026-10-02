"""Where each exchange publishes its end-of-day bhavcopy.

Both exchanges moved to the same UDiFF column layout in 2024, so one landing
schema and one staging model cover both. Only the transport differs.
"""
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    referer: str
    zipped: bool

    def url_for(self, day: date) -> str:
        return self.url.format(ymd=day.strftime("%Y%m%d"))


SOURCES = {
    "NSE": Source(
        name="NSE",
        url="https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip",
        referer="https://www.nseindia.com/",
        zipped=True,
    ),
    "BSE": Source(
        name="BSE",
        url="https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{ymd}_F_0000.CSV",
        referer="https://www.bseindia.com/",
        zipped=False,
    ),
}

# The columns every file must carry, in this order. A file that does not start
# with these is rejected at the door rather than loaded and discovered later.
EXPECTED_COLUMNS = [
    "TradDt", "BizDt", "Sgmt", "Src", "FinInstrmTp", "FinInstrmId", "ISIN",
    "TckrSymb", "SctySrs", "XpryDt", "FininstrmActlXpryDt", "StrkPric", "OptnTp",
    "FinInstrmNm", "OpnPric", "HghPric", "LwPric", "ClsPric", "LastPric",
    "PrvsClsgPric", "UndrlygPric", "SttlmPric", "OpnIntrst", "ChngInOpnIntrst",
    "TtlTradgVol", "TtlTrfVal", "TtlNbOfTxsExctd", "SsnId", "NewBrdLotQty",
    "Rmks", "Rsvd1", "Rsvd2", "Rsvd3", "Rsvd4",
]
