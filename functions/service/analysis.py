from service.firestore import FirestoreService
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalysisService:
    def __init__(self):
        self.firestore = FirestoreService()

    def update_analysis(self, symbol, quote):
        annual_ncav_ratio = self.get_annual_ncav(symbol, quote)
        quarter_ncav_ratio = self.get_quarter_ncav(symbol, quote)

        self.firestore.store_analysis(symbol, {
            "annualNcavRatio": annual_ncav_ratio,
            "quarterNcavRatio": quarter_ncav_ratio
        })

    def get_annual_ncav(self, symbol, quote) -> float:
        current_year = datetime.now().year

        for year in [current_year, current_year - 1]:  # 현재년도 → 작년도 순서로 시도
            balancesheet = self.firestore.get_balancesheet(symbol, str(year), "FY")
            if balancesheet:  # 데이터가 있으면 중단
                break

        if not balancesheet:
            logger.warning(f"Balancesheet not found for {symbol}")
            return -1

        nca = balancesheet.get("totalCurrentAssets") - balancesheet.get("totalLiabilities")
        market_cap = quote["marketCap"]
        ncav_ratio = nca / market_cap

        return ncav_ratio

    def get_quarter_ncav(self, symbol, quote) -> float:
        current_year = datetime.now().year
        balancesheet = None

        quarters = [
            (current_year, "Q4"), (current_year, "Q3"), (current_year, "Q2"), (current_year, "Q1"),
            (current_year - 1, "Q4"), (current_year - 1, "Q3")
        ]
        for year, quarter in quarters:
            balancesheet = self.firestore.get_balancesheet(symbol, str(year), quarter)
            if balancesheet:
                break

        if not balancesheet:
            logger.warning(f"Balancesheet not found for {symbol}")
            return -1

        nca = balancesheet.get("totalCurrentAssets") - balancesheet.get("totalLiabilities")
        market_cap = quote["marketCap"]
        ncav_ratio = nca / market_cap

        return ncav_ratio
