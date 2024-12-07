import statistics

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
        data = {}
        data.update(self.get_roi(symbol, quote))
        data.update(self.get_ncav_ratio(symbol, quote))
        data.update(self.get_retained_earnings(symbol))
        data.update(self.get_median_shareholder_returns(symbol))
        data.update(self.get_shareholder_return_frequency(symbol))
        data.update(self.get_per(symbol, quote))
        data.update(self.get_pbr(symbol, quote))
        data.update(self.get_eps(symbol))

        self.firestore.store_analysis(symbol, data)

    def get_shareholder_return_frequency(self, symbol):
        cashflows = self.get_latest_n_years_annual_cashflow(symbol, 100)
        shareholder_returns = []
        cnt = 0
        for cashflow in cashflows:
            shareholder_returns.append(self.get_shareholders_return(cashflow))

        for shareholder_return in shareholder_returns:
            if shareholder_return > 0:
                cnt += 1

        return {
            "shareholderReturnFrequency":  cnt / len(shareholder_returns)
        }

    def get_retained_earnings(self, symbol):
        annual_balancesheet = self.get_latest_annual_balancesheet(symbol)
        quarter_balancesheet = self.get_latest_quarter_balance_sheet(symbol)

        return {
            "annualRetainedEarnings": annual_balancesheet.get("retainedEarnings"),
            "quarterRetainedEarnings": quarter_balancesheet.get("retainedEarnings")
        }

    def get_roi(self, symbol, quote):
        sharedholder_returns = self.get_median_shareholder_returns(symbol)["shareholderReturns"]
        market_cap = quote["marketCap"]
        return {
            "roi": sharedholder_returns / market_cap
        }

    def get_median_shareholder_returns(self, symbol):
        cashflows = self.get_latest_n_years_annual_cashflow(symbol, 5)
        shareholder_returns = []
        for cashflow in cashflows:
            shareholder_returns.append(self.get_shareholders_return(cashflow))

        return {
            "shareholderReturns": statistics.median(shareholder_returns)
        }

    def get_shareholders_return(self, cashflow):
        dividends_paid = cashflow.get("dividendsPaid", 0)
        common_stock_repurchased = cashflow.get("commonStockRepurchased", 0)
        return -1 * (dividends_paid + common_stock_repurchased)

    def get_ncav_ratio(self, symbol, quote):
        annual_ncav_ratio = self.get_annual_ncav(symbol, quote)
        quarter_ncav_ratio = self.get_quarter_ncav(symbol, quote)

        return {
            "annualNcavRatio": annual_ncav_ratio,
            "quarterNcavRatio": quarter_ncav_ratio
        }

    def get_annual_ncav(self, symbol, quote) -> float:
        balancesheet = self.get_latest_annual_balancesheet(symbol)

        if not balancesheet:
            logger.warning(f"Balancesheet not found for {symbol}")
            return -1

        nca = balancesheet.get("totalCurrentAssets") - balancesheet.get("totalLiabilities")
        market_cap = quote["marketCap"]
        ncav_ratio = nca / market_cap

        return ncav_ratio

    def get_quarter_ncav(self, symbol, quote) -> float:
        balancesheet = self.get_latest_quarter_balance_sheet(symbol)

        if not balancesheet:
            logger.warning(f"Balancesheet not found for {symbol}")
            return -1

        nca = balancesheet.get("totalCurrentAssets") - balancesheet.get("totalLiabilities")
        market_cap = quote["marketCap"]
        ncav_ratio = nca / market_cap

        return ncav_ratio

    def get_per(self, symbol, quote):
        net_income = self.get_latest_net_income(symbol)
        market_cap = quote.get("marketCap", 1)  # Default to 1 to avoid division by zero
        per = market_cap / net_income if net_income else None
        return {
            "per": per
        }

    def get_pbr(self, symbol, quote):
        book_value = self.get_latest_book_value(symbol)
        market_cap = quote.get("marketCap", 1)  # Default to 1 to avoid division by zero
        pbr = market_cap / book_value if book_value else None
        return {
            "pbr": pbr
        }

    def get_eps(self, symbol):
        incomestmt = self.get_latest_annual_income_statement(symbol)
        return {
            "eps": incomestmt["eps"]
        }

    def get_latest_net_income(self, symbol):
        annual_income_statement = self.get_latest_annual_income_statement(symbol)
        return annual_income_statement.get("netIncome")

    def get_latest_book_value(self, symbol):
        annual_balancesheet = self.get_latest_annual_balancesheet(symbol)
        return annual_balancesheet.get("totalAssets") - annual_balancesheet.get("totalLiabilities")

    def get_latest_annual_income_statement(self, symbol):
        current_year = datetime.now().year

        for year in [current_year, current_year - 1]:  # 현재년도 → 작년도 순서로 시도
            incomestmt = self.firestore.get_incomestmt(symbol, str(year), "FY")
            if incomestmt:  # 데이터가 있으면 중단
                return incomestmt

    def get_latest_annual_balancesheet(self, symbol):
        current_year = datetime.now().year

        for year in [current_year, current_year - 1]:  # 현재년도 → 작년도 순서로 시도
            balancesheet = self.firestore.get_balancesheet(symbol, str(year), "FY")
            if balancesheet:  # 데이터가 있으면 중단
                return balancesheet

    def get_latest_quarter_balance_sheet(self, symbol):
        current_year = datetime.now().year

        quarters = [
            (current_year, "Q4"), (current_year, "Q3"), (current_year, "Q2"), (current_year, "Q1"),
            (current_year - 1, "Q4"), (current_year - 1, "Q3")
        ]
        for year, quarter in quarters:
            balancesheet = self.firestore.get_balancesheet(symbol, str(year), quarter)
            if balancesheet:
                return balancesheet

    def get_latest_n_years_annual_cashflow(self, symbol, n):
        current_year = datetime.now().year

        cashflows = []
        for year in range(current_year, current_year - n, -1):
            cashflow = self.firestore.get_cashflow(symbol, str(year), "FY")
            if cashflow:
                cashflows.append(cashflow)

        return cashflows
