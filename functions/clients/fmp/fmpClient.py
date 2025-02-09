import requests
import certifi
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_jsonparsed_data(url):
    response = requests.get(url, verify=certifi.where())
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()


class FmpClient:
    def __init__(self, base_url="https://financialmodelingprep.com/api", api_key=""):
        self.api_key = api_key
        self.base_url = base_url

    def __get_financial_url(self, statement_type, symbol, annual=True):
        period = "annual" if annual else "quarter"
        return f"{self.base_url}/v3/{statement_type}/{symbol}?period={period}&apikey={self.api_key}"

    def __get_incomestmt_url(self, symbol, annual=True):
        return self.__get_financial_url("income-statement", symbol, annual)

    def __get_incomestmt_as_reported_url(self, symbol, annual=True):
        return self.__get_financial_url("income-statement-as-reported", symbol, annual)

    def __get_balancesheet_url(self, symbol, annual=True):
        return self.__get_financial_url("balance-sheet-statement", symbol, annual)

    def __get_balancesheet_as_reported_url(self, symbol, annual=True):
        return self.__get_financial_url("balance-sheet-statement-as-reported", symbol, annual)

    def __get_cashflow_url(self, symbol, annual=True):
        return self.__get_financial_url("cash-flow-statement", symbol, annual)

    def __get_cashflow_as_reported_url(self, symbol, annual=True):
        return self.__get_financial_url("cash-flow-statement-as-reported", symbol, annual)

    def __get_quote_url(self, symbol):
        return f"{self.base_url}/v3/quote/{symbol}?apikey={self.api_key}"

    def get_company_core_info(self, symbol):
        url = f"{self.base_url}/v4/company-core-information?symbol={symbol}&apikey={self.api_key}"
        return get_jsonparsed_data(url)

    def get_company_outlook(self, symbol):
        url = f"{self.base_url}/v4/company-outlook?symbol={symbol}&apikey={self.api_key}"
        return get_jsonparsed_data(url)

    def get_income_statement(self, symbol, annual=True) -> list:
        url = self.__get_incomestmt_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_income_statement_as_reported(self, symbol, annual=True) -> list:
        url = self.__get_incomestmt_as_reported_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_balance_sheet(self, symbol, annual=True) -> list:
        url = self.__get_balancesheet_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_balance_sheet_as_reported(self, symbol, annual=True) -> list:
        url = self.__get_balancesheet_as_reported_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_cash_flow(self, symbol, annual=True) -> list:
        url = self.__get_cashflow_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_cash_flow_as_reported(self, symbol, annual=True) -> list:
        url = self.__get_cashflow_as_reported_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_quote(self, symbol):
        url = self.__get_quote_url(symbol)
        return get_jsonparsed_data(url)
