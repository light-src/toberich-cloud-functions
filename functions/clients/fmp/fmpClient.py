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

    def __get_incomstmt_url(self, symbol, annual=True):
        return self.__get_financial_url("income-statement", symbol, annual)

    def __get_balancesheet_url(self, symbol, annual=True):
        return self.__get_financial_url("balance-sheet-statement", symbol, annual)

    def __get_cashflow_url(self, symbol, annual=True):
        return self.__get_financial_url("cash-flow-statement", symbol, annual)

    def get_company_profile(self, symbol):
        url = f"{self.base_url}/v4/company-core-information?symbol={symbol}&apikey={self.api_key}"
        return get_jsonparsed_data(url)

    def get_income_statement(self, symbol, annual=True):
        url = self.__get_incomstmt_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_balance_sheet(self, symbol, annual=True):
        url = self.__get_balancesheet_url(symbol, annual)
        return get_jsonparsed_data(url)

    def get_cash_flow(self, symbol, annual=True):
        url = self.__get_cashflow_url(symbol, annual)
        return get_jsonparsed_data(url)


if __name__ == '__main__':
    import os

    def save_to_file(data, filename):
        import json
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

    client = FmpClient(api_key=os.getenv('FMP_CLIENT_API_KEY'))
    save_to_file(client.get_company_profile("AAPL"), "data/company_profile.json")
    save_to_file(client.get_income_statement("AAPL", annual=False), "data/income_statement.json")
    save_to_file(client.get_balance_sheet("AAPL", annual=False), "data/balance_sheet.json")
    save_to_file(client.get_cash_flow("AAPL", annual=False), "data/cash_flow.json")
    save_to_file(client.get_income_statement("AAPL"), "data/income_statement_annual.json")
    save_to_file(client.get_balance_sheet("AAPL"), "data/balance_sheet_annual.json")
    save_to_file(client.get_cash_flow("AAPL"), "data/cash_flow_annual.json")
