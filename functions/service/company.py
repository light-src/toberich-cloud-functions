import os
import logging
from clients.fmp.fmpClient import FmpClient
from google.cloud import firestore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompanyService:
    def __init__(self):
        self.fmpClient = FmpClient(api_key=os.getenv("FMP_CLIENT_API_KEY"))

        # Initialize Firestore client
        self.db = firestore.Client()
        logger.info("Initialized Firestore client")

    def get_company(self, symbol):
        logger.info(f"Fetching company profile for symbol: {symbol}")
        return self.fmpClient.get_company_profile(symbol)

    def get_incomestmts(self, symbol, annual=True):
        logger.info(f"Fetching income statements for symbol: {symbol}, annual: {annual}")
        return self.fmpClient.get_income_statement(symbol, annual)

    def get_balancesheets(self, symbol, annual=True):
        logger.info(f"Fetching balance sheets for symbol: {symbol}, annual: {annual}")
        return self.fmpClient.get_balance_sheet(symbol, annual)

    def get_cashflows(self, symbol, annual=True):
        logger.info(f"Fetching cash flows for symbol: {symbol}, annual: {annual}")
        return self.fmpClient.get_cash_flow(symbol, annual)

    def save_to_firestore(self, company_id, report_type, year_quarter, data):
        logger.info(f"Saving {report_type} data for {company_id} for period {year_quarter}")
        doc_ref = self.db.collection('companies').document(company_id)\
            .collection('financials').document(report_type)\
            .collection('periods').document(year_quarter)
        doc_ref.set(data)

    def fetch_and_store_company(self, symbol):
        company_profile = self.get_company(symbol)
        doc_ref = self.db.collection("companies").document(symbol)
        doc_ref.set(company_profile[0])
        logger.info(f"Company profile for {symbol} stored in Firestore")

    def fetch_and_store_financial(self, symbol, data_type, annual=True):
        logger.info(f"Fetching and storing {data_type} for {symbol}, annual: {annual}")
        if data_type == "income_statements":
            data = self.get_incomestmts(symbol, annual)
        elif data_type == "balance_sheets":
            data = self.get_balancesheets(symbol, annual)
        elif data_type == "cash_flows":
            data = self.get_cashflows(symbol, annual)
        else:
            raise ValueError("Invalid data type")

        for item in data:
            year = item.get("calendarYear")
            period = item.get("period")

            if not year or not period:
                logger.warning(f"Skipping item with missing year or period: {item}")
                continue

            doc_ref = self.db.collection("companies") \
                .document(symbol) \
                .collection("financials") \
                .document(data_type) \
                .collection("periods") \
                .document(f"{year}-{period}")

            doc_ref.set(item)
            logger.info(f"Stored {data_type} for {symbol} for period {year}-{period}")

    def fetch_and_store_financials(self, symbol, annual=True):
        self.fetch_and_store_financial(symbol, "income_statements", annual)
        self.fetch_and_store_financial(symbol, "balance_sheets", annual)
        self.fetch_and_store_financial(symbol, "cash_flows", annual)

    def fetch_and_store_all(self, symbol):
        logger.info(f"Fetching and storing all data for {symbol}")
        self.fetch_and_store_company(symbol)
        self.fetch_and_store_financials(symbol)
        self.fetch_and_store_financials(symbol, annual=False)
