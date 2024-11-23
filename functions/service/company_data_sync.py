import logging
from clients.fmp.fmpClient import FmpClient
from service.firestore import FirestoreService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompanyDataSyncService:
    def __init__(self):
        self.fmpClient = FmpClient(api_key="Mm7PVqAUBosTmVOLi0lsHbQUUVh3f7vd")
        self.firestore = FirestoreService()

    def sync_company_profile(self, symbol):
        company_profile = self.fmpClient.get_company_profile(symbol)
        self.firestore.store_company_profile(symbol, company_profile[0])
        logger.info(f"Company profile for {symbol} synced")

    def sync_quote(self, symbol):
        quote = self.fmpClient.get_quote(symbol)
        self.firestore.store_quote(symbol, quote[0])
        logger.info(f"Quote for {symbol} synced")

    def sync_incomstmt(self, symbol, annual=True):
        incomestmts = self.fmpClient.get_income_statement(symbol, annual)
        self.firestore.store_incomestmt(symbol, incomestmts)
        logger.info(f"Income statements for {symbol} synced")

    def sync_balancesheet(self, symbol, annual=True):
        balancesheets = self.fmpClient.get_balance_sheet(symbol, annual)
        self.firestore.store_balancesheet(symbol, balancesheets)
        logger.info(f"Balance sheets for {symbol} synced")

    def sync_cashflow(self, symbol, annual=True):
        cashflows = self.fmpClient.get_cash_flow(symbol, annual)
        self.firestore.store_cashflow(symbol, cashflows)
        logger.info(f"Cash flows for {symbol} synced")

    def sync_all(self, symbol):
        self.sync_company_profile(symbol)
        self.sync_quote(symbol)
        self.sync_incomstmt(symbol)
        self.sync_balancesheet(symbol)
        self.sync_cashflow(symbol)
        self.sync_incomstmt(symbol, annual=False)
        self.sync_balancesheet(symbol, annual=False)
        self.sync_cashflow(symbol, annual=False)
        logger.info(f"Data for {symbol} synced")