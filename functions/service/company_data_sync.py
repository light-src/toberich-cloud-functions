import logging
import os

from clients.fmp.fmpClient import FmpClient
from service.firestore import FirestoreService
from service.task_state import TaskStateService
import concurrent.futures

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompanyDataSyncService:
    def __init__(self):
        self.fmpClient = FmpClient(api_key=os.getenv("FMP_CLIENT_API_KEY"))
        self.firestore = FirestoreService()
        self.taskStateService = TaskStateService()

    def sync_company_profile(self, symbol):
        company_profile = self.fmpClient.get_company_profile(symbol)
        if len(company_profile) == 0:
            logger.error(f"Company profile for {symbol} not found")
            return
        self.firestore.store_company_profile(symbol, company_profile[0])
        logger.info(f"Company profile for {symbol} synced")

    def sync_quote(self, symbol):
        quote = self.fmpClient.get_quote(symbol)
        if len(quote) == 0:
            logger.error(f"Quote for {symbol} not found")
            return
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
        self.sync_incomstmt(symbol)
        self.sync_balancesheet(symbol)
        self.sync_cashflow(symbol)
        self.sync_incomstmt(symbol, annual=False)
        self.sync_balancesheet(symbol, annual=False)
        self.sync_cashflow(symbol, annual=False)
        self.sync_quote(symbol)
        logger.info(f"Data for {symbol} synced")

    def sync_all_companies_task(self, get_companies_func, sync_func, set_latest_func):
        companies = get_companies_func()

        def process_symbol(symbol):
            """sync_all 작업"""
            sync_func(symbol)
            return symbol  # 처리 완료된 symbol 반환

        # 4개씩 처리
        batch_size = 4
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]  # 4개씩 추출

            with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
                # batch의 symbol을 병렬로 처리
                futures = [executor.submit(process_symbol, symbol) for symbol in batch]

                # 모든 작업 완료 대기 및 결과 수집
                results = [future.result() for future in concurrent.futures.as_completed(futures)]

            # 가장 뒤에 있는 symbol (정렬 기준에 따라 results[-1])
            last_symbol = batch[-1]  # batch의 마지막 symbol 사용
            set_latest_func(last_symbol)

            logger.info(f"Batch completed: {batch}, updated with last symbol: {last_symbol}")

    def sync_all_companies_info(self):
        self.sync_all_companies_task(
            self.taskStateService.get_update_company_info_companies,
            self.sync_all,
            self.taskStateService.set_latest_updated_company_info
        )
        logger.info("All companies info data synced")

    def sync_all_companies_quotes(self):
        self.sync_all_companies_task(
            self.taskStateService.get_update_company_quotes_companies,
            self.sync_quote,
            self.taskStateService.set_latest_updated_company_quote
        )
        logger.info("All companies quotes data synced")
