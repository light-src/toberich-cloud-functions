import logging
import concurrent.futures
from google.cloud import firestore
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FirestoreService:
    def __init__(self):
        # Initialize Firestore client
        self.db = firestore.Client()
        logger.info("Initialized Firestore client")

    def _get_document(self, collection_path, document_id):
        logger.info(f"Fetching document from {collection_path}/{document_id}")
        return self.db.collection(collection_path).document(document_id).get().to_dict()

    def _store_document(self, collection_path, document_id, data):
        doc_ref = self.db.collection(collection_path).document(document_id)
        doc_ref.set(
            document_data=data,
            merge=True
        )
        logger.info(f"Stored document in {collection_path}/{document_id}")

    def get_task_state(self, task_name):
        return self._get_document("task_state", task_name)

    def set_task_state(self, task_name, data):
        self._store_document("task_state", task_name, data)

    def get_company_profile(self, symbol):
        return self._get_document("companies", symbol)

    def get_quote(self, symbol):
        date_str = datetime.today().date().strftime("%Y-%m-%d")
        return self._get_document(f"companies/{symbol}/quotes", date_str)

    def _get_financial(self, symbol, data_type, year, period):
        return self._get_document(f"companies/{symbol}/financials/{data_type}/periods", f"{year}-{period}")

    def get_incomestmt(self, symbol, year, period):
        return self._get_financial(symbol, "incomeStatements", year, period)

    def get_balancesheet(self, symbol, year, period):
        return self._get_financial(symbol, "balanceSheets", year, period)

    def get_cashflow(self, symbol, year, period):
        return self._get_financial(symbol, "cashFlows", year, period)

    def store_company_profile(self, symbol, data):
        self._store_document("companies", symbol, data)

    def store_quote(self, symbol, data):
        date_str = datetime.today().date().strftime("%Y-%m-%d")
        self._store_document(f"companies/{symbol}/quotes", date_str, data)

    def store_analysis(self, symbol, data):
        date_str = datetime.today().date().strftime("%Y-%m-%d")
        self._store_document(f"companies/{symbol}/analysis", date_str, data)

    def _store_financial(self, symbol, data_type, data_list):
        def process_item(item):
            """각 item을 처리하는 작업"""
            year = item.get("calendarYear")
            period = item.get("period")

            if not year:
                date = item.get("date")
                year = datetime.strptime(date, "%Y-%m-%d").year

            if not year or not period:
                logger.warning(f"Skipping item with missing year or period: {item}")
                return

            self._store_document(
                f"companies/{symbol}/financials/{data_type}/periods",
                f"{year}-{period}",
                item
            )

        # ThreadPoolExecutor를 사용하여 병렬 처리
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 각 item에 대해 process_item 함수를 병렬로 실행
            futures = [executor.submit(process_item, item) for item in data_list]

            # 완료된 작업 확인 (선택적)
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # 작업 결과를 확인 (필요시)
                except Exception as e:
                    logger.error(f"Error processing item: {e}")

    def _store_financial_as_reported(self, symbol, data_type, data_list):
        self._store_financial(symbol, f"{data_type}AsReported", data_list)

    def _store_financial_refined(self, symbol, data_type, data_list):
        self._store_financial(symbol, f"{data_type}", data_list)

    def store_incomestmt(self, symbol, data_list):
        self._store_financial_refined(symbol, "incomeStatements", data_list)

    def store_incomestmt_as_reported(self, symbol, data_list):
        self._store_financial_as_reported(symbol, "incomeStatements", data_list)

    def store_balancesheet(self, symbol, data_list):
        self._store_financial_refined(symbol, "balanceSheets", data_list)

    def store_balancesheet_as_reported(self, symbol, data_list):
        self._store_financial_as_reported(symbol, "balanceSheets", data_list)

    def store_cashflow(self, symbol, data_list):
        self._store_financial_refined(symbol, "cashFlows", data_list)

    def store_cashflow_as_reported(self, symbol, data_list):
        self._store_financial_as_reported(symbol, "cashFlows", data_list)
