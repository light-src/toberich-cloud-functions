import logging
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
        doc_ref.set(data)
        logger.info(f"Stored document in {collection_path}/{document_id}")

    def get_company_profile(self, symbol):
        return self._get_document("companies", symbol)

    def get_quote(self, symbol):
        date_str = datetime.today().date().strftime("%Y-%m-%d")
        return self._get_document(f"companies/{symbol}/quotes", date_str)

    def get_financial(self, symbol, data_type, year, period):
        return self._get_document(f"companies/{symbol}/financials/{data_type}/periods", f"{year}-{period}")

    def store_company_profile(self, symbol, data):
        self._store_document("companies", symbol, data)

    def store_quote(self, symbol, data):
        date_str = datetime.today().date().strftime("%Y-%m-%d")
        self._store_document(f"companies/{symbol}/quotes", date_str, data)

    def _store_financial(self, symbol, data_type, data_list):
        for item in data_list:
            year = item.get("calendarYear")
            period = item.get("period")

            if not year or not period:
                logger.warning(f"Skipping item with missing year or period: {item}")
                continue

            self._store_document(f"companies/{symbol}/financials/{data_type}/periods", f"{year}-{period}", item)

    def store_incomestmt(self, symbol, data_list):
        self._store_financial(symbol, "income_statements", data_list)

    def store_balancesheet(self, symbol, data_list):
        self._store_financial(symbol, "balance_sheets", data_list)

    def store_cashflow(self, symbol, data_list):
        self._store_financial(symbol, "cash_flows", data_list)