from firebase_functions import https_fn
from firebase_admin import initialize_app
from companies import companies
from service.company import CompanyService
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = initialize_app()


@https_fn.on_request()
def fetch_company(req: https_fn.Request) -> https_fn.Response:
    """Take the text parameter passed to this HTTP endpoint and insert it into
    a new document in the messages collection."""
    symbol = req.args.get("symbol")
    if symbol not in companies:
        logger.error(f"Symbol {symbol} is not in the defined list")
        return https_fn.Response("symbol is not in defined list", status=400)

    logger.info(f"Fetching and storing data for symbol: {symbol}")
    company_service = CompanyService()
    company_service.fetch_and_store_all(symbol)

    logger.info(f"Symbol {symbol} fetched and stored in Firestore")
    return https_fn.Response(f"Symbol {symbol} fetched and stored in Firestore", status=200)
