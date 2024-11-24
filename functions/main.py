from firebase_functions import https_fn, scheduler_fn, firestore_fn
from firebase_admin import initialize_app
from companies import companies
from service.company_data_sync import CompanyDataSyncService
from service.analysis import AnalysisService
import logging
from firebase_functions.firestore_fn import Event, Change, DocumentSnapshot

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = initialize_app()


@https_fn.on_request()
def sync_company(req: https_fn.Request) -> https_fn.Response:
    symbol = req.args.get("symbol")
    if symbol not in companies:
        logger.error(f"Symbol {symbol} is not in the defined list")
        return https_fn.Response("symbol is not in defined list", status=400)
    company_service = CompanyDataSyncService()
    company_service.sync_all(symbol)
    return https_fn.Response(f"Company {symbol} information updated")


@scheduler_fn.on_schedule(schedule="every 9 minutes from 00:01 to 23:59 on SUN")
def sync_company_scheduled(event: scheduler_fn.ScheduledEvent) -> None:
    sync_companies_exec()


@https_fn.on_request()
def sync_company_quote(req: https_fn.Request) -> https_fn.Response:
    symbol = req.args.get("symbol")
    if symbol not in companies:
        logger.error(f"Symbol {symbol} is not in the defined list")
        return https_fn.Response("symbol is not in defined list", status=400)
    company_service = CompanyDataSyncService()
    company_service.sync_quote(symbol)
    return https_fn.Response(f"Company {symbol} quotes updated")


@scheduler_fn.on_schedule(schedule="every 5 minutes from 09:30 to 16:00 on Mon, Tue, Wed, Thu, Fri")
def sync_companies_quotes_scheduled(event: scheduler_fn.ScheduledEvent) -> None:
    sync_companies_quote_exec()


@firestore_fn.on_document_written(document="companies/{symbol}/quotes/{date}")
def sync_analysis(event: Event[Change[DocumentSnapshot | None]]) -> None:
    document = (event.data.after.to_dict()
                if event.data.after is not None else None)

    if document is None or document["marketCap"] is None:
        logger.error("Document is empty")
        return

    analysis_service = AnalysisService()
    analysis_service.update_analysis(event.params["symbol"], document)


def sync_companies_exec() -> str:
    logger.info("Updating company information")
    company_service = CompanyDataSyncService()
    company_service.sync_all_companies_info()
    logger.info("Company information updated")
    return "Company information updated"


def sync_companies_quote_exec() -> str:
    logger.info("Updating company quotes")
    company_service = CompanyDataSyncService()
    company_service.sync_all_companies_quotes()
    logger.info("Company quotes updated")
    return "Company quotes updated"
