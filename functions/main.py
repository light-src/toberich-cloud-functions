from firebase_functions import https_fn, scheduler_fn
from firebase_admin import initialize_app
from companies import companies
from service.company_data_sync import CompanyDataSyncService
import logging

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


@https_fn.on_request()
def sync_companies(req: https_fn.Request) -> https_fn.Response:
    return https_fn.Response(sync_companies_exec())


@scheduler_fn.on_schedule(schedule="0 16 * * 1-5")
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


@https_fn.on_request()
def sync_companies_quote(req: https_fn.Request) -> https_fn.Response:
    return https_fn.Response(sync_companies_quote_exec())


@scheduler_fn.on_schedule(schedule="every 30 minutes from 09:30 to 16:00 on Mon, Tue, Wed, Thu, Fri")
def sync_companies_quotes_scheduled(event: scheduler_fn.ScheduledEvent) -> None:
    sync_companies_quote_exec()


def sync_companies_exec() -> str:
    logger.info("Updating company information")
    company_service = CompanyDataSyncService()
    for symbol in companies:
        company_service.sync_all(symbol)
    logger.info("Company information updated")
    return "Company information updated"


def sync_companies_quote_exec() -> str:
    logger.info("Updating company quotes")
    company_service = CompanyDataSyncService()
    for symbol in companies:
        company_service.sync_quote(symbol)
    logger.info("Company quotes updated")
    return "Company quotes updated"
