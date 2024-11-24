from service.firestore import FirestoreService
from datetime import datetime
from companies import companies


class TaskStateService:
    def __init__(self):
        self.firestore = FirestoreService()

    def get_info_task_state(self):
        return self.firestore.get_task_state("company_info")

    def get_quote_task_state(self):
        return self.firestore.get_task_state("company_quotes")

    def get_update_company_info_companies(self) -> list:
        latest = self.get_info_task_state()
        if latest is None or latest['date'] != datetime.now().strftime("%Y-%m-%d"):
            return companies
        idx = companies.index(latest['latest_symbol'])
        return companies[idx + 1:]

    def get_update_company_quotes_companies(self) -> list:
        latest = self.get_quote_task_state()
        if latest is None or latest['latest_symbol'] is companies[-1]:
            return companies
        idx = companies.index(latest['latest_symbol'])
        return companies[idx + 1:]

    def set_latest_updated_company_info(self, symbol):
        self.firestore.set_task_state("company_info", {
            'latest_symbol': symbol,
            'date': datetime.now().strftime("%Y-%m-%d")
        })

    def set_latest_updated_company_quote(self, symbol):
        self.firestore.set_task_state("company_quotes", {
            'latest_symbol': symbol,
            'date': datetime.now().strftime("%Y-%m-%d")
        })
