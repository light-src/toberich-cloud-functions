from service.firestore import FirestoreService


class AnalysisService:
    def __init__(self):
        self.firestore = FirestoreService()

    def update_analysis(self, symbol, quote):
        balancesheet = self.firestore.get_balancesheet(symbol, "2024", "FY")

        # Calculate NCAV
        nca = balancesheet.get("totalCurrentAssets") - balancesheet.get("totalLiabilities")
        market_cap = quote["marketCap"]
        ncav_ratio = nca / market_cap

        self.firestore.store_analysis(symbol, {"ncav": ncav_ratio})
