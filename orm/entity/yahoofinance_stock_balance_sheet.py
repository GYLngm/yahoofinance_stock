from sqlalchemy import Column, String, Float, Date
from orm.dbconfig import Base


class Balance(Base):
    __tablename__ = 'yahoofinance_stock_balance_sheet'

    Code = Column(String, primary_key=True)
    ReportDate = Column(Date, primary_key=True)
    TotalAssets = Column(Float)
    CurrentAssets = Column(Float)
    CashCashEquivalentsAndShortTermInvestments = Column(Float)
    CashAndCashEquivalents = Column(Float)
    OtherShortTermInvestments = Column(Float)
    AccountsReceivable = Column(Float)
    Inventory = Column(Float)
    OtherCurrentAssets = Column(Float)
    TotalNonCurrentAssets = Column(Float)
    NetPPE = Column(Float)
    GrossPPE = Column(Float)
    AccumulatedDepreciation = Column(Float)
    InvestmentsAndAdvances = Column(Float)
    Goodwill = Column(Float)
    OtherIntangibleAssets = Column(Float)
    OtherNonCurrentAssets = Column(Float)
    TotalLiabilitiesNetMinorityInterest = Column(Float)
    CurrentLiabilities = Column(Float)
    CurrentDebt = Column(Float)
    AccountsPayable = Column(Float)
    IncomeTaxPayable = Column(Float)
    CurrentAccruedExpenses = Column(Float)
    CurrentDeferredRevenue = Column(Float)
    OtherCurrentLiabilities = Column(Float)
    TotalNonCurrentLiabilitiesNetMinorityInterest = Column(Float)
    LongTermDebt = Column(Float)
    NonCurrentDeferredTaxesLiabilities = Column(Float)
    NonCurrentDeferredRevenue = Column(Float)
    OtherNonCurrentLiabilities = Column(Float)
    StockholdersEquity = Column(Float)
    CapitalStock = Column(Float)
    RetainedEarnings = Column(Float)
    GainsLossesNotAffectingRetainedEarnings = Column(Float)

    def __repr__(self):
        return """
            <Balance(
                Code = '%s'
                ReportDate = '%s'
                TotalAssets = '%s'
                CurrentAssets = '%s'
                CashCashEquivalentsAndShortTermInvestments = '%s'
                CashAndCashEquivalents = '%s'
                OtherShortTermInvestments = '%s'
                AccountsReceivable = '%s'
                Inventory = '%s'
                OtherCurrentAssets = '%s'
                TotalNonCurrentAssets = '%s'
                NetPPE = '%s'
                GrossPPE = '%s'
                AccumulatedDepreciation = '%s'
                InvestmentsAndAdvances = '%s'
                Goodwill = '%s'
                OtherIntangibleAssets = '%s'
                OtherNonCurrentAssets = '%s'
                TotalLiabilitiesNetMinorityInterest = '%s'
                CurrentLiabilities = '%s'
                CurrentDebt = '%s'
                AccountsPayable = '%s'
                IncomeTaxPayable = '%s'
                CurrentAccruedExpenses = '%s'
                CurrentDeferredRevenue = '%s'
                OtherCurrentLiabilities = '%s'
                TotalNonCurrentLiabilitiesNetMinorityInterest = '%s'
                LongTermDebt = '%s'
                NonCurrentDeferredTaxesLiabilities = '%s'
                NonCurrentDeferredRevenue = '%s'
                OtherNonCurrentLiabilities = '%s'
                StockholdersEquity = '%s'
                CapitalStock = '%s'
                RetainedEarnings = '%s'
                GainsLossesNotAffectingRetainedEarnings = '%s'
            )>
        """ % (
            self.Code,
            self.ReportDate,
            self.TotalAssets,
            self.CurrentAssets,
            self.CashCashEquivalentsAndShortTermInvestments,
            self.CashAndCashEquivalents,
            self.OtherShortTermInvestments,
            self.AccountsReceivable,
            self.Inventory,
            self.OtherCurrentAssets,
            self.TotalNonCurrentAssets,
            self.NetPPE,
            self.GrossPPE,
            self.AccumulatedDepreciation,
            self.InvestmentsAndAdvances,
            self.Goodwill,
            self.OtherIntangibleAssets,
            self.OtherNonCurrentAssets,
            self.TotalLiabilitiesNetMinorityInterest,
            self.CurrentLiabilities,
            self.CurrentDebt,
            self.AccountsPayable,
            self.IncomeTaxPayable,
            self.CurrentAccruedExpenses,
            self.CurrentDeferredRevenue,
            self.OtherCurrentLiabilities,
            self.TotalNonCurrentLiabilitiesNetMinorityInterest,
            self.LongTermDebt,
            self.NonCurrentDeferredTaxesLiabilities,
            self.NonCurrentDeferredRevenue,
            self.OtherNonCurrentLiabilities,
            self.StockholdersEquity,
            self.CapitalStock,
            self.RetainedEarnings,
            self.GainsLossesNotAffectingRetainedEarnings,
        )

