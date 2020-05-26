from sqlalchemy import Column, String, Float, Date
from orm.dbconfig import Base


class IncomeStatement(Base):
    __tablename__ = 'yahoofinance_stock_income_statement'

    ReportDate = Column(Date, primary_key=True)
    Code = Column(String, primary_key=True)
    ValuationMethod = Column(String, primary_key=True)

    TotalRevenue = Column(Float)
    CostOfRevenue = Column(Float)
    GrossProfit = Column(Float)
    OperatingExpense = Column(Float)
    ResearchAndDevelopment = Column(Float)
    SellingGeneralAndAdministration = Column(Float)
    OperatingIncome = Column(Float)
    InterestExpense = Column(Float)
    OtherIncomeExpense = Column(Float)
    PretaxIncome = Column(Float)
    TaxProvision = Column(Float)
    NetIncomeContinuousOperations = Column(Float)
    NetIncome = Column(Float)
    NetIncomeCommonStockholders = Column(Float)
    BasicEPS = Column(Float)
    DilutedEPS = Column(Float)
    BasicAverageShares = Column(Float)
    DilutedAverageShares = Column(Float)
    Ebitda = Column(Float)

    def __repr__(self):
        return """
            <IncomeStatement(
                ReportDate '%s',
                Code = '%s',
                ValuationMethod = '%s',
                TotalRevenue = '%s',
                CostOfRevenue = '%s',
                GrossProfit = '%s',
                OperatingExpense = '%s',
                ResearchAndDevelopment = '%s',
                SellingGeneralAndAdministration = '%s',
                OperatingIncome = '%s',
                InterestExpense = '%s',
                OtherIncomeExpense = '%s',
                PretaxIncome = '%s',
                TaxProvision = '%s',
                NetIncomeContinuousOperations = '%s',
                NetIncome = '%s',
                NetIncomeCommonStockholders = '%s',
                BasicEPS = '%s',
                DilutedEPS = '%s',
                BasicAverageShares = '%s',
                DilutedAverageShares = '%s',
                Ebitda = '%s',
            )>
        """ % (
            self.ReportDate,
            self.Code,
            self.ValuationMethod,
            self.TotalRevenue,
            self.CostOfRevenue,
            self.GrossProfit,
            self.OperatingExpense,
            self.ResearchAndDevelopment,
            self.SellingGeneralAndAdministration,
            self.OperatingIncome,
            self.InterestExpense,
            self.OtherIncomeExpense,
            self.PretaxIncome,
            self.TaxProvision,
            self.NetIncomeContinuousOperations,
            self.NetIncome,
            self.NetIncomeCommonStockholders,
            self.BasicEPS,
            self.DilutedEPS,
            self.BasicAverageShares,
            self.DilutedAverageShares,
            self.Ebitda,
        )