from sqlalchemy import Column, String, Float, Date
from orm.dbconfig import Base


class ValuationMeasures(Base):
    __tablename__ = 'yahoofinance_stock_valuation_measures'

    Code = Column(String, primary_key=True)
    ValuationMethod = Column(String, primary_key=True)
    ReportDate = Column(Date, primary_key=True)

    MarketCap = Column(Float)
    EnterpriseValue = Column(Float)
    PeRatio = Column(Float)
    ForwardPeRatio = Column(Float)
    PegRatio = Column(Float)
    PsRatio = Column(Float)
    PbRatio = Column(Float)
    EnterprisesValueRevenueRatio = Column(Float)
    EnterprisesValueEBITDARatio = Column(Float)

    def __repr__(self):
        return '''<ValuationMeasures(
            Code = "%s"
            ValuationMethod = "%s"
            ReportDate = "%s"
            MarketCap = "%s"
            EnterpriseValue = "%s"
            PeRatio = "%s"
            ForwardPeRatio = "%s"
            PegRatio = "%s"
            PsRatio = "%s"
            PbRatio = "%s"
            EnterprisesValueRevenueRatio = "%s"
            EnterprisesValueEBITDARatio = "%s"
        )>''' % (
            self.Code,
            self.ValuationMethod,
            self.ReportDate,
            self.MarketCap,
            self.EnterpriseValue,
            self.PeRatio,
            self.ForwardPeRatio,
            self.PegRatio,
            self.PsRatio,
            self.PbRatio,
            self.EnterprisesValueRevenueRatio,
            self.EnterprisesValueEBITDARatio,
        )
