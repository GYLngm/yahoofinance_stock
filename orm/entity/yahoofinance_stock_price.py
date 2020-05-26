from sqlalchemy import Column, String, Float, Date
from orm.dbconfig import Base


class Price(Base):
    __tablename__ = 'yahoofinance_stock_price'

    Code = Column(String, primary_key=True)
    Date = Column(Date, primary_key=True)

    Open = Column(Float)
    High = Column(Float)
    Low = Column(Float)
    Close = Column(Float)
    AdjClose = Column(Float)
    Volume = Column(Float)

    def __repr__(self):
        return """
            <Price(
                Code='%s', Date='%s',
                Open='%s', High = '%s', Low = '%s', Close = '%s', AdjClose = '%s', Volume = '%s'
            )>
        """ % (
            self.Code, self.Date, self.Open, self.High, self.Low, self.Close, self.AdjClose, self.Volume
        )