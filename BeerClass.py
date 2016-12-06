from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import FLOAT, VARCHAR, INTEGER, DATETIME, BLOB
from pandas import to_datetime
import zlib

Base = declarative_base()

class Beer(Base):
    __tablename__ = 'beer'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }
    id = Column('id', INTEGER, primary_key=True)
    abv = Column(FLOAT)
    beer_id = Column(INTEGER(unsigned=True))
    brewer_id = Column(INTEGER(unsigned=True))
    beer_name = Column(VARCHAR(76))
    beer_style = Column(VARCHAR(35))
    review_appearance = Column(FLOAT(precision=3, scale=1, unsigned=True))
    review_aroma = Column(FLOAT(precision=3, scale=1, unsigned=True))
    review_overall = Column(FLOAT(precision=3, scale=1, unsigned=True))
    review_palate = Column(FLOAT(precision=3, scale=1, unsigned=True))
    profile_name = Column(VARCHAR(16))
    review_taste = Column(FLOAT(precision=3, scale=1, unsigned=True))
    text = Column(BLOB(2424))
    time = Column(DATETIME)

    def __init__(self, row):
        self.abv = row[0]
        self.beer_id = row[1]
        self.brewer_id = row[2]
        self.beer_name = row[3]
        self.beer_style = row[4]
        self.review_appearance = row[5]
        self.review_aroma = row[6]
        self.review_overall = row[7]
        self.review_palate = row[8]
        self.profile_name = row[9]
        self.review_taste = row[10]
        self.text = row[11]
        f = '%Y-%m-%d %H:%M:%S'
        t = to_datetime(row[12]).strftime(f)
        self.time = t

