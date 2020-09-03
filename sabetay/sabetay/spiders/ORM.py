import os
import json
from datetime import datetime
from sqlalchemy import ForeignKey, desc, create_engine, func, JSON, Column, BigInteger, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased, relationship, joinedload, lazyload
from pprint import pprint 

engine = create_engine(os.environ.get('SABETAY_DATABASE'), echo=False)
Base = declarative_base()

def read_file(filename):
  file = open(filename, "r")
  return file.read().splitlines()

def safe_int_cast(value, tracer, listing):
  try:
    if isinstance(value, str):
      return int(value) if value != 'No' and value != None and value.isdigit() else None

  except Exception as e:
    print("{}: {}: {}: {}".format(value, tracer, listing, e))
    return None

class Error(Base):
  __tablename__ = 'error'

  Id = Column('id', Integer, primary_key=True)
  Error = Column('error', JSON)

  def __init__(self, data):
    self.Error = data

class ZIP(Base):
  __tablename__ = 'zip'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String, unique=True)

  def __init__(self, zip_code):
    self.Value = zip_code

class Proxy(Base):
  __tablename__ = 'proxy'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String)

  def __init__(self, proxy):
    self.Value = proxy

class PropertyStatus(Base):
  __tablename__ = 'property_status'

  Id = Column('id', Integer, primary_key=True)
  Value = Column('value', String) # Property Status (Pending, Contingent, etc)
  Date = Column('scrape_date', DateTime) # Date Updated
  _Property = Column('property', Integer, ForeignKey('property.id'))

  def __init__(self, data):
    self.Value = data['statusType']
    self.Date = datetime.now()
    self._Property = data['id']

class Property(Base):
  __tablename__ = 'property'

  Id = Column('id', Integer, primary_key=True)
  BrokerName = Column('broker_name', String)
  BrokerPhone = Column('broker_phone', String)
  Address = Column('address', String)
  Price = Column('price', Integer)
  URL = Column('url', String)
  statuses = relationship('PropertyStatus', lazy="joined") #Property Status (Pending, Contingent, etc)


  def __init__(self, data):
    self.Id = data.get('id') # Zillow/Redfin ID

    self.BrokerName = data.get('brokerName') #Agent Name
    self.BrokerPhone = data.get('brokerPhone') #Agent Name
    self.Address = data.get('address') #Property Address
    self.Price = safe_int_cast(data.get('unformattedPrice'), 'price', self.Id) #Property Price
    self.URL = data.get('detailUrl')


class Operations:
  def SaveZIP(data):
    session.add(data)
    session.commit()

  def QueryZIP():
    return session.query(ZIP).all()

  def QueryProxy():
      return session.query(Proxy).all()

  def SaveProxy(data):
    session.add(data)
    session.commit()

  def SaveError(data):
    session.add(Error(data))
    session.commit()

  def SaveProperty(data):
    _property = session.query(Property).filter_by(Id=data['id']).first()
    if _property == None:
      _property = Property(data)
      session.add(_property)
      session.commit()

    statuses = _property.statuses
    if len(statuses) > 0:
      newest_status = statuses[-1]

      if newest_status.Value != data.get('statusType'):
        print("NEW STATUS: {}".format(data['id']))
        session.add(PropertyStatus(data))
        session.commit()

    else:
      session.add(PropertyStatus(data))
      session.commit()

  def init_db():
    zip_codes = [ZIP(zip_code) for zip_code in read_file('zipcodes.txt')]
    for zip_code in zip_codes:
      Operations.SaveZIP(zip_code)

    proxies = [Proxy(proxy) for proxy in read_file('proxies.txt')]
    for proxy in proxies:
      Operations.SaveProxy(proxy)



Base.metadata.create_all(engine)
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

if __name__ == "__main__":
  Operations.init_db()
