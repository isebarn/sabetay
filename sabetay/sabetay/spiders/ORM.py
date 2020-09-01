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
  #Operations.init_db()
  data = {'zpid': '2078027924', 'id': '2078027924', 'hasImage': True, 'imgSrc': 'https://photos.zillowstatic.com/p_e/ISnmz8bvtuxrlh1000000000.jpg', 'detailUrl': 'https://www.zillow.com/homedetails/1228-E-70th-St-APT-1-Los-Angeles-CA-90001/2078027924_zpid/', 'statusType': 'FOR_SALE', 'statusText': 'Apartment for sale', 'price': '$735,000', 'unformattedPrice': 735000, 'festimate': None, 'address': '1228 E 70th St APT 1, Los Angeles, CA 90001', 'addressStreet': '1228 E 70th St Apt 1', 'addressState': 'CA', 'addressCity': 'Los Angeles', 'addressZipcode': '90001', 'countryCurrency': '$', 'beds': 2, 'baths': 1.0, 'area': 2226, 'latLong': {'latitude': 33.976435, 'longitude': -118.25313}, 'brokerName': 'C-21 Powerhouse Realty', 'brokerPhone': '(323) 562-7777', 'isZillowOwned': False, 'commute': None, 'variableData': {'type': 'TIME_ON_INFO', 'text': '16 hours ago', 'data': {'isFresh': False}}, 'hdpData': {'isReactHomeDetails': True, 'homeInfo': {'zpid': 2078027924, 'streetAddress': '1228 E 70th St APT 1', 'zipcode': '90001', 'city': 'Los Angeles', 'state': 'CA', 'latitude': 33.976435, 'longitude': -118.25313, 'price': 735000.0, 'dateSold': 0, 'bathrooms': 1.0, 'bedrooms': 2.0, 'livingArea': 2226.0, 'yearBuilt': 1963, 'lotSize': 5941.0, 'homeType': 'MULTI_FAMILY', 'homeStatus': 'FOR_SALE', 'photoCount': 3, 'imageLink': 'https://photos.zillowstatic.com/p_g/ISnmz8bvtuxrlh1000000000.jpg', 'daysOnZillow': 0, 'isFeatured': False, 'shouldHighlight': False, 'brokerId': 13862, 'contactPhone': '3235627777', 'listing_sub_type': {'is_FSBA': True}, 'isUnmappable': False, 'mediumImageLink': 'https://photos.zillowstatic.com/p_c/ISnmz8bvtuxrlh1000000000.jpg', 'isPreforeclosureAuction': False, 'homeStatusForHDP': 'FOR_SALE', 'priceForHDP': 735000.0, 'isListingOwnedByCurrentSignedInAgent': False, 'timeOnZillow': 1598916780000, 'isListingClaimedByCurrentSignedInUser': False, 'hiResImageLink': 'https://photos.zillowstatic.com/p_f/ISnmz8bvtuxrlh1000000000.jpg', 'watchImageLink': 'https://photos.zillowstatic.com/p_j/ISnmz8bvtuxrlh1000000000.jpg', 'contactPhoneExtension': '', 'lotId': 2115423631, 'tvImageLink': 'https://photos.zillowstatic.com/p_m/ISnmz8bvtuxrlh1000000000.jpg', 'tvCollectionImageLink': 'https://photos.zillowstatic.com/p_l/ISnmz8bvtuxrlh1000000000.jpg', 'tvHighResImageLink': 'https://photos.zillowstatic.com/p_n/ISnmz8bvtuxrlh1000000000.jpg', 'zillowHasRightsToImages': False, 'lotId64': 2115423631, 'desktopWebHdpImageLink': 'https://photos.zillowstatic.com/p_h/ISnmz8bvtuxrlh1000000000.jpg', 'hideZestimate': False, 'isPremierBuilder': False, 'isZillowOwned': False, 'currency': 'USD', 'country': 'USA', 'streetAddressOnly': '1228 E 70th St', 'unit': '1', 'moveInReady': False}}, 'isSaved': False, 'hasOpenHouse': False, 'isUserClaimingOwner': False, 'isUserConfirmedClaim': False, 'pgapt': 'ForSale', 'sgapt': 'For Sale (Broker)', 'photoCount': 3, 'zestimate': None, 'shouldShowZestimateAsPrice': False, 'has3DModel': False, 'hasVideo': False, 'list': True, 'relaxed': False}
  Operations.SaveProperty(data)