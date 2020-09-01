import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from time import time
from pprint import pprint
import os
import re
import json
from random import choice

if __name__ == '__main__':
  from ORM import Operations

else:
  from sabetay.spiders.ORM import Operations

def debug(variable):
  with open("debug.txt", "a") as text_file:
    text_file.write("\n{}".format(variable))

class RootSpider(scrapy.Spider):
  name = "root"
  search_url = 'https://www.zillow.com/homes/{}_rb/{}_p/'
  proxies = []
  properties = []
  errors = []

  agent = "//span[@class='cf-listing-agent-display-name']/text()"
  address = "//h1[@class='ds-address-container']/span/text()"
  price = "//span[@class='ds-value']/text()"

  def create_error(self, response, error):
    error = {}
    error['url'] = response.url
    error['error'] = str(error)

    self.errors.append(error)

  def save_errors(self):
    print("Saving errors")
    while len(self.errors) > 0:
      Operations.SaveError(self.errors.pop())

  def save_properties(self):
    print("Saving properties")
    properties = []
    while len(self.properties) > 0:
      properties.append(self.properties.pop())

    for _property in properties:
      Operations.SaveProperty(_property)

  def start_requests(self):
    zip_codes = Operations.QueryZIP()
    self.proxies = Operations.QueryProxy()
    for zip_code in zip_codes:
      url = self.search_url.format(zip_code.Value, 1)

      yield scrapy.Request(url=url,
        callback=self.parse_search_result_page,
        errback=self.errbacktest,
        meta={'root': url, 'proxy': choice(self.proxies).Value, 'page': 1, 'zip': zip_code.Value})

  def parse_search_result_page(self, response):
    data = re.search(r'listResults\":(.*?),\"hasListResults', response.text)
    if data != None:
      data = json.loads(data.group(1))

      for x in data:
        Operations.SaveProperty(x)

    else:
      return

    # if there is a next page, scrape it
    next_page_enabled = response.xpath("//a[@rel='next']/@disabled").extract_first() == None
    if next_page_enabled:
      url = response.xpath("//a[@rel='next']/@href").extract_first()

      if url == None: return

      yield scrapy.Request(self.search_url.format(response.meta.get('zip'), response.meta.get('page') + 1),
        callback=self.parse_listing,
        errback=self.errbacktest,
        meta={'zip': response.meta.get('zip'),
          'page': response.meta.get('page') + 1,
          'proxy': response.meta.get('proxy')})

  def parse_listing(self, response):
    if len(self.properties) > 0 and len(self.properties)%10 == 0:
      self.save_properties()

    if len(self.errors) > 0 and len(self.errors)%10 == 0:
      print(len(self.errors))
      self.save_errors()

    try:
      if response.status != 200:
        raise Exception(response.status)

      result = {}
      result['_id'] = re.search(r'(.*?)_zpid', response.url).group(1).split('/')[-1]
      result['agent'] = response.xpath(self.agent).extract_first('')
      result['address'] = response.xpath(self.address).extract_first()
      result['price'] = response.xpath(self.price
        ).extract_first(''
        ).replace('$', ''
        ).replace(',','')

      result['url'] = response.url

      result['status'] = None
      try:
        status_string = re.search(r'priceHistory\\\":(.*?)}]', response.text).group(1)
        status_string = status_string.replace("\\", '')
        status_string += '}]'

        if not status_string.startswith('[]'):
          status = json.loads(status_string)

          if 'event' in status[0]:
            result['status'] = status[0]['event']

      except Exception as e:
        pass


      self.properties.append(result)

    except Exception as e:
      self.create_error(response, e)




  def errbacktest(self, failiure):
    pass

  @classmethod
  def from_crawler(cls, crawler, *args, **kwargs):
    spider = super().from_crawler(crawler, *args, **kwargs)
    crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
    return spider

  def spider_closed(self, spider):
    self.save_properties()
    self.save_errors()
    pass

if __name__ == "__main__":
  process = CrawlerProcess({
      'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
  })

  process.crawl(RootSpider)
  process.start()
