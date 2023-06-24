import logging

from scrapy import Request

from example.items import TestItem
from scrapy_db.spiders import DBSpider


class DemoSpider(DBSpider):
    name = "demo"

    def __init__(self, *args, **kwargs):
        logging.getLogger('scrapy.core.scraper').setLevel(logging.INFO)
        logging.getLogger('scrapy.core.engine').setLevel(logging.DEBUG)
        super(DemoSpider, self).__init__(*args, **kwargs)

    def parse(self, response, **kwargs):
        result = response.xpath('//div[@class="el-card item m-t is-hover-shadow"]')
        next_url = None
        for a in result:
            item = TestItem()
            item['title'] = a.xpath('.//h2[@class="m-b-sm"]/text()').get()
            item['fraction'] = a.xpath('.//p[@class="score m-t-md m-b-n-sm"]/text()').get().strip()
            item['country'] = a.xpath('.//div[@class="m-v-sm info"]/span[1]/text()').get()
            item['time'] = a.xpath('.//div[@class="m-v-sm info"]/span[3]/text()').get()
            item['date'] = a.xpath('.//div[@class="m-v-sm info"][2]/span/text()').get()
            url = a.xpath('.//a[@class="name"]/@href').get()
            next_url = a.xpath('//a[@class="next"]/@href').get()
            yield Request(url=response.urljoin(url), callback=self.parse_person, meta={'item': item})
        if next_url:
            yield Request(url=response.urljoin(next_url), callback=self.parse)

    def parse_person(self, response):
        item = response.meta['item']
        item['director'] = response.xpath(
            '//div[@class="directors el-row"]//p[@class="name text-center m-b-none m-t-xs"]/text()').get()
        yield item
