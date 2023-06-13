# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TestItem(scrapy.Item):
    title = scrapy.Field()
    fraction = scrapy.Field()
    country = scrapy.Field()
    time = scrapy.Field()
    date = scrapy.Field()
    director = scrapy.Field()
