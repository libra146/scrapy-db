import datetime
from unittest import mock

import pytest
from peewee import DateTimeField, BigAutoField
from scrapy import Request, Spider

from scrapy_db.queue import LifoQueue, PriorityQueue

# The default fields for model classes
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now),
               'update_time': DateTimeField(default=datetime.datetime.now),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


class MySpider(Spider):
    name = 'test'

    def parse(self, response, **kwargs):
        return Request(url='https://www.baidu.com', callback=self.parse, meta={'meta': 'test'})


@mock.patch('scrapy_db.db._attributes', _attributes)
def test_fifo_queue():
    settings = {
        'DB_URL': 'sqlite:///:memory:'
    }
    spider = MySpider
    spider.settings = settings
    queue = LifoQueue(spider, 'test', 'queue')
    assert queue.db.db._meta.table_name == 'test'
    assert queue.spider.name == 'test'
    assert hasattr(queue.serializer, 'dumps')
    assert hasattr(queue.serializer, 'loads')
    with pytest.raises(TypeError) as _:
        LifoQueue(spider, 'test', 'queue', object)

    queue = PriorityQueue(spider, 'test', 'queue')
    request = Request(url='https://www.baidu.com', meta={'key1': 'key1'})
    encode_request = queue._encode_request(request)
    assert isinstance(encode_request, str)
    decode_request = queue._decode_request(encode_request)
    assert isinstance(decode_request, Request)
    assert decode_request.meta['key1'] == 'key1'
    assert decode_request.url == 'https://www.baidu.com'

    queue.push(request)
    assert len(queue) == 1
