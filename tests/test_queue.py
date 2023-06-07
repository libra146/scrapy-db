import pytest
from scrapy import Request, Spider

from src.scrapy_db.queue import LifoQueue


class TestSpider(Spider):
    name = 'test'

    def parse(self, response, **kwargs):
        return Request(url='https://www.baidu.com', callback=self.parse, meta={'meta': 'test'})


def test_fifo_queue():
    settings = {
        'DB_URL': 'sqlite:///:memory:'
    }
    spider = TestSpider
    spider.settings = settings
    queue = LifoQueue(spider, 'test', 'queue')
    assert queue.db.db._meta.table_name == 'test'
    assert queue.spider.name == 'test'
    assert hasattr(queue.serializer, 'dumps')
    assert hasattr(queue.serializer, 'loads')
    with pytest.raises(TypeError) as _:
        LifoQueue(spider, 'test', 'queue', object)

    queue = LifoQueue(spider, 'test', 'queue')
    request = Request(url='https://www.baidu.com', meta={'key1': 'key1'})
    encode_request = queue._encode_request(request)
    assert isinstance(encode_request, str)
    decode_request = queue._decode_request(encode_request)
    assert isinstance(decode_request, Request)
    assert decode_request.meta['key1'] == 'key1'
    assert decode_request.url == 'https://www.baidu.com'

    queue.push(request)
    assert len(queue) == 1
