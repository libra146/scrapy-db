import datetime
import time
from unittest import mock

import pytest
from peewee import DateTimeField, BigAutoField
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.settings import Settings

from scrapy_db import defaults
from scrapy_db.spiders import DBSpider, DBCrawlSpider

# The default fields for model classes
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now),
               'update_time': DateTimeField(default=datetime.datetime.now),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


class MySpider(DBSpider):
    name = 'myspider'


class MyCrawlSpider(DBCrawlSpider):
    name = 'myspider'


def get_crawler(**kwargs):
    settings = Settings()
    settings.set('DB_URL', 'sqlite:///:memory:')
    return mock.Mock(settings=settings, **kwargs)


def test_crawler_required():
    with pytest.raises(ValueError) as e:
        MySpider().setup_db()
    assert 'crawler' in str(e.value)


@mock.patch('scrapy_db.spiders.DBModel')
def test_crawler_table_name(db):
    my_spider = MySpider()
    my_spider.crawler = get_crawler()
    my_spider.setup_db()
    assert my_spider.table_name == defaults.START_URLS_TABLE % {'spider': my_spider.name}


@mock.patch('scrapy_db.db._attributes', _attributes)
def test_invalid_idle_time():
    my_spider = MySpider()
    my_spider.max_idle_time = 'x'
    my_spider.crawler = get_crawler()
    with pytest.raises(ValueError) as e:
        my_spider.setup_db()
    assert "max_idle_time" in str(e.value)


def test_invalid_batch_size():
    my_spider = MySpider()
    my_spider.db_batch_size = 'x'
    my_spider.crawler = get_crawler()
    with pytest.raises(ValueError) as e:
        my_spider.setup_db()
    assert 'db_batch_size' in str(e.value)


@mock.patch('scrapy_db.spiders.DBModel')
def test_via_from_crawler(DBModel):
    db = DBModel.build_model_from_settings.return_value = mock.Mock()
    crawler = get_crawler()
    myspider = MySpider.from_crawler(crawler)
    assert myspider.db is db
    DBModel.build_model_from_settings.assert_called_with(crawler.settings,
                                                         defaults.START_URLS_TABLE % {'spider': myspider.name},
                                                         'start_url')
    crawler.signals.connect.assert_called_with(myspider.spider_idle, signal=signals.spider_idle)
    # Second call does nothing.
    db = myspider.db
    crawler.signals.connect.reset_mock()
    myspider.setup_db()
    assert myspider.db is db
    assert crawler.signals.connect.call_count == 0


@mock.patch('scrapy_db.spiders.DBModel')
@pytest.mark.parametrize('spider_cls', [
    MySpider,
    MyCrawlSpider,
])
def test_from_crawler_with_spider_arguments(db, spider_cls):
    db.build_model_from_settings.return_value = mock.Mock()
    crawler = get_crawler()
    spider = spider_cls.from_crawler(
        crawler, 'foo',
        table_name='%(spider)s_start_urls',
        db_batch_size='1000',
        max_idle_time='100',
    )
    assert spider.name == 'foo'
    assert spider.table_name == 'foo_start_urls'
    assert spider.db_batch_size == 1000
    assert spider.max_idle_time == 100


@pytest.mark.parametrize('spider_cls', [
    MySpider,
    MyCrawlSpider
])
@mock.patch('scrapy_db.spiders.DBModel')
def test_consume_urls_from_db(db, spider_cls):
    # test push and fetch_data
    queue = []
    mock_db = mock.Mock()
    mock_db.push = lambda x: queue.append(x)
    mock_db.fetch_data = lambda x: [mock.Mock(start_url=a) for a in queue]
    mock_db.__len__ = lambda x: len(queue)
    db.build_model_from_settings.return_value = mock_db

    batch_size = 5
    table_name = 'foo_start_urls'
    crawler = get_crawler()
    crawler.settings.setdict({
        'START_URLS_TABLE': table_name,
        'CONCURRENT_REQUESTS': batch_size,
    })
    spider = spider_cls.from_crawler(crawler)
    urls = [
        f'https://example.com/{i}' for i in range(batch_size * 2)
    ]
    for url in urls:
        spider.db.push({'url': url})

    # First call is to start requests.
    start_requests = list(spider.start_requests())
    assert len(start_requests) == batch_size * 2

    queue.clear()
    for url in urls:
        spider.db.push({'url': url})

    with pytest.raises(DontCloseSpider):
        spider.spider_idle()
    with pytest.raises(DontCloseSpider):
        spider.spider_idle()
    assert crawler.engine.crawl.call_count == batch_size * 2

    queue.clear()
    for url in urls:
        spider.db.push(url)
    with pytest.raises(DontCloseSpider):
        spider.spider_idle()

    queue.clear()
    for url in urls:
        spider.db.push({'url': None})
    with pytest.raises(DontCloseSpider):
        spider.spider_idle()

    queue.clear()
    for url in urls:
        spider.db.push(url)
    spider.make_request_from_data = lambda x: None
    with pytest.raises(DontCloseSpider):
        spider.spider_idle()

    queue.clear()
    for url in urls:
        spider.db.push({'url': url})
    spider.max_idle_time = -1
    spider.schedule_next_requests = lambda: time.sleep(0.1)
    assert spider.spider_idle() is None
