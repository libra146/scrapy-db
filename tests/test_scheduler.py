import importlib
from unittest import mock

import pytest
from scrapy import Request
from scrapy.settings import Settings

from scrapy_db.scheduler import Scheduler


@pytest.fixture()
def crawler():
    c = get_crawler()
    return c


def get_crawler(**kwargs):
    settings = Settings()
    settings.set('DB_URL', 'sqlite:///:memory:')
    return mock.Mock(settings=settings, **kwargs)


@pytest.fixture()
def scheduler(crawler):
    return Scheduler.from_crawler(crawler)


def test_crawler(crawler):
    crawler.stats = mock.Mock()
    scheduler = Scheduler.from_crawler(crawler)
    assert scheduler.stats == crawler.stats

    crawler.settings.setdict({
        'SCHEDULER_QUEUE_CLASS': 'class',
        'SCHEDULER_SERIALIZER': 'json',
    })
    scheduler = Scheduler.from_crawler(crawler)
    assert scheduler.queue_cls == 'class'
    assert scheduler.serializer == importlib.import_module('json')

    crawler.settings.set('SCHEDULER_IDLE_BEFORE_CLOSE', -1)
    with pytest.raises(TypeError) as e:
        _ = Scheduler.from_crawler(crawler)
    assert 'idle_before_close' in str(e.value)


def test_queue_len(scheduler):
    scheduler.queue = mock.Mock(__len__=lambda x: 5)
    assert len(scheduler) == 5


def test_close(scheduler):
    scheduler.flush = mock.Mock()
    scheduler.persist = False
    scheduler.close()
    assert scheduler.flush.called


def test_flush(scheduler):
    scheduler.df = mock.Mock()
    scheduler.queue = mock.Mock()
    scheduler.flush()
    assert scheduler.df.clear.called
    assert scheduler.queue.clear.called
    scheduler.flush()
    scheduler.flush()
    assert scheduler.df.clear.call_count == 3
    assert scheduler.queue.clear.call_count == 3


@mock.patch('scrapy_db.scheduler.len')
def test_has_pending_requests(len_, scheduler):
    len_.return_value = 4
    assert scheduler.has_pending_requests() is True
    len_.return_value = 0
    assert scheduler.has_pending_requests() is False
    assert len_.call_count == 2


def test_next_request(scheduler):
    scheduler.queue = mock.Mock()
    scheduler.stats = mock.Mock()
    scheduler.queue.pop = mock.Mock()
    scheduler.queue.pop.return_value = Request(url='https://example.com')
    scheduler.idle_before_close = 30
    scheduler.spider = mock.Mock()

    assert scheduler.next_request().url == 'https://example.com'
    assert scheduler.queue.pop.called
    scheduler.queue.pop.assert_called_with(30)

    assert scheduler.stats.inc_value.called
    scheduler.stats.inc_value.assert_called_with('scheduler/dequeued/db', spider=scheduler.spider)


@mock.patch('scrapy_db.scheduler.load_object')
@mock.patch('scrapy_db.scheduler.len')
def test_open(len_, load_object, scheduler, mocker):
    load_object.side_effect = TypeError
    scheduler.queue_cls = mock.Mock()
    scheduler.dupefilter_cls = mock.Mock()
    spider = mock.Mock()
    scheduler.flush_on_start = True

    with pytest.raises(ValueError) as _:
        scheduler.open(spider)

    load_object.side_effect = None
    scheduler.flush = mock.Mock()
    len_.return_value = 1
    scheduler.open(spider)
    assert scheduler.flush.called
    scheduler.open(spider)
    assert scheduler.flush.call_count == 2


def test_enqueue_request(scheduler):
    scheduler.queue = mock.Mock()
    scheduler.df = mock.Mock()
    scheduler.stats = mock.Mock()
    request = Request(url='https://example.com')
    spider = mock.Mock()
    scheduler.spider = spider

    request.dont_filter = False
    scheduler.df.request_seen.return_value = True
    assert scheduler.enqueue_request(request) is False
    assert scheduler.df.log.called

    request.dont_filter = True
    assert scheduler.enqueue_request(request) is True
    scheduler.stats.inc_value.assert_called_with('scheduler/enqueued/db', spider=spider)
    assert scheduler.queue.push.called
