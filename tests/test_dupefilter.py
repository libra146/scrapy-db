import hashlib
from unittest import mock

import pytest
from peewee import OperationalError
from scrapy.http import Request
from scrapy.settings import Settings

from scrapy_db.db import DBModel
from scrapy_db.dupefilter import DBDupeFilter


@pytest.fixture(autouse=True)
def get_df(get_dupelifter_db):
    df = DBDupeFilter(get_dupelifter_db)
    yield df


def test_db_dupefilter(get_df):
    df = get_df
    req = Request('https://example.com')

    def same_request():
        assert not df.request_seen(req)
        assert df.request_seen(req)

    def diff_method():
        diff_method = Request('https://example.com', method='POST')
        assert df.request_seen(req)
        assert not df.request_seen(diff_method)

    def diff_url():
        diff_url = Request('https://example2.com')
        assert df.request_seen(req)
        assert not df.request_seen(diff_url)

    same_request()
    diff_method()
    diff_url()


def test_overridable_request_fingerprinter(get_df):
    df = get_df
    req = Request('https://example.com')
    with mock.patch('scrapy_db.dupefilter.fingerprint') as f:
        f.return_value = hashlib.sha1('xxx'.encode()).digest()
        assert not df.request_seen(req)
        assert f.called


def test_clear_deletes(get_df):
    get_df.clear()
    with pytest.raises(OperationalError) as e:
        get_df.request_seen(Request(url='https://example.com'))
    assert 'no such table' in str(e.value)


def test_close_calls_clear(get_df):
    get_df.clear = mock.Mock(wraps=get_df.clear)
    get_df.close()
    get_df.close(reason='foo')
    assert get_df.clear.call_count == 2


def test_log_dupes(get_df):
    def _test(df, dupes, logcount):
        df.logger.debug = mock.Mock(wraps=df.logger.debug)
        for i in range(dupes):
            req = Request('http://example')
            df.log(req, spider=mock.Mock())
        assert df.logger.debug.call_count == logcount

    df_quiet = DBDupeFilter(get_df)  # debug=False
    _test(df_quiet, 5, 1)

    df_debug = DBDupeFilter(get_df, debug=True)
    _test(df_debug, 5, 5)


@pytest.fixture
def settings():
    return Settings({
        'DUPEFILTER_DEBUG': True,
        'DB_URL': 'sqlite:///:memory:'
    })


def test_from_settings(settings, get_dupelifter_db):
    def assert_dupefilter(d):
        assert d.debug
        assert isinstance(d.table, DBModel)
        assert d.table is get_dupelifter_db

    with mock.patch('scrapy_db.db.DBModel.build_model_from_settings') as f:
        f.return_value = get_dupelifter_db
        df = DBDupeFilter.from_settings(settings)
        assert_dupefilter(df)

        crawler = mock.Mock(settings=settings)
        df = DBDupeFilter.from_crawler(crawler)
        assert_dupefilter(df)

        spider = mock.Mock()
        spider.settings = settings
        df = DBDupeFilter.from_spider(spider)
        assert_dupefilter(df)

        assert f.called
