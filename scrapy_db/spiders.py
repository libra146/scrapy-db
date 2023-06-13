import time
from collections.abc import Iterable

from scrapy import signals, FormRequest
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider

from . import defaults
from .db import DBModel
from .utils import TextColor, is_dict


class DBBase(object):
    """
    Base class
    """
    table_name = None
    db_batch_size = None

    db = None

    spider_idle_start_time = int(time.time())
    max_idle_time = None

    def start_requests(self):
        return self.next_requests()

    def setup_db(self, crawler=None):
        if self.db is not None:
            return

        if crawler is None:
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings

        if self.table_name is None:
            self.table_name = settings.get(
                'START_URLS_TABLE', defaults.START_URLS_TABLE,
            )

        self.table_name = self.table_name % {'spider': self.name}

        if not self.table_name.strip():
            raise ValueError("table_name must not be empty")

        if self.db_batch_size is None:
            self.db_batch_size = settings.getint('CONCURRENT_REQUESTS')

        try:
            self.db_batch_size = int(self.db_batch_size)
        except (TypeError, ValueError):
            raise ValueError("db_batch_size must be an integer")

        self.logger.info("Reading start URLs from table_name '%(table_name)s' "
                         "(batch size: %(db_batch_size)s)",
                         self.__dict__)

        self.db = DBModel.build_model_from_settings(crawler.settings, self.table_name, 'start_url')

        if self.max_idle_time is None:
            self.max_idle_time = settings.getint(
                "MAX_IDLE_TIME_BEFORE_CLOSE",
                defaults.MAX_IDLE_TIME
            )

        try:
            self.max_idle_time = int(self.max_idle_time)
        except (TypeError, ValueError):
            raise ValueError("max_idle_time must be an integer")

        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        found = 0
        datas = self.db.fetch_data(batch_size=self.db_batch_size)
        for data in datas:
            reqs = self.make_request_from_data(data)
            if isinstance(reqs, Iterable):
                for req in reqs:
                    yield req
                    found += 1
                    self.logger.info(f'start req url:{req.url}')
            elif reqs:
                yield reqs
                found += 1
            else:
                self.logger.debug(f"Request not made from data: {data}")

        if found:
            self.logger.debug(f"Read {found} requests from '{self.table_name}'")

    def make_request_from_data(self, data):
        d = data.start_url
        parameter = is_dict(d)
        if isinstance(parameter, str):
            self.logger.warning(f"{TextColor.WARNING}WARNING: String request is deprecated, please use JSON data format. \
                Detail information, please check https://github.com/rmax/scrapy-redis#features{TextColor.ENDC}")
            return FormRequest(d, dont_filter=True)

        if parameter.get('url', None) is None:
            self.logger.warning(f"{TextColor.WARNING}The data from db has no url key in push data{TextColor.ENDC}")
            return []

        url = parameter.pop("url")
        method = parameter.pop("method").upper() if "method" in parameter else "GET"
        metadata = parameter.pop("meta") if "meta" in parameter else {}

        return FormRequest(url, dont_filter=True, method=method, formdata=parameter, meta=metadata)

    def schedule_next_requests(self):
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        if self.db is not None and len(self.db) > 0:
            self.spider_idle_start_time = int(time.time())

        self.schedule_next_requests()

        idle_time = int(time.time()) - self.spider_idle_start_time
        if self.max_idle_time != 0 and idle_time >= self.max_idle_time:
            return
        raise DontCloseSpider


class DBSpider(DBBase, Spider):

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(DBSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_db(crawler)
        return obj


class DBCrawlSpider(DBBase, CrawlSpider):

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        obj = super(DBCrawlSpider, cls).from_crawler(crawler, *args, **kwargs)
        obj.setup_db(crawler)
        return obj
