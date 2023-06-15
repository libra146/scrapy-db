import logging
import time

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import fingerprint

from . import defaults
from .db import DBModel


class DBDupeFilter(BaseDupeFilter):
    """
    Duplicate filter
    """

    logger = logging.getLogger(__name__)

    def __init__(self, table, debug=False):
        """
        Initialize

        :param table: Table name
        :param debug: DUPEFILTER_DEBUG, whether to print duplicate values
        """
        self.table: DBModel = table
        self.debug = debug
        self.log_dupes = True

    @classmethod
    def from_settings(cls, settings):
        """
        Create an instance through settings

        :param settings: Spider settings
        :return: Instance of the current class
        """
        key = defaults.SCHEDULER_DUPEFILTER_TABLE % {'spider': int(time.time())}
        table = DBModel.build_model_from_settings(settings, key, 'dupelifter')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(table=table, debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Create an instance through crawler

        :param crawler: Crawler object
        :return: Instance of the current class
        """
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        """
        Returns True if the request has already been made

        :param request: Request object
        :return: Whether the request has been made
        """
        # request_fingerprint remove warnings
        fp = fingerprint(request).hex()
        added = self.table.db.select().where(self.table.db.key == fp).count()
        self.table.push(**{'key': fp})
        return added != 0

    @classmethod
    def from_spider(cls, spider):
        """
        Create an instance through spider

        :param spider: Spider object
        :return: Instance of the current class
        """
        settings = spider.settings
        key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_TABLE) % {'spider': spider.name}
        table = DBModel.build_model_from_settings(settings, key, 'dupelifter')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(table, debug=debug)

    def close(self, reason=''):
        """
        Clear data during shutdown

        :param reason: Shutdown reason
        :return: None
        """
        self.clear()

    def clear(self):
        """
        Clear fingerprint data

        :return: None
        """
        self.table.drop_table()

    def log(self, request, spider):
        """
        Prints the request

        :param request: Request
        :param spider: Spider object
        :return: None
        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.log_dupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.log_dupes = False
