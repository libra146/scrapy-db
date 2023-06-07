import logging
import time

from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import fingerprint

from . import defaults
from .db import DBModel


class DBDupeFilter(BaseDupeFilter):
    """
    重复过滤器

    """

    logger = logging.getLogger(__name__)

    def __init__(self, table, debug=False):
        """
        初始化

        :param table: 表名
        :param debug: DUPEFILTER_DEBUG，重复值是否打印
        """
        self.table: DBModel = table
        self.debug = debug
        self.log_dupes = True

    @classmethod
    def from_settings(cls, settings):
        """
        通过 settings 创建实例

        :param settings: spider 的 settings
        :return: 当前类的实例
        """
        key = defaults.SCHEDULER_DUPEFILTER_TABLE % {'timestamp': int(time.time())}
        table = DBModel.build_model_from_settings(settings, key, 'dupelifter')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(table=table, debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        """
        通过crawler创建实例

        :param crawler: crawler对象
        :return: 当前类的实例
        """
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        """
        如果已经请求过，返回 True

        :param request: 请求对象
        :return: 是否已经请求过
        """
        # request_fingerprint解除警告
        fp = fingerprint(request).hex()
        added = self.table.db.select().where(self.table.db.key == fp).count()
        self.table.push(**{'key': fp})
        return added != 0

    @classmethod
    def from_spider(cls, spider):
        """
        通过 spider 创建实例

        :param spider: spider对象
        :return: 当前类的实例
        """
        settings = spider.settings
        key = settings.get("SCHEDULER_DUPEFILTER_KEY", defaults.SCHEDULER_DUPEFILTER_TABLE) % {'spider': spider.name}
        table = DBModel.build_model_from_settings(settings, key, 'dupelifter')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(table, debug=debug)

    def close(self, reason=''):
        """
        在关闭时清除数据

        :param reason: 关闭原因
        :return: None
        """
        self.clear()

    def clear(self):
        """
        清除指纹数据

        :return: None
        """
        self.table.drop_table()

    def log(self, request, spider):
        """
        打印请求

        :param request: 请求
        :param spider: spider 对象
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
