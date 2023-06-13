import importlib

from scrapy.utils.misc import load_object

from . import defaults


class Scheduler(object):
    """
    Scheduler

    """

    def __init__(self,
                 persist=False,
                 flush_on_start=False,
                 queue_table=defaults.SCHEDULER_QUEUE_TABLE,
                 queue_cls=defaults.SCHEDULER_QUEUE_CLASS,
                 dupefilter_table=defaults.SCHEDULER_DUPEFILTER_TABLE,
                 dupefilter_cls=defaults.SCHEDULER_DUPEFILTER_CLASS,
                 idle_before_close=0,
                 serializer=None):
        """
        Initialize scheduler.

        :param persist: whether to restore previous progress
        :param flush_on_start: whether to clear progress when starting the crawler
        :param queue_table: name of the queue table
        :param queue_cls: class of the queue
        :param dupefilter_table: name of the duplicate filter table
        :param dupefilter_cls: class of the duplicate filter
        :param idle_before_close: maximum timeout when idle
        :param serializer: serialization tool
        """
        self.df = None
        self.queue = None
        self.spider = None
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.persist = persist
        self.flush_on_start = flush_on_start
        self.queue_table = queue_table
        self.queue_cls = queue_cls
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_table = dupefilter_table
        self.idle_before_close = idle_before_close
        self.serializer = serializer
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'persist': settings.getbool('SCHEDULER_PERSIST'),
            'flush_on_start': settings.getbool('SCHEDULER_FLUSH_ON_START'),
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }

        optional = {
            'queue_table': 'SCHEDULER_QUEUE_TABLE',
            'queue_cls': 'SCHEDULER_QUEUE_CLASS',
            'dupefilter_table': 'SCHEDULER_DUPEFILTER_TABLE',
            'dupefilter_cls': 'SCHEDULER_DUPEFILTER_CLASS',
            'serializer': 'SCHEDULER_SERIALIZER',
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        if isinstance(kwargs.get('serializer'), str):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        return cls(**kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Inject crawler.

        :param crawler: crawler
        :return: scheduler object
        """
        instance = cls.from_settings(crawler.settings)
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        try:
            self.queue = load_object(self.queue_cls)(
                spider=spider,
                table_name=self.queue_table % {'spider': spider.name},
                key='queue',
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError(f"Failed to instantiate queue class '{self.queue_cls}': {e}")

        self.df = load_object(self.dupefilter_cls).from_spider(spider)

        if self.flush_on_start:
            self.flush()
        if len(self.queue):
            spider.log(f"Resuming crawl ({len(self.queue)} requests scheduled)")

    def close(self, reason):
        if not self.persist:
            self.flush()

    def flush(self):
        self.df.clear()
        self.queue.clear()

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/db', spider=self.spider)
        self.queue.push(request)
        return True

    def next_request(self):
        block_pop_timeout = self.idle_before_close
        request = self.queue.pop(block_pop_timeout)
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/db', spider=self.spider)
        return request

    def has_pending_requests(self):
        return len(self) > 0
