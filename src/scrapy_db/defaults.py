STATS_TABLE = '%(spider)s_stats'

SCHEDULER_QUEUE_TABLE = '%(spider)s_requests'
SCHEDULER_QUEUE_CLASS = 'scrapy_db.queue.PriorityQueue'

SCHEDULER_DUPEFILTER_TABLE = '%(spider)s_dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_db.dupefilter.DBDupeFilter'

SCHEDULER_PERSIST = True
START_URLS_TABLE = '%(spider)s_start_urls'

# 最大空闲时间，如果队列为空的话
MAX_IDLE_TIME = 0

# 调度时如果队列为空的超时时间
SCHEDULER_IDLE_BEFORE_CLOSE = 0
