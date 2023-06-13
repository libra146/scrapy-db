STATS_TABLE = '%(spider)s_stats'

SCHEDULER_QUEUE_TABLE = '%(spider)s_requests'
SCHEDULER_QUEUE_CLASS = 'scrapy_db.queue.PriorityQueue'

SCHEDULER_DUPEFILTER_TABLE = '%(spider)s_dupefilter'
SCHEDULER_DUPEFILTER_CLASS = 'scrapy_db.dupefilter.DBDupeFilter'

SCHEDULER_PERSIST = True
START_URLS_TABLE = '%(spider)s_start_urls'

# Maximum idle time if the queue is empty
MAX_IDLE_TIME = 0

# Timeout period if the queue is empty during scheduling
SCHEDULER_IDLE_BEFORE_CLOSE = 0
