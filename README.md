# scrapy-db

Similar to [scrapy-redis](https://github.com/rmax/scrapy-redis), using the database as a queue, database-based scrapy
components.

## Features

- Distributed crawling/scraping

  > You can start multiple spider instances that share a single db queue. Best suitable for broad multi-domain crawls.

- Distributed post-processing

  > Scraped items gets pushed into a redis queued meaning that you can start as many as needed post-processing processes
  sharing the items queue.

- Scrapy plug-and-play components

  > Scheduler + Duplication Filter, Base Spiders.

## Requirements

- Python 3.7+
- MySQL= 5.0
- `Scrapy` >= 2.0
- `pymysql` >= 4.0

## Installation

From pip

```bash
pip install scrapy-db
```

From GitHub

```bash
git clone https://github.com/libra146/scrapy-db.git
cd scrapy-db
python setup.py install
```
