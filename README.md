# scrapy-db

[![codecov](https://codecov.io/github/libra146/scrapy-db/branch/main/graph/badge.svg?token=O9L0DVI0BR)](https://codecov.io/github/libra146/scrapy-db) [![build](https://github.com/libra146/scrapy-db/actions/workflows/codecov.yaml/badge.svg?branch=main)](https://github.com/libra146/scrapy-db/actions/workflows/codecov.yaml)

Similar to [scrapy-redis](https://github.com/rmax/scrapy-redis), using the database as a queue, database-based scrapy
components.

## Features

- Distributed crawling/scraping

  > You can start multiple spider instances that share a single db queue. Best suitable for broad multi-domain crawls.

- Distributed post-processing

  > Scraped items gets pushed into a DB queued meaning that you can start as many as needed post-processing processes
  sharing the items queue.

- Scrapy plug-and-play components

  > Scheduler + Duplication Filter, Base Spiders.

## Requirements

- Python 3.7+
- `peewee` >= 3.16.0
- `Scrapy` >= 2.7.0
- `pymysql` >= 1.0.3

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

From poetry

```bash
poetry add scrapy-db
```

If you are conducting distributed crawling tasks, scraper db is a very practical scraper component that can help you
complete tasks more efficiently.

## Use

Clone the current project and run the example crawler in example-project to experience it.

## ❗️Notice

This repository is still under development and may be unstable.
