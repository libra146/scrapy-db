# scrapy-db

[![codecov](https://codecov.io/github/libra146/scrapy-db/branch/main/graph/badge.svg?token=O9L0DVI0BR)](https://codecov.io/github/libra146/scrapy-db)
[![build](https://github.com/libra146/scrapy-db/actions/workflows/codecov.yaml/badge.svg?branch=main)](https://github.com/libra146/scrapy-db/actions/workflows/codecov.yaml)
![PyPI](https://img.shields.io/pypi/v/scrapy-db)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/scrapy-db)
![PyPI - Downloads](https://img.shields.io/pypi/dm/scrapy-db)

类似于 [scrapy-redis](https://github.com/rmax/scrapy-redis), 使用数据库充当队列，基于数据库的 scrapy。

## 特点

- 分布式爬取/抓取

> 您可以启动共享单个数据库队列的多个蜘蛛实例。最适合广泛的多域爬网。

- 分布式后处理

  > 抓取的项目被推送到数据库队列中，这意味着您可以启动任意数量的后处理进程
  共享项目队列。

- Scrapy 即插即用组件

  > 调度程序+重复过滤器和Spider。

## 依赖

- Python 3.7+
- `peewee` >= 3.16.0
- `Scrapy` >= 2.7.0
- `pymysql` >= 1.0.3

## 安装

使用 pip 安装

```bash
pip install scrapy-db
```

从 Github 安装

```bash
git clone https://github.com/libra146/scrapy-db.git
cd scrapy-db
python setup.py install
```

使用 poetry 安装

```bash
poetry add scrapy-db
```

如果你正在进行分布式爬取任务，scraper-db是一个非常实用的爬虫组件，可以帮助你更高效地完成任务。

## 使用

克隆当前项目，运行example-project中的示例爬虫来体验一下。

## ❗️注意

该存储库仍在开发中，可能不稳定。

## 为什么会有这个

因为我有一个巨大的请求池，它使用了超过 32G 的内存，我没有那么多内存让 redis 来保存它，所以，我想到了数据库，我
参考 scrapy-redis 创建了它并且工作正常。
