import datetime
from unittest import mock

import pytest
from peewee import BigAutoField, DateTimeField

from scrapy_db.db import DBModel

# The default fields for model classes
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now),
               'update_time': DateTimeField(default=datetime.datetime.now),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


@pytest.fixture(scope='session')
@mock.patch('scrapy_db.db._attributes', _attributes)
def get_queue_db():
    db = DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_queue', 'queue')
    return db


@pytest.fixture(scope='session')
@mock.patch('scrapy_db.db._attributes', _attributes)
def get_dupelifter_db():
    db = DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'dupelifter')
    return db
