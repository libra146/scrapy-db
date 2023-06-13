import datetime
import time
from unittest import mock

import pytest
from peewee import SqliteDatabase, BigAutoField, DateTimeField

from scrapy_db.db import DBModel

# The default fields for model classes
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now),
               'update_time': DateTimeField(default=datetime.datetime.now),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


@pytest.fixture
@mock.patch('scrapy_db.db._attributes', _attributes)
def database():
    db = SqliteDatabase(':memory:')
    with db.atomic():
        yield db


@mock.patch('scrapy_db.db._attributes', _attributes)
def test_db_connection(database):
    with pytest.raises(ValueError) as _:
        DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'test')

    model = DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'queue')
    assert model.db.select().count() == 0

    model.db.create(**{'key': '111', 'score': 1})
    model.db.create(**{'key': '222', 'score': 1})
    model.db.create(**{'key': '333', 'score': 1})
    assert model.db.select().count() == 3

    model.push(**{'key': '444', 'score': 1})
    assert model.db.select().count() == 4

    data = model.pop()
    assert data.key == '444'
    assert data.id == 4
    assert len(model) == 3

    data = model.pop(desc=False)
    assert data.key == '111'
    assert data.id == 1
    assert len(model) == 2

    model.pop()
    model.pop()
    assert len(model) == 0
    assert model.db.select().limit(1) == []

    t = time.time()
    model.pop(timeout=0.3)
    assert 0.4 >= time.time() - t >= 0.3

    model.push(**{'key': '555', 'score': 1})
    data = model.pop(timeout=2)
    # TODO why == 1 ?
    assert data.id == 5
    assert data.key == '555'
