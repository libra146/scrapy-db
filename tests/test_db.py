import time

import pytest
from peewee import SqliteDatabase

from src.scrapy_db.db import DBModel


@pytest.fixture
def database():
    db = SqliteDatabase(':memory:')
    with db.atomic():
        yield db


def test_db_connection(database):
    with pytest.raises(ValueError) as _:
        DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'test')

    model = DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'dupelifter')
    assert model.db.select().count() == 0

    model.db.create(**{'key': '111'})
    model.db.create(**{'key': '222'})
    model.db.create(**{'key': '333'})
    assert model.db.select().count() == 3

    model.push(**{'key': '444'})
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

    model.push(**{'key': '555'})
    data = model.pop(timeout=2)
    # TODO why == 1 ?
    assert data.id == 1
    assert data.key == '555'
