import time

import pytest

from scrapy_db.db import DBModel, BaseDB


def test_base_db():
    class DB(BaseDB):
        pass

    with pytest.raises(TypeError) as e:
        db = DB()
        db.pop()
        db.push()
        db.pop()
        len(db)
        db.pop_by_score()
    assert "Can't instantiate abstract class" in str(e.value)


def test_db_connection(get_queue_db):
    get_db = get_queue_db
    with pytest.raises(ValueError) as _:
        DBModel.build_model_from_settings({'DB_URL': 'sqlite:///:memory:'}, 'test_dupelifter', 'test')

    assert get_db.db.select().count() == 0

    get_db.db.create(**{'key': '111', 'score': 1})
    get_db.db.create(**{'key': '222', 'score': 1})
    get_db.db.create(**{'key': '333', 'score': 1})
    assert get_db.db.select().count() == 3

    get_db.push(**{'key': '444', 'score': 1})
    assert get_db.db.select().count() == 4

    data = get_db.pop()
    assert data.key == '444'
    assert data.id == 4
    assert len(get_db) == 3

    data = get_db.pop(desc=False)
    assert data.key == '111'
    assert data.id == 1
    assert len(get_db) == 2

    result = get_db.pop(batch_size=1)
    assert len(result) == 1
    assert result[0].key == '333'

    get_db.pop()
    assert len(get_db) == 0
    assert get_db.db.select().limit(1) == []

    t = time.time()
    get_db.pop(timeout=0.3)
    assert 0.4 >= time.time() - t >= 0.3

    get_db.push(**{'key': '555', 'score': 1})
    data = get_db.pop(timeout=2)
    assert data.id == 5
    assert data.key == '555'

    get_db.push(**{'key': 'bbb', 'score': 1})
    result = get_db.fetch_data()
    assert len(result) == 1
    assert result[0].key == 'bbb'

    get_db.push(**{'key': 'ccc', 'score': 1})
    result = get_db.pop_by_score()
    assert result is not None
    assert result.key == 'ccc'
    result = get_db.pop_by_score()
    assert result is None

    assert get_db.drop_table() is None

    with pytest.raises(ValueError) as e:
        get_db.db = None
        get_db.pop()
    assert 'db' in str(e.value)
