import copy
import datetime
from abc import ABCMeta, abstractmethod

from peewee import DateTimeField, CharField, BigAutoField, Model, IntegerField, SQL, BooleanField
from playhouse.db_url import connect

from scrapy_db.utils import execute_with_timeout


class BaseDB(metaclass=ABCMeta):
    """
    Base class for database queues
    """

    @abstractmethod
    def push(self, **value):
        """
        Add an element to the queue

        :param value: The element to add to the queue
        :return: None
        """
        pass

    @abstractmethod
    def pop(self, timeout=0, desc=True):
        """
        Remove an element from the queue, support FIFO and LIFO

        :param timeout: Timeout parameter
        :param desc: FIFO or LIFO, default is FIFO
        :return: The element removed from the queue
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        Get the number of elements in the queue

        :return: The number of elements in the queue
        """
        pass

    @abstractmethod
    def pop_by_score(self, timeout=0):
        """
        Remove an element from the queue by score

        :param timeout: Timeout parameter
        :return: The element removed from the queue
        """
        pass


# The default fields for model classes
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now,
                                            constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')]),
               'update_time': DateTimeField(default=datetime.datetime.now,
                                            constraints=[SQL('ON UPDATE CURRENT_TIMESTAMP')]),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


def get_model_class_for_db(name, attrs) -> Model:
    """
    Return a model class based on name and attributes

    :param name: The prefix of the class name
    :param attrs: The attributes of the model class
    :return: The model class
    """
    attrs_ = copy.deepcopy(_attributes)
    attrs_.update(attrs)
    return type(f'{name.capitalize()}Model', (Model,), attrs_)  # noqa


# A dictionary that stores the fields of the model class
_params = {
    'dupelifter': {
        'key': CharField(),
    },
    'start_url': {
        'start_url': CharField(),
        'deleted': BooleanField(default=False)
    },
    'queue': {
        'key': CharField(max_length=10000),
        'score': IntegerField(index=True),
        'deleted': BooleanField(default=False)
    }
}


def db_require(fun):
    """
    Check if db exists

    :param fun: The function to be executed
    :return: The inner function
    """

    def inner(self, *args, **kwargs):
        if self.db is None:
            raise ValueError('db is None!')
        return fun(self, *args, **kwargs)

    return inner


class DBModel(BaseDB):
    def __init__(self, db=None):
        self.db: Model = db

    @classmethod
    def build_model_from_settings(cls, settings, name, key):
        """
        Initialize an instance of the current class based on the settings and name

        :param settings: The settings
        :param name: The name
        :param key: The key in the field dictionary
        :return: An instance of the model class
        """
        attrs = _params.get(key)
        if not attrs:
            raise ValueError(f'key {key} not found in params!')
        model = get_model_class_for_db(name, attrs)
        url = settings.get('DB_URL')
        assert url is not None
        model._meta.database = connect(url)  # noqa
        model._meta.table_name = name  # noqa
        model.create_table()
        return cls(model)

    @db_require
    def push(self, **value):
        self.db.create(**value)

    @db_require
    def drop_table(self):
        self.db.drop_table()

    @execute_with_timeout
    @db_require
    def pop(self, timeout=0, desc=True, batch_size=None):
        if not batch_size:
            result = self.db.select().where(self.db.deleted == 0).order_by(
                self.db.id.desc() if desc else self.db.id.asc()).limit(1).first()
            if result:
                self._delete(result)
            return result
        else:
            result = self.db.select().where(self.db.deleted == 0).order_by(
                self.db.id.desc() if desc else self.db.id.asc()).limit(batch_size)
            if result.count() > 0:
                self._delete(*copy.deepcopy(list(result)))
            return result

    def _delete(self, *results):
        if results:
            self.db.update(deleted=1).where(self.db.id.in_([a.id for a in results])).execute()

    @db_require
    def fetch_data(self, batch_size=1):
        result = self.db.select().where(self.db.deleted == 0).order_by(self.db.id.desc()).limit(batch_size)
        if result.count() > 0:
            self._delete(*copy.deepcopy(list(result)))
        return list(result)

    @db_require
    def __len__(self):
        return self.db.select().where(self.db.deleted == 0).count()

    @execute_with_timeout
    @db_require
    def pop_by_score(self, timeout=0):
        result = self.db.select().where(self.db.deleted == 0).order_by(self.db.score.desc()).limit(1).first()
        if result:
            self._delete(result)
        return result if result else None
