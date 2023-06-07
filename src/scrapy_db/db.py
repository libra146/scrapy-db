import copy
import datetime
from abc import ABCMeta, abstractmethod

from peewee import DateTimeField, CharField, BigAutoField, Model, IntegerField, SQL, BooleanField
from playhouse.db_url import connect

from src.scrapy_db.utils import execute_with_timeout


class BaseDB(metaclass=ABCMeta):
    """
    数据库队列基类

    """

    @abstractmethod
    def push(self, **value):
        """
        将元素放到队列内
        
        :param value: 需要放到队列中的元素
        :return: None
        """
        pass

    @abstractmethod
    def pop(self, timeout=0, desc=True):
        """
        弹出队列内的元素，支持先入先出和先入后出

        :param timeout: 超时参数
        :param desc: 先入先出还是先入后出，默认是先入先出
        :return: 队列内的元素
        """
        pass

    @abstractmethod
    def __len__(self):
        """
        判断队列内元素的数量

        :return: 队列内元素的数量
        """
        pass

    @abstractmethod
    def pop_by_score(self, timeout=0):
        """
        根据权重弹出数据

        :param timeout: 超时参数
        :return: 队列内的元素
        """
        pass


# 模型类的默认字段
_attributes = {'id': BigAutoField(primary_key=True),
               'create_time': DateTimeField(default=datetime.datetime.now,
                                            constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')]),
               'update_time': DateTimeField(default=datetime.datetime.now,
                                            constraints=[SQL('ON UPDATE CURRENT_TIMESTAMP')]),
               'Meta': type('Meta', (object,), {'table_name': None, 'database': None})}


def get_model_class_for_db(name, attrs) -> Model:
    """
    根据名字和属性返回模型类

    :param name: 类名前缀
    :param attrs: 模型类的属性
    :return: 模型类
    """
    attrs_ = copy.deepcopy(_attributes)
    attrs_.update(attrs)
    return type(f'{name.capitalize()}Model', (Model,), attrs_)  # noqa


# 一个字典，存储模型类的字段
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
    校验 db 是否存在

    :param fun: 需要执行的函数
    :return: inner 函数
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
        根据设置和名字初始化当前类的实例，依赖注入

        :param settings: 设置
        :param name: 名字
        :param key: 字段字典的 key
        :return: 模型类的实例
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
        if batch_size == 1:
            result = self.db.select().where(self.db.deleted == 0).order_by(
                self.db.id.desc() if desc else self.db.id.asc()).limit(batch_size).first()
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
        return result.key if result else None
