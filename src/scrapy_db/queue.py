from src.scrapy_db.db import DBModel
from src.scrapy_db.utils import CustomPickle

try:
    from scrapy.utils.request import request_from_dict
except ImportError:
    from scrapy.utils.reqser import request_to_dict, request_from_dict


class Base(object):
    """
    基础队列类

    """

    def __init__(self, spider, table_name, key, serializer=None):
        """
        初始化爬虫队列对象

        :param spider: spider 对象
        :param table_name: 表名
        :param key: 当前对象 key
        :param serializer: 序列化
        """
        if serializer is None:
            serializer = CustomPickle
        if not hasattr(serializer, 'loads'):
            raise TypeError(f"serializer does not implement 'loads' function: {serializer}")
        if not hasattr(serializer, 'dumps'):
            raise TypeError(f"serializer does not implement 'dumps' function: {serializer}")
        self.db = DBModel.build_model_from_settings(spider.settings, table_name % {'spider': spider.name}, key)
        self.spider = spider
        self.serializer = serializer

    def _encode_request(self, request):
        """
        编码请求

        :param request: 请求对象
        :return: 编码结果
        """
        try:
            obj = request.to_dict(spider=self.spider)
        except AttributeError:
            obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """
        解码请求

        :param encoded_request: 需要解码的请求
        :return: 解码结果
        """
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, spider=self.spider)

    def __len__(self):
        return len(self.db)

    def push(self, request):
        self.db.push(**{'key': self._encode_request(request)})

    def pop(self, timeout=0):
        raise NotImplementedError

    def clear(self):
        self.db.drop_table()


class FifoQueue(Base):

    def pop(self, timeout=0):
        result = self.db.pop(timeout, desc=False)
        if result:
            return self._decode_request(result)


class PriorityQueue(Base):

    def push(self, request):
        data = self._encode_request(request)
        score = -request.priority
        self.db.push(**{'key': data, 'score': score})

    def pop(self, timeout=0):
        result = self.db.pop_by_score(timeout)
        if result:
            return self._decode_request(result)


class LifoQueue(Base):

    def pop(self, timeout=0):
        result = self.db.pop(timeout)
        if result:
            return self._decode_request(result)
