from scrapy.utils.request import request_from_dict

from scrapy_db.db import DBModel
from scrapy_db.utils import CustomPickle


class Base(object):
    """
    Basic queue class
    """

    def __init__(self, spider, table_name, key, serializer=None):
        """
        Initialize spider queue object

        :param spider: spider object
        :param table_name: table name
        :param key: current object key
        :param serializer: serialization
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
        Encode request

        :param request: request object
        :return: encode result
        """
        obj = request.to_dict(spider=self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """
        Decode request

        :param encoded_request: request to be decoded
        :return: decoding result
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
