from unittest import mock

import pytest
from scrapy import Request

from scrapy_db.queue import Base, FifoQueue, PriorityQueue, LifoQueue


@mock.patch('scrapy_db.queue.DBModel')
@mock.patch('scrapy_db.queue.len')
@pytest.mark.parametrize('q', [
    Base,
    FifoQueue,
    PriorityQueue,
    LifoQueue,
])
def test_fifo_queue(db, len_, q):
    spider = mock.Mock()
    spider.name = 'test'
    spider.settings = mock.Mock()
    queue = q(spider, 'test', 'queue')
    queue.db = db
    queue._encode_request = mock.Mock(wraps=queue._encode_request)
    queue._decode_request = mock.Mock(wraps=queue._decode_request)

    # test clear
    queue.clear()
    assert queue.db.drop_table.called

    # test push
    r = Request(url='https://www.baidu.com', meta={'key1': 'key1'})
    queue.push(r)
    assert queue.db.push.called
    assert queue._encode_request.called
    if isinstance(queue, PriorityQueue):
        queue.db.push.assert_called_with(key_=queue._encode_request(r), score=0)
    else:
        queue.db.push.assert_called_with(key_=queue._encode_request(r))

    # test loads and dumps
    assert queue.spider.name == 'test'
    assert hasattr(queue.serializer, 'dumps')
    assert hasattr(queue.serializer, 'loads')
    with pytest.raises(TypeError) as _:
        q(spider, 'test', 'queue', object)

    with pytest.raises(TypeError) as _:
        serializer = type('test', (), {'loads': lambda: None})
        q(spider, 'test', 'queue', serializer)

    # test encode and decode
    queue = q(spider, 'test', 'queue')
    request = Request(url='https://www.baidu.com', meta={'key1': 'key1'})
    encode_request = queue._encode_request(request)
    assert isinstance(encode_request, str)
    decode_request = queue._decode_request(encode_request)
    assert isinstance(decode_request, Request)
    assert decode_request.meta['key1'] == 'key1'
    assert decode_request.url == 'https://www.baidu.com'

    # test len
    queue.push(request)
    len_.return_value = 1
    assert len(queue) == 1

    # test pop
    if type(queue) == Base:
        with pytest.raises(NotImplementedError) as _:
            queue.pop()
    else:
        r = mock.Mock()
        r.key_ = encode_request
        queue.db.pop_by_score.return_value = r
        queue.db.pop.return_value = r
        result = queue.pop()
        assert result.url == decode_request.url
        assert result.meta == decode_request.meta
