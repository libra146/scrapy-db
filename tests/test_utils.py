import time

from scrapy_db.utils import execute_with_timeout, CustomPickle


def test_execute_with_timeout(mocker):
    @execute_with_timeout
    def func_without_timeout():
        return 'value'

    assert func_without_timeout() == 'value'

    @execute_with_timeout
    def func(timeout: float = 0):
        time.sleep(timeout)
        return timeout

    assert func() == 0
    assert func(timeout=0.1) == 0.1
    assert 0.1 >= func(timeout=0.1) >= 0

    class Test(object):
        @execute_with_timeout
        def func(self, timeout: float = 0):
            time.sleep(0.1)
            self.call_count()

        @classmethod
        def call_count(cls):
            pass

    t = time.time()
    spy = mocker.spy(Test, 'call_count')
    assert Test().func(timeout=0.5) is None
    assert spy.call_count == 5
    assert 0.6 > time.time() - t >= 0.5


def test_custom_json():
    a = {'test': 'value', 'test_bytes': b'test_bytes'}
    result = CustomPickle.dumps(a)
    assert isinstance(result, str)
    assert result == 'gASVLgAAAAAAAAB9lCiMBHRlc3SUjAV2YWx1ZZSMCnRlc3RfYnl0ZXOUQwp0ZXN0X2J5dGVzlHUu'

    r = CustomPickle.loads(result)
    assert isinstance(r, dict)
    assert r['test'] == 'value'
    assert isinstance(r['test_bytes'], bytes)
    # assert isinstance(result, dict)
    # assert result == a
    # assert a['test_bytes'] == a['test_bytes']
