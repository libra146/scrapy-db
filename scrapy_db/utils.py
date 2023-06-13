import base64
import pickle
import time
from ast import literal_eval


class TextColor:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def is_dict(content):
    try:
        return literal_eval(content)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        return content


class CustomPickle(object):
    @staticmethod
    def loads(data):
        return pickle.loads(base64.b64decode(data))

    @staticmethod
    def dumps(*args, **kwargs):
        return base64.b64encode(pickle.dumps(*args, **kwargs)).decode()


def execute_with_timeout(func):
    def inner(*args, **kwargs):
        timeout_ = kwargs.get('timeout', 0)

        if timeout_ == 0:
            return func(*args, **kwargs)
        t = time.time()
        while True:
            if time.time() - t >= timeout_:
                break
            if r := func(*args, **kwargs):
                return r

    return inner
