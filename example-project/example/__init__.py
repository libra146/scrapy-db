import datetime
import logging
import threading
import time
import warnings
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

warnings.filterwarnings("ignore", message="Could not load referrer policy")


def beijing(sec):
    if time.strftime('%z') == "+0800":
        return datetime.datetime.now().timetuple()
    return (datetime.datetime.now() + datetime.timedelta(hours=8)).timetuple()


class SingletonType(type):
    _instance_lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        """
        使用元类实现单例模式

        :param args: 位置参数
        :param kwargs: 关键字参数
        :return: 类的实例
        """
        if not hasattr(cls, '_instance'):
            with SingletonType._instance_lock:
                if not hasattr(cls, '_instance'):
                    cls._instance = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instance


class CustomLogger(object, metaclass=SingletonType):
    """
    日志类

    记录程序日志，单例模式
    """

    def __init__(self, log_path='./logs', backup_count=30, debug_log_name='debug.log',
                 info_log_name='info.log', error_log_name='error.log', all_in_one=None,
                 stream_handler_level=logging.INFO, formatter=None, file_filter=None, stream_filter=None):
        """
       日志类

        可以自定义日志文件的文件名，文件路径，文件名，是否把所有日志打印到一个文件中，默认是否，控制台日志级别，输出格式等

       :param log_path: 日志路径
       :param backup_count: 备份日志文件个数
       :param debug_log_name: debug 级别文件名，如果想关闭此级别，传 ''
       :param info_log_name: info 级别文件名，如果想关闭此级别，传 ''
       :param error_log_name: error 级别文件名，如果想关闭此级别，传 ''
       :param all_in_one: 所有的日志打印到一个文件中，默认为 None，如果需要开启，传需要的日志级别，例如 logging.INFO
       :param stream_handler_level: 控制台日志级别，传 None 可以关闭控制台日志
       :param formatter: 输出格式
       """
        self._logger_root = logging.getLogger()
        self._logger_root.setLevel(logging.DEBUG)
        self._backup_count = backup_count
        self._debug_log_name = debug_log_name
        self._info_log_name = info_log_name
        self._error_log_name = error_log_name
        self._all_in_one = all_in_one
        self._stream_handler_level = stream_handler_level
        self._file_filter = file_filter
        self._stream_filter = stream_filter
        self._all_in_one_log_name = 'all_in_one.log'
        self._log_path = Path(log_path)
        # 定义handler的输出格式
        self._formatter = formatter or logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(threadName)s %(thread)d] [%(name)s] [%(filename)s %(funcName)s %(lineno)d] %(message)s')  # noqa
        self._formatter.converter = beijing
        self.init()

    def init(self):
        if not self._log_path.exists():
            self._log_path.mkdir()
        # 给logger添加handler
        # 如果将_instance删除重新获取这里会重复，所以添加handler之前先清空
        self._logger_root.handlers.clear()
        # 创建一个handler，用于输出到控制台
        if self._stream_handler_level:
            self._logger_root.addHandler(self._init_stream_handler(self._stream_handler_level))
        if self._all_in_one:
            # self._logger_root.handlers.clear()
            self._add_handler(self._init_file_handler(self._all_in_one_log_name, logging.DEBUG))
            self._add_handler(self._init_file_handler(self._all_in_one_log_name, logging.INFO))
            self._add_handler(self._init_file_handler(self._all_in_one_log_name, logging.ERROR))
        else:
            # self._logger_root.handlers.clear()
            self._add_handler(self._init_file_handler(self._error_log_name, logging.ERROR))
            self._add_handler(self._init_file_handler(self._info_log_name, logging.INFO))
            self._add_handler(self._init_file_handler(self._debug_log_name, logging.DEBUG))

    def _add_handler(self, handler):
        if handler:
            self._logger_root.addHandler(handler)

    def _init_stream_handler(self, level):
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_handler.setFormatter(self._formatter)
        if self._stream_filter:
            stream_handler.addFilter(self._stream_filter)
        return stream_handler

    def _init_file_handler(self, file_name, level, formatter=None,
                           backup_count=30, when='midnight', encoding='utf_8'):
        if file_name:
            file_handler = TimedRotatingFileHandler(filename=self._log_path / file_name, backupCount=backup_count,
                                                    when=when, encoding=encoding)
            file_handler.setLevel(level)
            # 只过滤当前等级的日志
            file_handler.addFilter(lambda record: record.levelno == level)
            file_handler.setFormatter(formatter or self._formatter)
            if self._file_filter:
                file_handler.addFilter(self._file_filter)
            return file_handler

    def set_handler(self, handler):
        """
        添加适配器

        :param handler: 需要添加的适配器
        :return: self
        """
        self._logger_root.addHandler(handler)
        return self


CustomLogger(stream_handler_level=logging.DEBUG).init()
