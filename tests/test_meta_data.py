import scrapy_db


def test_meta_data():
    assert scrapy_db.__email__
    assert scrapy_db.__author__
    assert scrapy_db.__version__
