from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from example.spiders.demo import DemoSpider

if __name__ == '__main__':
    settings = get_project_settings()
    process = CrawlerProcess(settings, install_root_handler=False)
    process.crawl(DemoSpider)
    process.start()  # the script will block here until all crawling jobs are finished
