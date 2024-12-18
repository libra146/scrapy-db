import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scrapy-db",
    version="0.0.5",
    description="Similar to [scrapy-redis](https://github.com/rmax/scrapy-redis), using the database as a queue, database-based scrapy components.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="libra146",
    author_email="shumeipai146@gmail.com",
    packages=setuptools.find_packages(include=["scrapy_db"]),
    license="GPL-3.0-only",
    url="https://github.com/libra146/scrapy-db",
    project_urls={
        "Homepage": "https://github.com/libra146/scrapy-db/blob/main/README.md",
        "Repository": "https://github.com/libra146/scrapy-db",
        "Documentation": "https://github.com/libra146/scrapy-db/blob/main/README.md"
    },
    classifiers=[
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    install_requires=[
        "scrapy>=2.7.0",
        "peewee>=3.16.0",
        "pymysql>=1.0.3",
    ],
    extras_require={
        "test": [
            "pytest>=7.3.2",
            "pytest-mock>=3.10.0",
            "codecov>=2.1.13",
            "pytest-cov>=4.1.0",
        ]
    },
)
