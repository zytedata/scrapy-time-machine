from setuptools import setup

from scrapy_time_machine import __version__

with open("README.md") as f:
    readme = f.read()

setup(
    name="scrapy-time-machine",
    version=__version__,
    license="MIT license",
    description="A downloader middleware that stores the current request chain to be crawled at another time.",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Luiz Francisco Rodrigues da Silva",
    author_email="luizfrdasilva@gmail.com",
    url="https://github.com/zytedata/scrapy-time-machine",
    packages=["scrapy_time_machine"],
    platforms=["Any"],
    keywords="scrapy cache middleware",
    include_package_data=True,
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    install_requires=["Scrapy>=2.0.0"],
)
