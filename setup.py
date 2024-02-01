from setuptools import setup

with open("README.md") as f:
    readme = f.read()

setup(
    name="scrapy-time-machine",
    version="1.1.1",
    license="MIT license",
    description="A downloader middleware that stores the current request chain to be crawled at another time.",  # noqa
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    install_requires=["Scrapy>=2.0.0", "boto3"],
)
