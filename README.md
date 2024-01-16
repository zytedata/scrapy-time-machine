# scrapy-time-machine

![PyPI](https://img.shields.io/pypi/v/scrapy-time-machine)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/scrapy-time-machine)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/zytedata/scrapy-time-machine/Unit%20tests)

Run your spider with a previously crawled request chain.

## Install

    pip install scrapy-time-machine

## Why?

Lets say your spider crawls some page everyday and after some time you notice that an important information was added and you want to start saving it.

You may modify your spider and extract this information from now on, but what if you want the historical value of this data, since it was first introduced to the site?

With this extension you can save a snapshot of the site at every run to be used in the future (as long as you don't change the request chain).

## Enabling

To enable this middlware, add this information to your projects's `settings.py`:

    DOWNLOADER_MIDDLEWARES = {
        "scrapy_time_machine.timemachine.TimeMachineMiddleware": 901
    }

    TIME_MACHINE_ENABLED = True
    TIME_MACHINE_STORAGE = "scrapy_time_machine.storages.DbmTimeMachineStorage"

## Using

### Store a snapshot of the current state of the site

`scrapy crawl sample -s TIME_MACHINE_SNAPSHOT=true -s TIME_MACHINE_URI="/tmp/%(name)s-%(time)s.db"`

This will save a snapshot at `/tmp/sample-YYYY-MM-DDThh-mm-ss.db`


### Retrieve a snapshot from a previously saved state of the site

`scrapy crawl sample -s TIME_MACHINE_RETRIEVE=true -s TIME_MACHINE_URI=/tmp/sample-YYYY-MM-DDThh-mm-ss.db`

If no change was made to the spider between the current version and the version that produced the snapshot, the extracted items should be the same.


## Sample project

There is a sample Scrapy project available at the [examples](examples/project/) directory.
