# Sample Scrapy Project

## Running the spider

First update your PYTHONPATH to import from `scrapy_time_machine` module:

`export PYTHONPATH=$PYTHONPATH:../../`

### Running the Spider without snapshotting or retrieving

`scrapy crawl random`


### Store a snapshot of the current state of the site

`scrapy crawl random -s TIME_MACHINE_SNAPSHOT=true`


### Retrieve a snapshot from a previously saved state of the site

`scrapy crawl random -s TIME_MACHINE_RETRIEVE=true`
