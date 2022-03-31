# Sample Scrapy Project

## Running the spider

First update your PYTHONPATH to import from `scrapy_time_machine` module:

`export PYTHONPATH=$PYTHONPATH:../../`

### Running the Spider without snapshotting or retrieving

`scrapy crawl random`

This spider should produce an item like this:
`{'number': '92', 'timestamp': 'Timestamp: 2022-03-31 19:50:24 UTC'}`


### Store a snapshot of the current state of the site

`scrapy crawl random -s TIME_MACHINE_SNAPSHOT=true -s TIME_MACHINE_URI="/tmp/%(name)s-%(time)s.db"`

This will save a snapshot at `/tmp/random-YYYY-MM-DDThh-mm-ss.db`

Remember the item extracted in this run, let's say the random number extracted was `42`.


### Retrieve a snapshot from a previously saved state of the site

`scrapy crawl random -s TIME_MACHINE_RETRIEVE=true -s TIME_MACHINE_URI=/tmp/random-YYYY-MM-DDThh-mm-ss.db`

The extracted item extracted using this URI will always be `42`, because the response will be fetch from the snapshot database.
