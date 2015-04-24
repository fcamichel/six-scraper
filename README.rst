SIX Scraper CLI
===============

A command line tool to scrape, store and manage stock data from six-swiss-exchange.com.


Installation
------------

Extract somewhere and install dependencies::

    pip install -r requirements.txt


Usage
-----

::

      six-scraper.py list
      six-scraper.py add <symbol-or-isin>...
      six-scraper.py remove <symbol-or-isin>...
      six-scraper.py purge <symbol-or-isin>...
      six-scraper.py update [<symbol-or-isin>...]
      six-scraper.py grab <symbol-or-isin>... (--csv | --json) [options]
      six-scraper.py export <symbol-or-isin>... (--csv | --json) [options]
      six-scraper.py load -f <file> [--csv | --json] [--as <symbol-or-isin>]
      six-scraper.py setup

    Options:
      -h --help      Show this screen.
      --csv          Output CSV.
      --json         Output JSON.
      -a --append    Append data to target file if exists.
      --overwrite    Overwrite target file if exists.
      -f <file>      Use named file, defaults to <symbol>.csv or <symbol>.json.
                     Use "-f -" to write to STDOUT.
      --from=<from>  Start range from this datetime.
      --to=<to>      End range with this datetime.


Testing
-------

::

        pip install -r test_requirements.txt
        py.test


TODO
-----

- simplify and improve CLI syntanx.
- integration tests, e.g. using scripttest.
- Documentation.
