#!/usr/bin/env python3
"""SIX Scraper CLI.

Usage:
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

Datetimes could be specified in any of the following formats:

    %d.%m.%Y
    %d.%m.%Y %H:%M
    %d.%m.%Y %H:%M:%S
    %d.%m.%YT%H:%M
    %d.%m.%YT%H:%M:%S

"""

import sys
import os
import os.path
import io
import time
import datetime
import csv
import json
from operator import itemgetter
from contextlib import suppress

from funcy import retry, re_find, chain, pluck, cat, lremove, some, silent, first
from docopt import docopt
import requests
import pymongo


# Business logic abstractions

EPOCH = datetime.datetime(1970, 1, 1)

class MarketData:
    """
    A class encapsulating stock data range.
    """
    def __init__(self, symbol, isin, data):
        self.symbol = symbol
        self.isin = isin
        self.data = data

    @classmethod
    def from_rows(cls, symbol, isin, rows):
        data = [
            (parse_datetime(dt), float(price), int(volume))
            for dt, price, volume in rows
        ]
        return cls(symbol, isin, data)

    def encoded_rows(self, start=EPOCH):
        for dt, price, volume in self.data:
            if dt > start:
                yield str_datetime(dt), price, volume


def parse_datetime(dt_str):
    return datetime.datetime.strptime(dt_str, '%d.%m.%Y %H:%M:%S')

def str_datetime(dt):
    return dt.strftime('%d.%m.%Y %H:%M:%S')


# Grab data from six-swiss-exchange.com

def grab(symbol_or_isin):
    raw_data = _grab_raw(symbol_or_isin)
    return MarketData(*_parse_raw(raw_data))


@retry(2, requests.RequestException)
def _grab_raw(symbol_or_isin):
    url = 'http://www.six-swiss-exchange.com/shares/info_market_data_download.csv'
    response = requests.get(url, params={'id': symbol_or_isin})
    if 'not_found' in response.url:
        _exit("Security %s is not found." % symbol_or_isin)
    return response.text


def _parse_raw(raw_data):
    parsed = _parse_csv(raw_data)

    symbol, isin = re_find(r'\((\w+)\/(\w+)\)', parsed[0][0])
    date_str = parsed[1][0].strip()
    data = list(reversed([
        (parse_datetime(date_str + ' ' + t.strip()), float(price), int(volume))
        for t, price, volume, *_ in parsed[3:-1]
    ]))

    return symbol, isin, data

def _parse_csv(raw_csv):
    f = io.StringIO(raw_csv)
    reader = csv.reader(f, delimiter=';')
    return list(reader)


# Data export/import functions

class BrokenFile(Exception):
    pass


def _write_json(f, data, last_dt=EPOCH, old_ticks=()):
    ticks = list(chain(old_ticks, data.encoded_rows(start=last_dt)))
    json.dump({'symbol': data.symbol, 'isin': data.isin, 'ticks': ticks}, f)

def _read_json(f):
    try:
        raw = json.load(f)
        return MarketData.from_rows(raw['symbol'], raw['isin'], raw['ticks'])
    except (ValueError, KeyError):
        raise BrokenFile

def _peek_json(f):
    try:
        old_data = json.load(f)
        old_ticks = old_data['ticks']
        if old_ticks:
            last_dt = parse_datetime(old_ticks[-1][0])
        else:
            last_dt = EPOCH
    except (KeyError, IndexError, ValueError, TypeError):
        raise BrokenFile

    return last_dt, old_ticks


def _write_csv(f, data, last_dt=EPOCH, old_ticks=()):
    writer = csv.writer(f, delimiter=';')
    writer.writerows(data.encoded_rows(start=last_dt))

def _read_csv(f):
    try:
        reader = csv.reader(f, delimiter=';')
        return MarketData.from_rows(None, None, reader)
    except ValueError:
        raise BrokenFile

def _peek_csv(f):
    try:
        lines = f.readlines()
        if lines:
            last_dt = parse_datetime(lines[-1].split(';')[0])
        else:
            last_dt = EPOCH
    except (IndexError, ValueError):
        raise BrokenFile

    return last_dt, []


def save_data(data, format=None, mode='strict', filename=None):
    assert format in {'csv', 'json'}
    assert mode in {'strict', 'append', 'overwrite'}

    IMPLEMENTATIONS = {
        'json': (_write_json, _peek_json, 'w'),
        'csv': (_write_csv, _peek_csv, 'a')
    }
    write, peek, file_mode = IMPLEMENTATIONS[format]

    if not filename:
        filename = '%s.%s' % (data.symbol, format)

    last_dt = EPOCH
    old_ticks = []
    if filename != '-':
        try:
            with open(filename) as f:
                if mode == 'append':
                    last_dt, old_ticks = peek(f)
                elif mode == 'overwrite':
                    pass
                else:
                    _exit('File %s already exists. Use --append or --overwrite.' % filename)
        except FileNotFoundError:
            pass
        except BrokenFile:
            _exit('File %s format is broken. Remove it or use --overwrite.' % filename)

    if filename != '-':
        with open(filename, file_mode) as f:
            write(f, data, last_dt, old_ticks)
    else:
        write(sys.stdout, data, last_dt, old_ticks)


def load_data(filename, symbol_or_isin=None, format=None):
    READERS = {'json': _read_json, 'csv': _read_csv}

    if not format:
        _, format = filename.rsplit('.', 1)
        if format not in READERS:
            _exit("Don't know how to read *.%s files. "
                  "Try specifying format explicitely with --csv or --json." % format)

    # Reading file
    try:
        with open(filename) as f:
            data = READERS[format](f)
    except FileNotFoundError:
        _exit("File %s not found." % filename)
    except BrokenFile:
        _exit("File %s format is broken." % filename)

    # Guessing symbol/isin, needed for CSV files.
    if not data.symbol:
        # If no clue supplied then use filename
        if not symbol_or_isin:
            basename = os.path.basename(filename)
            guess, _ = os.path.splitext(basename)
        else:
            guess = symbol_or_isin

        stock = find_stock(guess)
        if not stock:
            if symbol_or_isin:
                _exit("Stock %s not registered in database." % symbol_or_isin)
            else:
                _exit("Can't guess stock for %s file. Use --as to specify." % filename)

        data.symbol = stock['symbol']
        data.isin = stock['isin']

    return data


# Database data functions

def _get_db():
    connect = pymongo.MongoClient()
    return connect[os.environ.get('SIX_SCRAPER_DB', 'smi')]


def find_stock(symbol_or_isin):
    return _get_db().stocks.find_one({'$or': [
        {'symbol': symbol_or_isin}, {'isin': symbol_or_isin}
    ]})


def save_data_to_db(data):
    db = _get_db()

    doc = db.ticks.find_one({'symbol': data.symbol}, sort=[('time', -1)])
    start_time = doc['time'] if doc else EPOCH

    rows = [{
        'symbol': data.symbol,
        'isin': data.isin,
        'time': t,
        'price': price,
        'volume': volume,
    } for t, price, volume in data.data if t > start_time]

    if rows:
        db.ticks.insert(rows)


def load_data_from_db(symbol_or_isin, from_=None, to=None):
    # Find stock
    stock = find_stock(symbol_or_isin)
    if stock is None:
        _exit("Stock %s is not found in database." % symbol_or_isin)

    # Load ticks
    range_query = {'symbol': stock['symbol']}
    if from_:
        range_query['time'] = {'$gte': from_}
    if to:
        range_query['time'] = range_query.get('time', {})
        range_query['time']['$lte'] = to
    rows = _get_db().ticks.find(range_query).sort('time', pymongo.ASCENDING)

    # Construct MarketData
    data = map(itemgetter('time', 'price', 'volume'), rows)
    return MarketData(stock['symbol'], stock['isin'], list(data))


# Database commands

def do_list():
    records = list(_get_db().stocks.find())
    for stock in records:
        print("%(symbol)s\t%(isin)s" % stock)
    if not records:
        _warn("Update list is empty")


def do_add(symbol_or_isin):
    db = _get_db()

    stock = db.stocks.find_one({'$or': [
        {'symbol': symbol_or_isin}, {'isin': symbol_or_isin}
    ]})
    if stock:
        _exit("Stock %s is already in update list." % symbol_or_isin)

    # Grab data
    # NOTE: we have 2 resong to grab before saving to database:
    #           - validate symbol/isin
    #           - get corresponding symbol/isin
    data = grab(symbol_or_isin)
    save_data_to_db(data)

    # Add to update list
    db.stocks.insert({'symbol': data.symbol, 'isin': data.isin})
    print("Stock %s added to update list." % symbol_or_isin)


def do_remove(stocks, purge_data=False):
    db = _get_db()
    query = {'$or': [
        {'symbol': {'$in': stocks}},
        {'isin': {'$in': stocks}},
    ]}

    # Query and remove
    records = list(db.stocks.find(query))
    if records:
        db.stocks.remove(query)
        print("Stocks %s removed from update list." % ', '.join(pluck('symbol', records)))

    # Check of all listed stocks were found
    found = set(cat((r['symbol'], r['isin']) for r in records))
    not_found = lremove(found, stocks)
    if not_found:
        _warn("Stocks %s are not on update list." % ', '.join(not_found))

    # Purge data
    if purge_data:
        res = db.ticks.remove(query)
        if res['n']:
            print("Stocks %s data erased." % ', '.join(stocks))
        else:
            _warn("No data for %s to erase." % ', '.join(stocks))


def do_update(stocks):
    if not stocks:
        stocks = [stock['symbol'] for stock in _get_db().stocks.find()]

    _process_stocks(_do_update, stocks)


def _do_update(stock):
    print("Updating %s..." % stock)
    data = grab(stock)
    save_data_to_db(data)


# Other commands

def do_grab(symbol_or_isin, options=None):
    data = grab(symbol_or_isin)
    save_data(data, **options)


def do_export(symbol_or_isin, from_=None, to=None, options=None):
    data = load_data_from_db(symbol_or_isin, from_=from_, to=to)
    save_data(data, **options)


def do_load(symbol_or_isin=None, options=None):
    data = load_data(options['filename'], symbol_or_isin=symbol_or_isin, format=options['format'])
    save_data_to_db(data)


def do_setup():
    db = _get_db()

    db.stocks.ensure_index('symbol')
    db.stocks.ensure_index('isin')
    db.ticks.ensure_index([('symbol', pymongo.ASCENDING), ('time', pymongo.ASCENDING)])


# Main procedure


def main():
    args = docopt(__doc__)
    options = {
        'format': 'csv' if args['--csv'] else
                  'json' if args['--json'] else None,
        'mode': 'append' if args['--append'] else
                'overwrite' if args['--overwrite'] else 'strict',
        'filename': args['-f']
    }
    if args['list']:
        do_list()
    elif args['add']:
        _process_stocks(do_add, args['<symbol-or-isin>'])
    elif args['remove']:
        do_remove(args['<symbol-or-isin>'])
    elif args['purge']:
        do_remove(args['<symbol-or-isin>'], purge_data=True)
    elif args['grab']:
        _process_stocks(do_grab, args['<symbol-or-isin>'], options=options)
    elif args['update']:
        do_update(args['<symbol-or-isin>'])
    elif args['export']:
        from_ = _parse_datetime(args['--from']) if args['--from'] else None
        to = _parse_datetime(args['--to']) if args['--to'] else None
        _process_stocks(do_export, args['<symbol-or-isin>'], from_=from_, to=to, options=options)
    elif args['load']:
        do_load(symbol_or_isin=first(args['<symbol-or-isin>']), options=options)
    elif args['setup']:
        do_setup()


def _process_stocks(action, stocks, *args, **kwargs):
    for stock in stocks:
        # Suppress failed subtask and go to the next one
        with suppress(SystemExit):
            action(stock, *args, **kwargs)


def _parse_datetime(dt_str):
    FORMATS = ['%d.%m.%Y', '%d.%m.%YT%H:%M', '%d.%m.%YT%H:%M:%S',
                           '%d.%m.%Y %H:%M', '%d.%m.%Y %H:%M:%S']
    tries = (silent(datetime.datetime.strptime)(dt_str, f) for f in FORMATS)
    return some(tries) or _exit("Can't parse \"%s\" into datetime." % dt_str)


def _exit(message):
    print(message, file=sys.stderr)
    sys.exit(1)

def _warn(message):
    print(message, file=sys.stderr)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
    except requests.HTTPError as e:
        _exit('HTTP error: %s. Terminating...' % e.reason)
    except (requests.Timeout, requests.ConnectionError) as e:
        _exit('Failed to connect to %s. Terminating...' % e.request.url)
    except pymongo.errors.ConnectionFailure:
        _exit('Problem with database connection. Terminating...')
