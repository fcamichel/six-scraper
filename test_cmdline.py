import os
import re
import copy
import datetime
import json

import pytest
from scripttest import TestFileEnvironment
import pymongo


# Constants, fixtures and utilities

COMMAND = '../six-scraper.py'
DB_NAME = 'test_smi'
STOCKS = [
    {'isin': 'CH0012221716', 'symbol': 'ABBN'},
    {'isin': 'CH0010532478', 'symbol': 'ATLN'},
]
TICKS = [
    {'symbol': 'ABBN', 'isin': 'CH0012221716',
     'time': datetime.datetime(2014, 7, 29, 15, 23, 3), 'price': 21.52, 'volume': 5738},
    {'symbol': 'ABBN', 'isin': 'CH0012221716',
     'time': datetime.datetime(2014, 7, 29, 15, 24, 35), 'price': 21.6, 'volume': 9010},
    {'symbol': 'ABBN', 'isin': 'CH0012221716',
     'time': datetime.datetime(2014, 7, 30, 1, 10, 0), 'price': 22.1, 'volume': 2305},

    {'symbol': 'ATLN', 'isin': 'CH0010532478',
     'time': datetime.datetime(2014, 7, 29, 15, 23, 12), 'price': 10, 'volume': 12040},
    {'symbol': 'ATLN', 'isin': 'CH0010532478',
     'time': datetime.datetime(2014, 7, 29, 15, 24, 5), 'price': 9.2, 'volume': 7876},
]


@pytest.fixture
def env():
    environ = os.environ.copy()
    environ['SIX_SCRAPER_DB'] = DB_NAME
    return TestFileEnvironment(environ=environ)

@pytest.yield_fixture
def db():
    conn = pymongo.Connection()
    conn.drop_database(DB_NAME) # start afresh
    yield conn[DB_NAME]
    conn.drop_database(DB_NAME)

@pytest.fixture
def stocks(db):
    db.stocks.insert(copy.deepcopy(STOCKS))

@pytest.fixture
def ticks(db):
    db.ticks.insert(copy.deepcopy(TICKS))


def _list_stocks(db):
    return list(db.stocks.find(fields={'_id': False}))

def _list_ticks(db):
    return list(db.ticks.find(fields={'_id': False}))

def _json_res(result, symbol):
    filename = symbol + '.json'
    if filename in result.files_updated:
        return json.loads(result.files_updated[filename].bytes)
    else:
        assert filename in result.files_created
        return json.loads(result.files_created[filename].bytes)


# Tests

def test_add(env, db):
    # Simple add
    env.run(COMMAND, 'add', 'ABBN')
    assert _list_stocks(db) == STOCKS[:1]

    # Multiple add with errors
    result = env.run(COMMAND, 'add', 'ABBN', 'XXXX', 'CH0010532478', expect_stderr=True)
    assert re.search(r'ABBN.*in update list', result.stderr)
    assert re.search(r'XXX.*not found', result.stderr)
    assert r'CH0010532478 added' in result.stdout
    assert _list_stocks(db) == STOCKS


def test_list(env, stocks):
    result = env.run(COMMAND, 'list')
    assert re.match(r'ABBN\s+CH0012221716\nATLN\s+CH0010532478', result.stdout)


def test_remove(env, db, stocks, ticks):
    env.run(COMMAND, 'remove', 'ABBN')
    assert _list_stocks(db) == STOCKS[1:]
    assert _list_ticks(db) == TICKS


def test_purge(env, db, stocks, ticks):
    env.run(COMMAND, 'purge', 'ABBN')
    assert _list_stocks(db) == STOCKS[1:]
    assert _list_ticks(db) == [t for t in TICKS if t['symbol'] != 'ABBN']

def test_update(env, db, stocks):
    env.run(COMMAND, 'update')


def test_grab(env):
    result = env.run(COMMAND, 'grab', 'ABBN', '--json')
    data = _json_res(result, 'ABBN')
    assert data['symbol'] == 'ABBN'
    assert isinstance(data['ticks'], list)


def test_export(env, stocks, ticks):
    result = env.run(COMMAND, 'export', 'ABBN', '--json')
    data = _json_res(result, 'ABBN')
    assert data['symbol'] == 'ABBN'
    assert len(data['ticks']) == 3

    result = env.run(COMMAND, 'export', 'ABBN', '--csv')
    assert 'ABBN.csv' in result.files_created
    assert len(result.files_created['ABBN.csv'].bytes.splitlines()) == 3


def test_write_modes(env, stocks, ticks):
    env.run(COMMAND, 'export', 'ABBN', '--json')

    # strict
    result = env.run(COMMAND, 'export', 'ABBN', '--json', expect_stderr=True)
    assert 'ABBN.json already exists' in result.stderr

    # overwrite
    result = env.run(COMMAND, 'export', 'ABBN', '--json', '--overwrite', '--to=30.07.2014')
    assert 'ABBN.json' in result.files_updated
    data = _json_res(result, 'ABBN')
    assert len(data['ticks']) == 2

    # append
    result = env.run(COMMAND, 'export', 'ABBN', '--json', '-a')
    data = _json_res(result, 'ABBN')
    assert len(data['ticks']) == 3


def test_load_json(env, db):
    env.writefile('ABBN.json', json.dumps({
        'symbol': 'ABBN',
        'isin': 'CH0012221716',
        'ticks': [['30.07.2014 15:05:20', 5.5, 1230]]
    }).encode('utf-8'))
    env.run(COMMAND, 'load', '-f', 'ABBN.json')
    assert _list_ticks(db) == [{'isin': 'CH0012221716', 'symbol': 'ABBN',
        'time': datetime.datetime(2014, 7, 30, 15, 5, 20), 'price': 5.5, 'volume': 1230}]


def test_load_csv(env, db, stocks):
    env.writefile('some.csv', b'30.07.2014 15:05:20;5.5;1230\n')
    env.run(COMMAND, 'load', '-f', 'some.csv', '--as', 'ATLN')
    assert _list_ticks(db) == [{'isin': 'CH0010532478', 'symbol': 'ATLN',
        'time': datetime.datetime(2014, 7, 30, 15, 5, 20), 'price': 5.5, 'volume': 1230}]
