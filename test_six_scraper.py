import os
import io
import datetime
import json

import pytest
import mongomock
import pymongo


# NOTE: Can't do normal import since name contains hyphen.
script = __import__('six-scraper')


# Test data, utilities and fixtures

RAW_DATA = """ABB LTD N (ABBN/CH0012221716)
          29.07.2014;
          Time;Price;Volume;
          15:24:35;21.6;9010;
          15:23:03;21.52;5738;

"""
DATA = script.MarketData('ABBN', 'CH0012221716', [
    (datetime.datetime(2014, 7, 29, 15, 23, 3), 21.52, 5738),
    (datetime.datetime(2014, 7, 29, 15, 24, 35), 21.6, 9010),
])


class capture:
    """An utility context manager to """
    def __enter__(self):
        self.file = io.StringIO()
        return self

    def __exit__(self, exctype, excinst, exctb):
        self.file.seek(0)
        self.out = self.file.read()


@pytest.yield_fixture()
def mock_db():
    db = mongomock.Connection().smi

    real = script._get_db
    script._get_db = lambda: db
    yield db
    script._get_db = real


# Parse/grab tests

def test_parse():
    parsed = script._parse_raw(RAW_DATA)
    data = script.MarketData(*parsed)

    assert data.symbol == DATA.symbol
    assert data.isin == DATA.isin
    assert data.data == DATA.data


# JSON/CSV tests

def test_write_json():
    with capture() as c:
        script._write_json(c.file, DATA)

    assert json.loads(c.out) == {
        'symbol': 'ABBN',
        'isin': 'CH0012221716',
        'ticks': [["29.07.2014 15:23:03", 21.52, 5738], ["29.07.2014 15:24:35", 21.6, 9010]]
    }

def test_peek_json():
    with pytest.raises(script.BrokenFile):
        script._peek_json(io.StringIO(''))

    assert script._peek_json(io.StringIO('{"ticks": []}')) == (script.EPOCH, [])

    with pytest.raises(script.BrokenFile):
        WRONG_DATE = '''{"ticks": [["2014-07-29 15:24:35", 21.6, 9010]}'''
        script._peek_json(io.StringIO(WRONG_DATE))

    JSON = '''{"ticks": [["29.07.2014 15:24:35", 21.6, 9010],
                        ["29.07.2014 15:23:03", 21.52, 5738]],
               "isin": "CH0012221716", "symbol": "ABBN"}'''
    last_dt, ticks = script._peek_json(io.StringIO(JSON))
    assert last_dt == datetime.datetime(2014, 7, 29, 15, 23, 3)
    assert len(ticks) == 2


def test_write_csv():
    with capture() as c:
        script._write_csv(c.file, DATA)

    assert c.out.splitlines() == [
        '29.07.2014 15:23:03;21.52;5738',
        '29.07.2014 15:24:35;21.6;9010',
    ]

def test_peek_csv():
    assert script._peek_csv(io.StringIO('')) == (script.EPOCH, [])

    # Wrong date format
    with pytest.raises(script.BrokenFile):
        script._peek_csv(io.StringIO('2014-07-29 15:24:35;21.6;9010\n'))

    CSV = '29.07.2014 15:23:03;21.6;9010\n' \
        + '29.07.2014 15:24:35;21.52;5738\n'
    last_dt, _ = script._peek_csv(io.StringIO(CSV))
    assert last_dt == datetime.datetime(2014, 7, 29, 15, 24, 35)


# Database tests

def test_db_save(mock_db):
    script.save_data_to_db(DATA)
    assert list(mock_db.ticks.find(fields={'_id': False}, sort=[('time', pymongo.ASCENDING)])) == [
        {'price': 21.52, 'isin': 'CH0012221716', 'symbol': 'ABBN', 'volume': 5738,
         'time': datetime.datetime(2014, 7, 29, 15, 23, 3)},
        {'price': 21.6, 'isin': 'CH0012221716', 'symbol': 'ABBN', 'volume': 9010,
        'time': datetime.datetime(2014, 7, 29, 15, 24, 35)}
    ]

def test_db_load(mock_db):
    mock_db.stocks.insert({'symbol': DATA.symbol, 'isin': DATA.isin})
    script.save_data_to_db(DATA)

    data = script.load_data_from_db(DATA.symbol)
    assert data.symbol == DATA.symbol
    assert data.isin == DATA.isin
    assert data.data == DATA.data

    data = script.load_data_from_db(DATA.symbol, from_=DATA.data[1][0])
    assert data.data == DATA.data[1:]

    data = script.load_data_from_db(DATA.symbol, to=DATA.data[0][0])
    assert data.data == DATA.data[:1]

