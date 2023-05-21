import datetime
import re
from dateutil.relativedelta import *
import sqlite3
import time

import utils

# region SQL Strings
CREATE_FUNDS_TABLE = '''
CREATE TABLE IF NOT EXISTS funds (
    symbol TEXT NOT NULL,
    performanceId TEXT,
    longName TEXT,
    quoteType TEXT,
    firstTradeDateMilliseconds INTEGER,
    lastScreened INTEGER,
    yhFinanceLastAcquired INTEGER,
    msFinanceLastAcquired INTEGER,
    ytd REAL,
    lastBearMkt REAL,
    lastBullMkt REAL,
    oneMonth REAL,
    threeMonth REAL,
    oneYear REAL,
    threeYear REAL,
    fiveYear REAL,
    tenYear REAL,
    beta3Year REAL,
    bookValue REAL,
    category TEXT,
    exchange TEXT,
    fundFamily TEXT,
    market TEXT,
    marketCap INTEGER,
    marketState TEXT,
    priceHint INTEGER,
    priceToBook REAL,
    regularMarketChange REAL,
    regularMarketChangePercent REAL,
    regularMarketPreviousClose REAL,
    regularMarketPrice REAL,
    sharesOutstanding INTEGER,
    starRating INTEGER,
    totalAssets INTEGER,
    twelveBOne REAL,
    tradeable TEXT,
    triggerable TEXT,
    yield REAL,
    PRIMARY KEY (symbol),
    UNIQUE (performanceId)
);
'''

DROP_FUNDS_TABLE = '''DROP TABLE IF EXISTS funds;'''

CREATE_ANNUALTOTALRETURNS_TABLE = '''
CREATE TABLE IF NOT EXISTS annualTotalReturns (
    symbol TEXT NOT NULL,
    year INTEGER NOT NULL,
    return REAL,
    PRIMARY KEY (symbol, year),
    FOREIGN KEY (symbol) REFERENCES funds(symbol) ON DELETE CASCADE
);
'''

DROP_ANNUALTOTALRETURNS_TABLE = '''DROP TABLE IF EXISTS annualTotalReturns;'''

CREATE_BROKERAGES_TABLE = '''
CREATE TABLE IF NOT EXISTS brokerages (
    symbol TEXT NOT NULL,
    brokerage TEXT NOT NULL,
    PRIMARY KEY (symbol, brokerage),
    FOREIGN KEY (symbol) REFERENCES funds(symbol) ON DELETE CASCADE
);
'''

DROP_BROKERAGES_TABLE = '''DROP TABLE IF EXISTS brokerages;'''


# endregion

class DB:
    def __init__(self, dbname='tickerTracker.db'):
        self.connection = sqlite3.connect(dbname)
        self.connection.create_function('REGEXP', 2, function_regex)
        self.cursor = self.connection.cursor()

    def close_connections(self):
        self.cursor.close()
        self.connection.close()

    def create_tables(self):
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute('PRAGMA foreign_keys = ON;')
            self.cursor.execute(CREATE_FUNDS_TABLE)
            self.cursor.execute(CREATE_ANNUALTOTALRETURNS_TABLE)
            self.cursor.execute(CREATE_BROKERAGES_TABLE)
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def drop_tables(self):
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute(DROP_ANNUALTOTALRETURNS_TABLE)
            self.cursor.execute(DROP_FUNDS_TABLE)
            self.cursor.execute(DROP_BROKERAGES_TABLE)
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def add_from_screener(self, quotes):
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            for quote in quotes:
                quote['unix_time'] = unix_time()
                rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol;', quote)
                if rows.fetchone():
                    self.cursor.execute('''UPDATE funds
                    SET
                        longName = :longName,
                        quoteType = :quoteType,
                        firstTradeDateMilliseconds = :firstTradeDateMilliseconds,
                        exchange = :exchange,
                        market = :market,
                        marketCap = :marketCap,
                        marketState = :marketState,
                        priceHint = :priceHint,
                        priceToBook = :priceToBook,
                        regularMarketChange = :regularMarketChange,
                        regularMarketChangePercent = :regularMarketChangePercent,
                        regularMarketPreviousClose = :regularMarketPreviousClose,
                        regularMarketPrice = :regularMarketPrice,
                        sharesOutstanding = :sharesOutstanding,
                        tradeable = :tradeable,
                        triggerable = :triggerable
                    WHERE :symbol = symbol;''', quote)
                else:
                    self.cursor.execute('''INSERT INTO funds (
                        symbol,
                        longName,
                        quoteType,
                        firstTradeDateMilliseconds,
                        exchange,
                        market,
                        marketCap,
                        marketState,
                        priceHint,
                        priceToBook,
                        regularMarketChange,
                        regularMarketChangePercent,
                        regularMarketPreviousClose,
                        regularMarketPrice,
                        sharesOutstanding,
                        tradeable,
                        triggerable
                    ) VALUES (
                        :symbol,
                        :longName,
                        :quoteType,
                        :firstTradeDateMilliseconds,
                        :exchange,
                        :market,
                        :marketCap,
                        :marketState,
                        :priceHint,
                        :priceToBook,
                        :regularMarketChange,
                        :regularMarketChangePercent,
                        :regularMarketPreviousClose,
                        :regularMarketPrice,
                        :sharesOutstanding,
                        :tradeable,
                        :triggerable
                    );''', quote)
                self.cursor.execute('UPDATE funds SET lastScreened = :unix_time WHERE :symbol = symbol;', quote)
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def update_from_yh_finance(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol;', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    ytd = :ytd,
                    lastBearMkt = :lastBearMkt,
                    lastBullMkt = :lastBullMkt,
                    oneMonth = :oneMonth,
                    threeMonth = :threeMonth,
                    oneYear = :oneYear,
                    threeYear = :threeYear,
                    fiveYear = :fiveYear,
                    tenYear = :tenYear,
                    beta3Year = :beta3Year,
                    category = :category,
                    totalAssets = :totalAssets,
                    fundFamily = :fundFamily,
                    yield = :percent_yield,
                    twelveBOne = :twelveBOne
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('DELETE FROM annualTotalReturns WHERE :symbol = symbol;', data)
            for single_return in data['returns']:
                single_return['symbol'] = data['symbol']
                self.cursor.execute('INSERT OR IGNORE INTO annualTotalReturns VALUES (:symbol, :year, :annualValue);',
                                    single_return)
            for brokerage in data['brokerages']:
                self.cursor.execute('INSERT OR IGNORE INTO brokerages VALUES (:symbol, :brokerage);',
                                    {'symbol': data['symbol'], 'brokerage': brokerage})
            self.cursor.execute('UPDATE funds SET yhFinanceLastAcquired = :unix_time WHERE :symbol = symbol;', data)
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def update_from_ms_finance(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol;', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    starRating = :starRating
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('UPDATE funds SET msFinanceLastAcquired = :unix_time WHERE :symbol = symbol;', data)
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def update_performance_id(self, data):
        self.cursor.execute('BEGIN TRANSACTION;')
        data['unix_time'] = unix_time()
        try:
            rows = self.cursor.execute('SELECT symbol FROM funds WHERE symbol = :symbol;', data)
            if rows.fetchone():
                self.cursor.execute('''UPDATE funds
                SET
                    performanceId = :performanceId
                WHERE :symbol = symbol;''', data)
            else:
                raise sqlite3.OperationalError(f'symbol {data["symbol"]} is not in the database.')
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def valid_for_yh_finance_view(self) -> set:
        sql = '''
        SELECT symbol FROM funds WHERE yhFinanceLastAcquired IS NULL OR yhFinanceLastAcquired <= :lastMonthEpoch;
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()}).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e
        selection_set = set()
        for select in selection:
            selection_set.add(select[0])
        return selection_set & self.valid_funds()

    def valid_for_ms_finance_view(self) -> set:
        sql = '''
        SELECT symbol, performanceId FROM funds WHERE
        (msFinanceLastAcquired IS NULL OR msFinanceLastAcquired <= :lastMonthEpoch) AND performanceId IS NOT NULL;
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()}).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e
        selection_symbols = set()
        for symbol, _ in selection:
            selection_symbols.add(symbol)
        valid_symbols = selection_symbols & self.valid_funds()
        return_perf_ids = set()
        for symbol, performance_id in selection:
            if symbol in valid_symbols:
                return_perf_ids.add(performance_id)
        return return_perf_ids

    def valid_for_perf_id_view(self) -> set:
        sql = '''
        SELECT symbol FROM funds WHERE performanceId IS NULL;
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e
        selection_set = set()
        for select in selection:
            selection_set.add(select[0])
        return selection_set & self.valid_funds()

    def valid_funds(self) -> set:
        # TODO add dynamic filter recognition (1 filter file that automatically filters from screen, yh, & ms)
        data = {
            'epoch_ms_ten_years': get_epoch_from_ms(years=10),
            'lastMonthEpoch': get_last_month_epoch_ms()
        }
        sql = utils.valid_funds
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            selection = self.cursor.execute(sql, data).fetchall()
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e
        return_selection = set()
        for item in selection:
            return_selection.add(item[0])
        return return_selection

    def delete_unscreened(self):
        sql = '''
        DELETE FROM funds WHERE lastScreened <= :lastMonthEpoch;
        '''
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute(sql, {'lastMonthEpoch': get_last_month_epoch_ms()})
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e

    def delete_fund(self, symbol, perf_id):
        if symbol:
            sql = '''
            DELETE FROM funds WHERE symbol = :symbol;
            '''
        elif perf_id:
            sql = '''
            DELETE FROM funds WHERE performanceId = :performanceId;
            '''
        else:
            raise Exception('Bad Input to Delete Fund')
        self.cursor.execute('BEGIN TRANSACTION;')
        try:
            self.cursor.execute(sql, {'symbol': symbol, 'performanceId': perf_id})
            self.cursor.execute('COMMIT TRANSACTION;')
        except Exception as e:
            self.cursor.execute('ROLLBACK TRANSACTION;')
            raise e


def function_regex(value, pattern):
    c_pattern = re.compile(pattern.lower())
    return c_pattern.search(fr'\b{value.lower()}\b') is not None


def unix_time():
    return int(time.time_ns() / 1000)


def get_last_month_epoch_ms():
    today = datetime.datetime.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    return int(last_month.timestamp() * 1000)


def get_epoch_from_ms(days=0, months=0, years=0):
    today = datetime.datetime.today() - relativedelta(years=years, months=months, days=days)
    return int(today.timestamp() * 1000)


if __name__ == '__main__':
    db = DB()
    print(db.valid_for_perf_id_view())
    print(db.valid_for_ms_finance_view())
    print(db.valid_for_yh_finance_view())
    print(get_last_month_epoch_ms())
    print(get_epoch_from_ms(years=10))
