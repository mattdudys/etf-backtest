#!/usr/bin/env python
import datetime
import sqlite3
import sys
import ystockquote
from pprint import pprint
import yaml
from itertools import izip
import numpy as np
from collections import defaultdict

def drop_source(c):
    drop(c, *'period duration quote symbol'.split())

def drop_derived(c):
    drop(c, *'return volatility'.split())
    c.execute('DROP VIEW IF EXISTS daily_return')

def drop_all(c):
    drop_derived(c)
    drop_source(c)

def create_source_tables(c):
    drop_all(c)
    
    print 'create source tables'
    c.execute('''
CREATE TABLE symbol (
    sym_id INTEGER PRIMARY KEY,
    sym TEXT UNIQUE NOT NULL,
    description TEXT NOT NULL
)''')
    c.execute('''
CREATE TABLE quote (
    quote_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    dt DATE NOT NULL, 
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    adjClose REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id)
)''')
    c.execute('CREATE UNIQUE INDEX quote_sym_dt ON quote (sym_id, dt)')
    c.execute('CREATE UNIQUE INDEX quote_dt_sym ON quote(dt, sym_id)')
    c.execute('''
CREATE TABLE duration (
    duration_id INTEGER PRIMARY KEY,
    unit TEXT NOT NULL,
    unit_qty INTEGER NOT NULL,
    days INTEGER UNIQUE NOT NULL
)''')
    c.execute('CREATE UNIQUE INDEX duration_unit_qty ON duration (unit, unit_qty)')
    truncate(c, 'duration')
    durations = [ \
        ['day', 1, 1], \
        ['month', 1, 21], \
        ['quarter', 1, 63]]
    c.executemany('INSERT INTO duration (unit, unit_qty, days) VALUES (?,?,?)', durations)

    c.execute('''
CREATE TABLE period (
    period_id INTEGER PRIMARY KEY,
    duration_id INTEGER,
    start_dt DATE NOT NULL,
    end_dt DATE NOT NULL,
    FOREIGN KEY(duration_id) REFERENCES duration(duration_id)
)''')

def create_derived_tables(c):
    print 'create derived tables'
    drop_derived(c)

    c.execute('''
CREATE TABLE return (
    return_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    period_id INTEGER,
    return REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id),
    FOREIGN KEY(period_id) REFERENCES period(period_id)
)''')
    c.execute('CREATE UNIQUE INDEX return_sym_period ON return (sym_id, period_id)')

    c.execute('''
CREATE VIEW daily_return AS
SELECT r.sym_id AS sym_id, r.period_id AS period_id, return
FROM return r natural join period p natural join duration d
WHERE d.days = 1''')

    c.execute('''
CREATE TABLE volatility (
    volatility_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    period_id INTEGER,
    volatility REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id),
    FOREIGN KEY(period_id) REFERENCES period(period_id)
)''')
    c.execute('CREATE UNIQUE INDEX volatility_sym_period ON volatility (sym_id, period_id)')

def insert_symbols(c, symbols_filename):
    print 'load symbols'
    truncate(c, 'symbol')
    with open(symbols_filename) as sym_cfg:
        sym_data = yaml.load(sym_cfg)
        syms = sorted((sym, desc) for cat in sym_data.itervalues() for sym, desc in cat.iteritems())
    c.executemany('INSERT INTO symbol (sym, description) VALUES (?,?)', syms)

def get_historical_prices(sym, start_date, end_date):
    print 'get quotes:', sym, start_date, end_date
    ticks = ystockquote.get_historical_prices(sym, start_date, end_date)[1:]
    return [[sym, datetime.datetime.strptime(tick[0], '%Y-%m-%d').date()] + tick[1:] for tick in ticks]

def insert_quotes(c):
    truncate(c, 'quote')
    ksym_vid = dict_q(c, 'symbol', 'sym', 'sym_id')
    for sym, sym_id in ksym_vid.iteritems():
        ticks = get_historical_prices(sym, '2010-06-12', '2012-07-08')
        c.executemany('''
INSERT OR REPLACE INTO quote (sym_id, dt, open, high, low, close, volume, adjClose) VALUES (
(SELECT sym_id FROM symbol WHERE sym = ?),?,?,?,?,?,?,?)''', ticks)

def compute_periods(c):
    durations = c.execute('select duration_id, unit, unit_qty, days from duration').fetchall()
    trading_days = col_query(c, 'select distinct dt from quote order by dt')
    for duration_id, unit, unit_qty, days in durations:
        if len(trading_days) < days:
            raise ValueError, "not enough trading days to compute {} day periods".format(days)        
        print 'compute {} {} period: {} days'.format(unit_qty, unit, days)
        for start_dt, end_dt in izip(trading_days, trading_days[days:]):
            c.execute('INSERT INTO period (duration_id, start_dt, end_dt) VALUES (?,?,?)', (duration_id, start_dt, end_dt))

def compute_returns(c):
    print 'compute returns'
    c.execute('''
INSERT INTO return (sym_id, period_id, return)
SELECT q1.sym_id, p.period_id, (q2.adjClose - q1.adjClose) / q1.adjClose
FROM period p,
    quote q1,
    quote q2
WHERE
    p.start_dt = q1.dt AND
    p.end_dt = q2.dt AND
    q1.sym_id = q2.sym_id''')

class Volatility:
    def __init__(self):
        self.a = []

    def step(self, value):
        self.a.append(value)

    def finalize(self):
        return np.array(self.a).std() * 254 ** 0.5

def compute_volatility(c):
    c.create_aggregate('vol', 1, Volatility)
    sym_symid = dict_q(c, 'symbol', 'sym', 'sym_id')
    for sym, sym_id in sym_symid.iteritems():
        print 'compute volatility for', sym
        c.execute('''
INSERT INTO volatility (sym_id, period_id, volatility)
SELECT ?, vwp.period_id, vol(wr.return)
FROM
   (SELECT wp.period_id AS period_id, wp.start_dt AS start_dt, wp.end_dt AS end_dt
    FROM
       (SELECT MIN(drp.start_dt) AS mn_dt, MAX(drp.end_dt) AS mx_dt
        FROM daily_return dr
             NATURAL JOIN period drp
        WHERE dr.sym_id = ?) drr,
        period wp
        NATURAL JOIN duration vd
    WHERE vd.days > 1 AND
          drr.mn_dt <= wp.start_dt AND
          wp.end_dt <= drr.mx_dt) AS vwp,
   (SELECT drp.end_dt AS dt, dr.return AS return
    FROM daily_return dr
         NATURAL JOIN period drp
    WHERE dr.sym_id = ?) AS wr
WHERE vwp.start_dt < wr.dt AND
      wr.dt <= vwp.end_dt
GROUP BY vwp.period_id''', (sym_id, sym_id, sym_id))

def screen(c, end_dt, w0, w1, w2):
    assert w0 + w1 + w2 == 1
    return c.execute('''
SELECT s.sym, t0.return0, t1.return1, t2.volatility,
       t0.return0 * ? + t1.return1 * ? - t2.volatility * ? AS score
FROM
(SELECT r0.sym_id AS sym_id, r0.return AS return0 FROM
    return r0
    NATURAL JOIN period r0p
    NATURAL JOIN duration r0d
 WHERE r0d.unit = 'quarter' AND
       r0p.end_dt = ?) AS t0
 NATURAL JOIN
(SELECT r1.sym_id AS sym_id, r1.return AS return1 FROM
    return r1
    NATURAL JOIN period r1p
    NATURAL JOIN duration r1d
 WHERE r1d.unit = 'month' AND
       r1p.end_dt = ?) AS t1
 NATURAL JOIN
 (SELECT v0.sym_id AS sym_id, v0.volatility AS volatility FROM
    volatility v0
    NATURAL JOIN period v0p
    NATURAL JOIN duration v0d
 WHERE v0d.unit = 'month' AND
       v0p.end_dt = ?) AS t2
 NATURAL JOIN symbol s
 ORDER BY score DESC''', (w0, w1, w2, end_dt, end_dt, end_dt)).fetchall()

def backtest(c, syms, period, metrics, weights):
    last_sym, last_dt = None, None
    total_ret = 1
    returns = []
    for end_dt in end_dts(c, period):
        s = screen(c, syms, end_dt, metrics, weights)
        if not s:
            print end_dt, 'No screen results.'
        else:
            if last_sym:
                ret = tuple(r[0] for r in \
                            c.execute('select return from return_month_1 where sym = ? and end_dt = ?', (last_sym, last_dt)))[0]
                returns.append(ret)
                total_ret *= (1 + ret)
                print ret, total_ret
            sym = s[0][0]
            print end_dt, sym,
            last_sym, last_dt = sym, end_dt

    print
    print 'Total Return: ', total_ret
    print 'Volatility: ', stdev(returns) * (12 ** 0.5)

def symbols(c):
    return col_query(c, 'select distinct sym from prices;')

def dict_q(c, table, key_col, value_col):
    '''returns a q_col_i -> id_col_i dict'''
    return dict(c.execute('select {}, {} from {}'.format(key_col, value_col, table)).fetchall())

def truncate(c, table):
    c.execute('delete from {}'.format(table))

def drop(c, *tables):
    print 'dropping', tables
    for t in tables:
        c.execute('drop table if exists ' + t)

def period_tables(c):
    return col_query(c, "select name from sqlite_master where name like 'period_%'")

def end_dts(c, period):
    return col_query(c, "select end_dt from {} order by end_dt".format(period))

def col_query(c, query):
    return tuple(r[0] for r in c.execute(query).fetchall())

def avg(s):
    return sum(s) / len(s)

def stdev(s):
    a = avg(s)
    sdsq = sum([(i - a) ** 2 for i in s])
    stdev = (sdsq / (len(s) - 1)) ** 0.5
    return stdev

def main():    
    with sqlite3.connect('prices.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as c:
##        create_source_tables(c)
##        insert_symbols(c, 'symbols.yml')
##        insert_quotes(c)
##        compute_periods(c)
##        create_derived_tables(c)
##        compute_returns(c)
##        compute_volatility(c)
        for s in screen(c, datetime.date(2012, 7, 6), 0.4, 0.4, 0.2):
            print '\t'.join(map(str, s))
        #backtest(c, symbols(c), 'period_month_1', ('return_month_3', 'return_day_20', 'volatility_day_20'), (0.4, 0.3, -0.3))       


if __name__ == '__main__':
    main()
