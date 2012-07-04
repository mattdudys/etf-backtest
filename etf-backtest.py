#!/usr/bin/env python
import datetime
import sqlite3
import sys
import ystockquote
from pprint import pprint
import yaml

def drop_source(c):
    drop(c, *'period duration quote symbol'.split())

def drop_derived(c):
    drop(c, *'return volatility'.split())

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
    c.execute('''
CREATE TABLE duration (
    duration_id INTEGER PRIMARY KEY,
    unit TEXT NOT NULL,
    unit_qty INTEGER NOT NULL,
    days INTEGER NOT NULL
)''')
    c.execute('CREATE UNIQUE INDEX duration_unit_qty ON duration (unit, unit_qty)')
    truncate(c, 'duration')
    durations = [ \
        ['day', 1, 1], \
        ['week', 1, 10], \
        ['day', 20, 20], \
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
        ticks = get_historical_prices(sym, '2010-06-12', '2012-06-30')
        c.executemany('''
INSERT INTO quote (sym_id, dt, open, high, low, close, volume, adjClose) VALUES (
(SELECT sym_id FROM symbol WHERE sym = ?),?,?,?,?,?,?,?)''', ticks)

def compute_trading_days(c):
    print 'compute trading days'
    c.execute('create table if not exists trading_day (dt date)')
    truncate(c, 'trading_day')
    c.execute('insert into trading_day select distinct dt from prices')
    c.execute('create unique index if not exists trading_day_idx on trading_day (dt)')

def compute_trading_months(c):
    print 'compute last trading day of the month'
    c.execute('create table if not exists trading_month (dt date)')
    truncate(c, 'trading_month')
    c.execute('''insert into trading_month
select max(dt) from trading_day group by date(dt, 'start of month')''')
    c.execute('create unique index if not exists trading_month_idx on trading_month (dt)')

def compute_periods(c, n, unit):
    period_table = 'period_{}_{}'.format(unit, n)
    print 'compute {} {} periods -> {}'.format(n, unit, period_table)
    days = col_query(c, 'select dt from trading_{} order by dt asc'.format(unit))
    if len(days) <= n:
        raise ValueError, 'not enough data to compute {} {} periods'.format(n, unit)
    periods = zip(days, days[n:])
    c.execute('''
create table if not exists {} (
    start_dt date,
    end_dt date);'''.format(period_table))
    truncate(c, period_table)
    c.executemany('insert into {} values (?,?)'.format(period_table), periods)

def compute_returns(c):
    for period_t in period_tables(c):
        unit, n = period_t.split('_')[1:3]
        return_table = 'return_{}_{}'.format(unit, n)
        print 'compute {} {} returns -> {}'.format(n, unit, return_table)
        c.execute('''
create table if not exists {} (
    sym varchar(10),
    end_dt date,
    return real);'''.format(return_table))
        truncate(c, return_table)
        c.execute('''
insert into {}
select p1.sym, pt.end_dt, (p2.adjClose - p1.adjClose) / p1.adjClose
from {} pt, prices p1, prices p2
where pt.start_dt = p1.dt and pt.end_dt = p2.dt and p1.sym = p2.sym;'''.format(return_table, period_t))

def compute_volatility(c):
    vol_periods = col_query(c, "select name from sqlite_master where name like 'period_%' and name not like '%_day_1'")
    for period_t in vol_periods:
        unit, n = period_t.split('_')[1:3]
        volatility_table = 'volatility_{}_{}'.format(unit, n)
        print 'compute {} {} volatility -> {}'.format(n, unit, volatility_table)
        c.execute('''
create table if not exists {} (
    sym varchar(10),
    end_dt date,
    volatility real);'''.format(volatility_table))
        truncate(c, volatility_table)
        syms = symbols(c)
        
        for start_dt, end_dt in c.execute('select start_dt, end_dt from {}'.format(period_t)).fetchall():
            print period_t, end_dt
            for sym in syms:
                returns = tuple(r[0] for r in c.execute('''
select return
from return_day_1
where sym = ? and
      ? < date(end_dt) and
      date(end_dt) <= ?''', (sym, start_dt, end_dt)).fetchall())
                if len(returns) < n:
                    continue
                vol = stdev(returns) * (254 ** 0.5)
                c.execute('insert into {} values (?, ?, ?)'.format(volatility_table), (sym, end_dt, vol))

class ScreenResult:

    def __init__(self, metrics, results):
        self.sym = results[0]
        self.metrics = dict(zip(metrics, results[1:]))

    def __str__(self):
        print sym, metrics

def screen(c, syms, end_dt, metrics, weights):
    assert len(metrics) == len(weights) and len(metrics) >= 1
    num_tables = len(metrics)

    selects = 't0.sym, ' + \
              ', '.join('t{0}.{1}'.format(i, m.split('_')[0]) \
                        for i, m in enumerate(metrics))
    table_aliases = ', '.join('{1} t{0}'.format(i, m) for i, m in enumerate(metrics))
    joins = ' and '.join('t{0}.sym = t{1}.sym and t{0}.end_dt = t{1}.end_dt'.format(i, i+1) \
                         for i in range(0, num_tables - 1))

    query = 'select {0} from {1} where {2} and t0.end_dt = ? and t0.sym in ({3})'''.format( \
        selects, table_aliases, joins, ','.join('?' for s in syms))

    results = c.execute(query, (end_dt,) + syms).fetchall()
    results = sorted(results, key=lambda r: sum(m*w for m,w in zip(r[1:], weights)), reverse=True)
    return results

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
        insert_quotes(c)
##        insert_prices(c, *syms)
##        compute_trading_days(c)
##        compute_trading_months(c)
##        for days in (1, 10, 20, 30):
##            compute_periods(c, days, 'day')
##        for months in (1, 3, 6):
##            compute_periods(c, months, 'month')
##        compute_returns(c)
##        compute_volatility(c)
##        print screen(c, symbols(c), datetime.date(2012, 6, 29), ('return_month_3', 'return_day_20', 'volatility_day_20'), (0.4, 0.3, -0.3))
        #backtest(c, symbols(c), 'period_month_1', ('return_month_3', 'return_day_20', 'volatility_day_20'), (0.4, 0.3, -0.3))       


if __name__ == '__main__':
    main()
