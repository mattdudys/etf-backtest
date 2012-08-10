#!/usr/bin/env python
import datetime
import sqlite3
import sys
import ystockquote
from pprint import pprint
import yaml
from itertools import izip, islice
import numpy as np
from collections import defaultdict
from operator import itemgetter
from math import floor
import cProfile

START_DATE = '2005-09-30'
END_DATE = '2012-07-25'

def drop_source(c):
    drop(c, *'period duration quote symbol'.split())

def drop_derived(c):
    drop(c, *'return volatility ulcer_index parabolic_sar'.split())
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
        ['month', 1, 20], \
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
CREATE TABLE IF NOT EXISTS return (
    return_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    period_id INTEGER,
    return REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id),
    FOREIGN KEY(period_id) REFERENCES period(period_id)
)''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS return_sym_period ON return (sym_id, period_id)')

    c.execute('''
CREATE VIEW IF NOT EXISTS daily_return AS
SELECT r.sym_id AS sym_id, r.period_id AS period_id, return
FROM return r natural join period p natural join duration d
WHERE d.days = 1''')

    c.execute('''
CREATE TABLE IF NOT EXISTS volatility (
    volatility_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    period_id INTEGER,
    volatility REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id),
    FOREIGN KEY(period_id) REFERENCES period(period_id)
)''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS volatility_sym_period ON volatility (sym_id, period_id)')

    c.execute('''
CREATE TABLE IF NOT EXISTS ulcer_index (
    ulcer_index_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    period_id INTEGER,
    ulcer_index REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id),
    FOREIGN KEY(period_id) REFERENCES period(period_id)    
)''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS ulcer_index_sym_period ON ulcer_index (sym_id, period_id)')

    c.execute('''
CREATE TABLE IF NOT EXISTS parabolic_sar (
    parabolic_sar_id INTEGER PRIMARY KEY,
    sym_id INTEGER,
    dt DATE NOT NULL,
    long_short CHAR(1) NOT NULL,
    parabolic_sar REAL NOT NULL,
    FOREIGN KEY(sym_id) REFERENCES symbol(sym_id)
)''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS parabolic_sar_sym_date ON parabolic_sar (sym_id, dt)')
    c.execute('CREATE INDEX IF NOT EXISTS parabolic_sar_date ON parabolic_sar (dt)')

def insert_symbols(c, symbols_filename):
    print 'load symbols'
    truncate(c, 'symbol')
    with open(symbols_filename) as sym_cfg:
        sym_data = yaml.load(sym_cfg)
        syms = sorted((sym, desc) for cat in sym_data.itervalues() for sym, desc in cat.iteritems())
    c.executemany('INSERT INTO symbol (sym, description) VALUES (?,?)', syms)

def get_historical_prices(sym, start_date, end_date):
    print 'get quotes:', sym, start_date, end_date
    attempt = 0
    while True:
        attempt += 1
        if attempt >= 3:
            raise IOError, "Unable to get data for " + sym + "."
        try:
            ticks = ystockquote.get_historical_prices(sym, start_date, end_date)[1:]
        except IOError as e:
            continue
        break
        
    return [[sym, datetime.datetime.strptime(tick[0], '%Y-%m-%d').date()] + tick[1:] for tick in ticks]

def insert_quotes(c, start_date, end_date):
    truncate(c, 'quote')
    ksym_vid = dict_q(c, 'symbol', 'sym', 'sym_id')
    for sym, sym_id in ksym_vid.iteritems():
        ticks = get_historical_prices(sym, start_date, end_date)
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

class VolatilityHelper:
    def __init__(self):
        pass
    def returns(self, g):
        '''returns a generator of returns. after consuming from this, you can see the last dt in self.last_dt'''
        for dt, ret in g:
            self.last_dt = dt
            yield ret

def compute_volatility(c):
    vh = VolatilityHelper()
    for duration_id, unit, unit_qty, days in c.execute('select duration_id, unit, unit_qty, days from duration where days <> 1').fetchall():
        periods = {end_dt: period_id for period_id, end_dt in c.execute('select period_id, end_dt from period where duration_id = ?', (duration_id,))}
        for sym_id, sym in c.execute('select sym_id, sym from symbol').fetchall():
            print '{} day volatility for {}'.format(days, sym)
            dt_dr = c.execute("""
SELECT p.end_dt, r.return from
 return r, symbol s, period p, duration d
WHERE r.sym_id = s.sym_id AND
      r.period_id = p.period_id AND
      p.duration_id = d.duration_id AND
      d.days = 1 AND
      s.sym = ?""", (sym,)).fetchall()
            for start, end in enumerate(range(days+1, len(dt_dr)+1)):
                ret = np.fromiter(vh.returns(islice(dt_dr, start, end)), np.float)
                vol = ret.std() * 254 ** 0.5
                c.execute('INSERT INTO volatility (sym_id, period_id, volatility) VALUES (?,?,?)', (sym_id, periods[vh.last_dt], vol))

def compute_ulcer_index(c):
    for duration_id, unit, unit_qty, days in c.execute('select duration_id, unit, unit_qty, days from duration where days <> 1').fetchall():
        periods = {end_dt: period_id for period_id, end_dt in c.execute('select period_id, end_dt from period where duration_id = ?', (duration_id,))}
        for sym_id, sym in c.execute('select sym_id, sym from symbol').fetchall():
            print '{} day ulcer index for {}'.format(days, sym)
            dt_quote = c.execute('select dt, adjClose from quote natural join symbol where sym = ? order by dt asc', (sym,)).fetchall()
            for start, end in enumerate(range(days+1, len(dt_quote)+1)):
                mx = None
                ssq = 0
                for dt, p in islice(dt_quote, start, end):
                    mx = max(mx, p)
                    ssq += (100 * (p - mx) / mx) ** 2
                ui = (ssq / days) ** 0.5
                c.execute('INSERT INTO ulcer_index (sym_id, period_id, ulcer_index) VALUES (?,?,?)', (sym_id, periods[dt], ui))

class HighLow:
    def __init__(self, dt, high, low):
        self.dt = dt
        self.high = high
        self.low = low

    def __repr__(self):
        return 'HighLow[{}, {}, {}]'.format(self.dt, self.high, self.low)

def is_hip(d1, d2, d3):
    return d1.high < d2.high and d2.high > d3.high

def is_lop(d1, d2, d3):
    return d1.low > d2.low and d2.low < d3.low

def sip(d1, d2, d3):
    hip, lop = is_hip(d1, d2, d3), is_lop(d1, d2, d3)
    if hip and lop:
        return None, None
    elif hip:
        return d2, None
    elif lop:
        return None, d2
    else:
        return None, None

def compute_parabolic_sar(c):
    for sym_id, sym in dict_q(c, 'symbol', 'sym_id', 'sym').iteritems():
        print 'computing parabolic sar for', sym
        dhl = [HighLow(*r) for r in c.execute('SELECT dt, high, low FROM quote WHERE quote.sym_id = ? ORDER BY dt ASC', (sym_id,)).fetchall()]
        d1, d2, d3 = dhl[0:3]

        # Find first high or low significant points to determine whether to initiate an initial short or long position.
        for i, d4 in enumerate(islice(dhl, 3, None), start=1):
            hip, lop = sip(d1, d2, d3)
            d1, d2, d3  = d2, d3, d4
            if hip or lop:
                #print hip, lop
                first_sigp_i = i
                break

        # For the first day of entry, SAR is the previous significant point. If long, the LOP, if short, the HIP.
        if lop:
            position = 'L'
            max_p = d2.high
            sar = lop.low
        else:
            position = 'S'
            min_p = d2.low
            sar = hip.high
        af = 0.02

        for d4 in islice(dhl, first_sigp_i + 3, None):
            #print '{} | {:<5} | {:>6.2f} | {:>6.2f} | {:>6.2f} |'.format(d2.dt, position, d2.high, d2.low, sar),
            c.execute('INSERT INTO parabolic_sar (sym_id, dt, long_short, parabolic_sar) VALUES (?,?,?,?)', (sym_id, d2.dt, position, sar))
            
            if position == 'L':
                if sar > d2.low:
                    # reversal
                    position = 'S'
                    af = 0.02
                    min_p = d2.low
                    diff = 0
                    af_diff = 0
                    #print '{:>6.2f} | {:>6.2f} | {:>1.2f} | {:>2.2f} |'.format(min_p, diff, af, af_diff)
                    sar = max_p
                else:
                    if d2.high > max_p:
                        max_p = d2.high
                        new_ep = 1
                    else:
                        new_ep = 0
                    #print '{:>6.2f} |'.format(max_p),

                    diff = max_p - sar
                    af_diff = af * diff
                    #print '{:>6.2f} | {:>1.2f} | {:>2.2f} |'.format(diff, af, af_diff)

                    if new_ep:
                        af = min(af + 0.02, 0.2)
                    
                    sar += af_diff

                    if sar > d1.low or sar > d2.low:
                        sar = min(d1.low, d2.low)
            else:
                if sar < d2.high:
                    # reversal
                    position = 'L'
                    af = 0.02
                    max_p = d2.high
                    diff = 0
                    af_diff = 0
                    #print '{:>6.2f} | {:>6.2f} | {:>1.2f} | {:>2.2f} |'.format(max_p, diff, af, af_diff)
                    sar = min_p
                else:
                    if d2.low < min_p:
                        min_p = d2.low
                        new_ep = 1
                    else:
                        new_ep = 0
                    #print '{:>6.2f} |'.format(min_p),

                    diff = sar - min_p
                    af_diff = af * diff
                    #print '{:>6.2f} | {:>1.2f} | {:>2.2f} |'.format(diff, af, af_diff)

                    if new_ep:
                        af = min(af + 0.02, 0.2)

                    sar -= af_diff

                    if sar < d1.high or sar < d2.high:
                        sar = max(d1.high, d2.high)

            d1, d2, d3 = d2, d3, d4

def return_vol_screen(c, syms, end_dt, weights):
    assert abs(sum(weights) - 1) < 0.001
    params = '?,' * (len(syms) - 1) + '?'
    raw_results = c.execute('''
SELECT s.sym, t0.return0, t1.return1, t2.volatility0, t3.volatility1
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
 (SELECT v0.sym_id AS sym_id, v0.volatility AS volatility0 FROM
    volatility v0
    NATURAL JOIN period v0p
    NATURAL JOIN duration v0d
 WHERE v0d.unit = 'month' AND
       v0p.end_dt = ?) AS t2
 NATURAL JOIN
 (SELECT v1.sym_id AS sym_id, v1.volatility AS volatility1 FROM
    volatility v1
    NATURAL JOIN period v1p
    NATURAL JOIN duration v1d
 WHERE v1d.unit = 'quarter' AND
       v1p.end_dt = ?) AS t3
 NATURAL JOIN symbol s
 WHERE s.sym IN ({})'''.format(params), (end_dt,) * 4 + tuple(syms)).fetchall()

    scores = defaultdict(float)
    for i in range(1,5):
        rev = i <= 2
        sym_score = sorted(map(itemgetter(0,i), raw_results), key=itemgetter(1), reverse=rev)
        for sym, score in sym_score:
            scores[sym] += weights[i-1] * score
    final_ranked = sorted(scores.items(), key=itemgetter(1))
    ksym_vdata = dict((r[0], r[1:]) for r in raw_results)
    return [(i, sym) + ksym_vdata[sym] for i, (sym, score) in enumerate(final_ranked)]

def return_vol_ranked_screen(c, syms, end_dt, weights):
    assert abs(sum(weights) - 1) < 0.001
    params = '?,' * (len(syms) - 1) + '?'
    raw_results = c.execute('''
SELECT s.sym, t0.return0, t1.return1, t2.volatility0, t3.volatility1
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
 (SELECT v0.sym_id AS sym_id, v0.volatility AS volatility0 FROM
    volatility v0
    NATURAL JOIN period v0p
    NATURAL JOIN duration v0d
 WHERE v0d.unit = 'month' AND
       v0p.end_dt = ?) AS t2
 NATURAL JOIN
 (SELECT v1.sym_id AS sym_id, v1.volatility AS volatility1 FROM
    volatility v1
    NATURAL JOIN period v1p
    NATURAL JOIN duration v1d
 WHERE v1d.unit = 'quarter' AND
       v1p.end_dt = ?) AS t3
 NATURAL JOIN symbol s
 WHERE s.sym IN ({})'''.format(params), (end_dt,) * 4 + tuple(syms)).fetchall()

    scores = defaultdict(float)
    for i in range(1,5):
        rev = i <= 2
        ranked = sorted(map(itemgetter(0,i), raw_results), key=itemgetter(1), reverse=rev)
        for rank, (sym, score) in enumerate(ranked):
            scores[sym] += weights[i-1] * rank
    final_ranked = sorted(scores.items(), key=itemgetter(1))
    ksym_vdata = dict((r[0], r[1:]) for r in raw_results)
    return [(i, sym) + ksym_vdata[sym] for i, (sym, score) in enumerate(final_ranked)]

def sharpe_screen(c, syms, end_dt, weights):
    assert abs(sum(weights) - 1) < 0.001
    params = '?,' * (len(syms) - 1) + '?'
    raw_results = c.execute('''
SELECT s.sym, t0.return0, t1.return1,
       t0.return0 / t2.volatility0, t1.return1 / t3.volatility1,
       (t1.return1 / t3.volatility1) - (t0.return0 / 3 / t2.volatility0)
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
 (SELECT v0.sym_id AS sym_id, v0.volatility AS volatility0 FROM
    volatility v0
    NATURAL JOIN period v0p
    NATURAL JOIN duration v0d
 WHERE v0d.unit = 'quarter' AND
       v0p.end_dt = ?) AS t2
 NATURAL JOIN
 (SELECT v1.sym_id AS sym_id, v1.volatility AS volatility1 FROM
    volatility v1
    NATURAL JOIN period v1p
    NATURAL JOIN duration v1d
 WHERE v1d.unit = 'month' AND
       v1p.end_dt = ?) AS t3
 NATURAL JOIN symbol s
 WHERE s.sym IN ({})'''.format(params), (end_dt,) * 4 + tuple(syms)).fetchall()

    scores = defaultdict(float)
    for i in range(1,6):
        ranked = sorted(map(itemgetter(0,i), raw_results), key=itemgetter(1), reverse=True)
        for rank, (sym, score) in enumerate(ranked):
            scores[sym] += weights[i-1] * rank
    final_ranked = sorted(scores.items(), key=itemgetter(1))
    ksym_vdata = dict((r[0], r[1:]) for r in raw_results)
    return [(i, sym) + ksym_vdata[sym] for i, (sym, score) in enumerate(final_ranked)]

def backtest(c, syms, screener, screener_args, start_cash=50000.00):
    assert start_cash > 0
    cash = start_cash
    spy_cash = start_cash

    returns = []
    spy_returns = []

    sym = None
    for d in month_ends(c):
        scr_res = screener(c, syms, d, screener_args)
        if not scr_res:
            continue

        if sym:
            
            # Sell the shares
            exit_prc = price(c, sym, d)
            exit_amt = shares * exit_prc
            pnl = exit_amt - enter_amt
            pnl_pct = (exit_prc - enter_prc) / enter_prc
            returns.append(pnl_pct)
            cash += exit_amt

            # Sell SPY
            exit_spy_prc = price(c, 'SPY', d)
            exit_spy_amt = spy_shares * exit_spy_prc
            spy_pnl = exit_spy_amt - enter_spy_amt
            spy_pnl_pct = (exit_spy_prc - enter_spy_prc) / enter_spy_prc
            spy_returns.append(spy_pnl_pct)
            spy_cash += exit_spy_amt

            # Print performance for period
            print '{} | {} | {:>5} | {:>5} | {:>6.2f} | {:>6.2f} | {:>9.2f} | {:>7.2%} | {:>10.2f} | {:>9.2f} | {:>7.2%} | {:>10.2f}'.format( \
                last_d, d, sym, shares, enter_prc, exit_prc, pnl, pnl_pct, cash, spy_pnl, spy_pnl_pct, spy_cash)

        # Find the best
        # print scr_res[0][1:]
        sym = scr_res[0][1]
        last_d = d

        # Buy the best
        enter_prc = price(c, sym, d)
        shares = int(cash // enter_prc)
        enter_amt = enter_prc * shares
        cash -= enter_amt

        # Buy SPY
        enter_spy_prc = price(c, 'SPY', d)
        spy_shares = int(spy_cash // enter_spy_prc)
        enter_spy_amt = enter_spy_prc * spy_shares
        spy_cash -= enter_spy_amt

    # Sell the shares
    exit_prc = price(c, sym, d)
    exit_amt = shares * exit_prc
    pnl = exit_amt - enter_amt
    pnl_pct = (exit_prc / enter_prc) - 1
    cash += exit_amt

    # Sell SPY
    exit_spy_prc = price(c, 'SPY', d)
    exit_spy_amt = spy_shares * exit_spy_prc
    spy_pnl = exit_spy_amt - enter_spy_amt
    spy_pnl_pct = (exit_spy_prc / enter_spy_prc) - 1
    spy_cash += exit_spy_amt

    # Print next screen
    print '\n'.join(map(str, screener(c, syms, d, screener_args)))

    # Print total performance
    tot_return = (cash - start_cash) / start_cash
    spy_tot_return = (spy_cash - start_cash) / start_cash
    tot_vol = stdev(returns) * 12 ** 0.5
    spy_vol = stdev(spy_returns) * 12 ** 0.5
    for f, v in zip(('Return', 'SPY Return', 'Vol', 'SPY Vol'), (tot_return, spy_tot_return, tot_vol, spy_vol)):
        print '{:>10}: {:6.2%}'.format(f, v)

def dict_q(c, table, key_col, value_col):
    '''returns a q_col_i -> id_col_i dict'''
    return dict(c.execute('select {}, {} from {}'.format(key_col, value_col, table)).fetchall())

def truncate(c, table):
    c.execute('delete from {}'.format(table))

def drop(c, *tables):
    print 'dropping', tables
    for t in tables:
        c.execute('drop table if exists ' + t)

def month_ends(c):
    return col_query(c, "SELECT MAX(dt) AS end_dt FROM quote GROUP BY date(dt, 'start of month') ORDER BY end_dt")

def scalar_query(c, query, *args):
    return col_query(c, query, *args)[0]

def col_query(c, query, *args):
    return tuple(r[0] for r in c.execute(query, args).fetchall())

def price(c, sym, dt):
    return scalar_query(c, "SELECT q.adjClose FROM quote q NATURAL JOIN symbol s WHERE s.sym = ? AND q.dt = ?", sym, dt)

def symbols(c):
    return col_query(c, "SELECT s.sym FROM symbol s")

def avg(s):
    return sum(s) / len(s)

def stdev(s):
    a = avg(s)
    sdsq = sum([(i - a) ** 2 for i in s])
    stdev = (sdsq / (len(s) - 1)) ** 0.5
    return stdev

def main():
    syms = 'VTI VEU VWO BLV'.split()
    syms = 'RSP BLV EWA DBC VNQ VWO'.split()
    with sqlite3.connect('symbols2.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES) as c:
##        create_source_tables(c)
##        insert_symbols(c, 'symbols2.yml')
        #syms = list(symbols(c))
        #syms.remove('MS')
##        insert_quotes(c, START_DATE, END_DATE)
##        compute_periods(c)
        create_derived_tables(c)
##        compute_returns(c)
##        compute_volatility(c)
##        compute_ulcer_index(c)
        compute_parabolic_sar(c)
##        print '\n'.join(map(str, return_vol_ranked_screen(c, syms, datetime.date(2012, 7, 17), (0.4, 0.3, 0, 0.3))))
##        print '\n'.join(map(str, sharpe_screen(c, 'SPY SHY TIP TLT BLV QQQ GLD VNQ EWA VWO'.split(), datetime.date(2012, 7, 10), (0.5, 0.5))))
##        print price(c, 'SPY', datetime.date(2012, 7, 10))
        #cProfile.runctx('backtest(c, syms, return_vol_screen, (0.4, 0.3, 0, 0.3))', globals(), locals())
        #backtest(c, syms, return_vol_ranked_screen, (.3, .3, 0, .4))
        #backtest(c, syms, sharpe_screen, (0, 0, 0, 1, 0))
        #backtest(c, ('SPY',), sharpe_screen, (0.6, 0.4))

if __name__ == '__main__':
    main()
