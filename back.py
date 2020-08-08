from datetime import datetime, timedelta
import backtrader as bt
import pandas as pd
import sys
import uuid
from yaho import run_analysis
import time

class BuySellSaver(object):
    def __init__(self):
        self._last_buy_key = None
        self._buy_sell_dict = {}

    def buy(self, buy_datetime):
        buy_key = str(uuid.uuid4())
        self._buy_sell_dict[buy_key] = {'trend_up': buy_datetime, 'trend_down':None}
        self._last_buy_key = buy_key

    def sell(self, sell_datetime):
        self._buy_sell_dict[self._last_buy_key].update({'trend_down': sell_datetime})

    def get_results(self):
        return self._buy_sell_dict

    @staticmethod
    def pretty_print(buy_sell_dict, stock_name):
        for key, dic in buy_sell_dict.items():
            buy, sell = dic['trend_up'], dic['trend_down']
            print('[{}] trend_up date: {} trend_down date: {}'.format(stock_name, buy, sell))


class SmaCross(bt.Strategy):
    bss = None
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=20,  # period for the fast moving average
        pslow=50,   # period for the slow moving average
    )

    @classmethod
    def initialize_bss(cls):
        cls.bss = BuySellSaver()

    def __init__(self):
        sma1 = bt.ind.EMA(period=self.p.pfast)  # fast moving average
        sma2 = bt.ind.EMA(period=self.p.pslow)  # slow moving average
        self.rsi = bt.indicators.RSI_SMA(self.data.close, period=14)
        self._record_buy = False
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal

    def next(self):
        if self._record_buy:
            self.bss.buy(self.position.datetime)
            self._record_buy = False

        if not self.position:  # not in the market
            if self.crossover > 0:  # if fast crosses slow to the upside

                self.buy()  # enter long
                self._record_buy = True

        elif self.crossover < 0:  # in the market & cross to the downside
            self.close() # close long position
            self.bss.sell(self.position.datetime)


def get_cmd_params():
    params = sys.argv
    stck_to_run, slow, fast = None, None, None
    if len(params) > 1:
        stck_to_run = [sys.argv[1]]
    if len(params) > 2:
        slow = int(sys.argv[2])
        fast = int(sys.argv[3])
    return stck_to_run, slow, fast


def alert_stocks_sma_crossing(stocks, slow_sma=None, fast_sma=None):
    if slow and fast:
        SmaCross.params.pfast = fast
        SmaCross.params.pslow = slow

    stocks_dict_bss = {}
    for idx, stock in enumerate(stocks):
        try:
            print('attempting to analyze {}, stock number {}'.format(stock, idx))
            exc=False
            cerebro = bt.Cerebro()  # create a "Cerebro" engine instance
            data = bt.feeds.YahooFinanceData(dataname=stock,
                                             fromdate=datetime(year=2019, month=1, day=1),
                                             todate=datetime(year=2020, month=8, day=7))
            cerebro.adddata(data)  # Add the data feed
            SmaCross.initialize_bss()
            cerebro.addstrategy(SmaCross)  # Add the trading strategy
            cerebro.run()  # run it all
            results = SmaCross.bss.get_results()
            stocks_dict_bss[stock] = results
            BuySellSaver.pretty_print(stock_name=stock, buy_sell_dict=results)
        except Exception as e:
            print('error with {}: {}'.format(stock, e))
            exc = True
        if exc:
            continue
    return stocks_dict_bss


def get_optimal_day_range(rates_for_day_count):
    df = rates_for_day_count.copy()
    grouped_by_day_count = df.groupby('days_count_from_alert', as_index=False)
    avg = grouped_by_day_count.agg({'change_in_percentage': pd.np.mean}).rename(columns={'change_in_percentage': 'mean'})
    median = grouped_by_day_count.agg({'change_in_percentage': pd.np.median}).rename(columns={'change_in_percentage': 'median'})
    sumed = grouped_by_day_count.agg({'change_in_percentage': pd.np.sum}).rename(columns={'change_in_percentage': 'sum'})
    merged = pd.merge(avg, median, on='days_count_from_alert')
    fully_merged = pd.merge(merged, sumed, on='days_count_from_alert')
    return fully_merged


def get_day_range_precision(rates_for_day_count, percentage_th=3):
    def calculate_group_precision(group):
        days_count = group.days_count_from_alert.iloc[0]
        gte_percentage_th = group.change_in_percentage >= percentage_th
        lt_percentage_th = group.change_in_percentage < percentage_th
        tp_count = float(group.loc[gte_percentage_th].shape[0])
        fp_count = group.loc[lt_percentage_th].shape[0]
        try:
            precision = tp_count / tp_count + fp_count
        except Exception as e:
            print('failed to calcuate precision.')
            precision = pd.np.nan

        return pd.DataFrame({'days_count': [days_count], 'precision': [precision]})

    precision = rates_for_day_count.groupby('days_count_from_alert', as_index=False).apply(calculate_group_precision)
    return precision


if __name__ == '__main__':
    cmd_stock, slow, fast = get_cmd_params()
    if not cmd_stock:
        tier1 = ['TSMC','DRIO','AAPL', 'AMZN', 'GOOGL', 'FB', 'AMD', "LDOS", 'AIG', 'IPGP', 'DDOG', 'AMD']
        # tier2 = ['MSFT', 'WMT', 'NVDA', 'VZ',
        #         'DAL', 'LIAUTO', 'AAL', 'AVIS', 'RCL', 'KL', 'LMND', 'BA', 'AC']
        # tier3 = ['ADTN', 'ADBE']

        # tier4 = ['CMCSA', 'CMCL', 'TMUS', 'DRD', 'ASR.TO', 'WPM']
        # tier5 = ['ABB', 'ABBV', 'ABC', 'ABCB', 'SNAP']
        snp500 = list(pd.read_csv('/home/yoni/PycharmProjects/pythonProject/constituents_csv.csv').Symbol)
        # stocks = all_nasdaq_stocks[140:]
        # stocks = ['FB','AMZN', 'AMD', 'GOOGL', 'ARM', 'NVDA', 'YNDX', 'MSFT', 'AAPL', 'CMCL', 'CMCSA', 'ABB', 'ABBV', 'ABC',
        #           'ABCB','SNAP','VZ', 'ASR.TO', 'WPM']
        # tier2_stocks = ['BMCH', 'EXPE', 'DRIO', 'XBIT', 'BLUE', ] #some airlines and biotech
        stocks = snp500
        # stocks = tier3
    else:
        stocks = cmd_stock

    stocks = tier1
    # stocks = ['CTAS', 'CINF', 'BMCH','BLUE', 'CMPR', 'CBNK', 'CCBG', 'CPLP', 'CSWC']
    # stocks = tier1 + tier2
    # stocks_bss_dict_13_26 = alert_stocks_sma_crossing(stocks, 13, 26)
    # stocks_bss_dict_5_10 = alert_stocks_sma_crossing(stocks, 5, 10)
    stocks_bss_dict_20_50 = alert_stocks_sma_crossing(stocks, 20, 50)
    # stocks_bss_dict_10_50 = alert_stocks_sma_crossing(stocks, 10, 50)
    # stocks_bss_dict_50_100 = alert_stocks_sma_crossing(stocks, 50, 100)
    # stocks_bss_dict_50_200 = alert_stocks_sma_crossing(stocks, 50, 200)

    all_measured_smas = {'20_50': stocks_bss_dict_20_50,}
           # '13_26':stocks_bss_dict_13_26,}
           # '20_20':stocks_bss_dict_20_50,
           # '50_200': stocks_bss_dict_50_200,}
           # '50_100': stocks_bss_dict_50_100}
    rates_for_day_count = pd.DataFrame()
    days_count_to_check = [i for i in range(1, 20)]

    for measured_smas, results in all_measured_smas.items():
        for stock, stocks_bss in results.items():
            for key, bss in stocks_bss.items():
                analysis_start_datetime = bss['trend_up']
                buy_time = (analysis_start_datetime + timedelta(days=1)).date()
                analysis_end_datetime = analysis_start_datetime + timedelta(days=max(days_count_to_check) + 1)
                try:
                    result = run_analysis(stock, analysis_start_datetime, analysis_end_datetime)
                except Exception as e:
                    print('error on {} analysis - {}.'.format(stock, e))
                    result = pd.DataFrame()

                if not result.empty:
                    for days_to_check in days_count_to_check:
                        days_to_check_date = (analysis_start_datetime + timedelta(days=days_to_check)).date()
                        if days_to_check_date in result.index:
                            change_after = result.loc[days_to_check_date, 'price_change_from_day_after_alert']
                            df = pd.DataFrame({'stock_name': [stock], 'days_count_from_alert': [days_to_check],
                                               'date': [days_to_check_date],
                                               'alert_time': analysis_start_datetime,
                                               'change_in_percentage': [change_after],
                                               'measured_smas': measured_smas,
                                               'buy_date': buy_time})
                            rates_for_day_count = pd.concat([rates_for_day_count, df], ignore_index=True)
                        else:
                            print('couldnt find day count - {} for {}'.format(days_to_check, stock))


    rates_for_day_count.drop_duplicates(inplace=True)
    day_range_precision = get_day_range_precision(rates_for_day_count, percentage_th=1)
    day_range_stats = get_optimal_day_range(rates_for_day_count)

    timestamp = int(time.time())
    day_range_stats.to_pickle('archive/day_range_stats_{}.pkl'.format(timestamp))
    rates_for_day_count.to_pickle('archive/rates_for_day_count_{}.pkl'.format(timestamp))
    day_range_precision.to_pickle('archive/day_range_precision_{}.pkl'.format(timestamp))

    print(rates_for_day_count)