from yahooquery import Ticker
import pandas as pd
from datetime import datetime, timedelta


def calculate_price_percentage_diff(original, new):
    return 100 - ((original / float(new)) * 100)


def get_stock_history(stock_name, start_analyze_datetime, end_analyze_datetime):
    start_analyze_date, end_analyze_date = start_analyze_datetime.date(), end_analyze_datetime.date()
    ticker = Ticker(stock_name)
    stock_history = ticker.history(start=str(start_analyze_date), end=str(end_analyze_date))
    stock_history.reset_index(inplace=True)
    stock_history['date'] = pd.to_datetime(stock_history['date'])
    stock_history.set_index('date', inplace=True)
    return stock_history


def get_stock_change_from_day_after_start(stock_history, start_analyze_datetime):
    one_day_after_alert = (start_analyze_datetime + timedelta(days=1)).date()
    day_after_alert_open_price = float(stock_history.loc[one_day_after_alert, 'open'])
    stock_history['price_change_from_day_after_alert'] = stock_history['close'].apply(
        lambda daily_price: calculate_price_percentage_diff(day_after_alert_open_price, daily_price))
    return stock_history


def run_analysis(stock_name, start_analyze_datetime, end_analyze_datetime):
    history = get_stock_history(stock_name, start_analyze_datetime, end_analyze_datetime)
    analysis = get_stock_change_from_day_after_start(history, start_analyze_datetime)
    return analysis


if __name__ == '__main__':
    start_analyze = datetime(year=2020, month=5, day=8)
    end_analyze = datetime(year=2020, month=5, day=30)
    stock = 'AAPL'
    analysis_result = run_analysis(stock, start_analyze, end_analyze)
    print(analysis_result)

