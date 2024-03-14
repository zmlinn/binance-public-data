#!/usr/bin/env python

"""
  script to download klines.
  set the absolute path destination folder for STORE_DIRECTORY, and run

  e.g. STORE_DIRECTORY=/data/ ./download-kline.py

"""
import sys

import pandas as pd

from enums import *
from utility import download_file, get_all_symbols, get_parser, convert_to_date_object, get_path


def download_monthly_klines(trading_type, symbols, num_symbols, intervals, years, months, start_date, end_date, folder, checksum):
    current = 0
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = pd.to_datetime(str(start_date))

    if not end_date:
        end_date = END_DATE
    else:
        end_date = pd.to_datetime(str(end_date))

    print("Found {} symbols".format(num_symbols))

    for symbol in symbols:
        print("[{}/{}] - start download monthly {} klines ".format(current + 1, num_symbols, symbol))
        for interval in intervals:
            for year in years:
                for month in months:
                    current_date = convert_to_date_object('{}-{}-01'.format(year, month))
                    if current_date >= start_date and current_date <= end_date:
                        path = get_path(trading_type, "klines", "monthly", symbol, interval)
                        file_name = "{}-{}-{}-{}.zip".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                        download_file(path, file_name, date_range, folder)

                        if checksum == 1:
                            checksum_path = get_path(trading_type, "klines", "monthly", symbol, interval)
                            checksum_file_name = "{}-{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, year, '{:02d}'.format(month))
                            download_file(checksum_path, checksum_file_name, date_range, folder)

        current += 1


def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum, njobs=1, overwrite=0):
    date_range = None

    if start_date and end_date:
        date_range = start_date + " " + end_date

    if not start_date:
        start_date = START_DATE
    else:
        start_date = convert_to_date_object(start_date)

    if not end_date:
        end_date = END_DATE
    else:
        end_date = convert_to_date_object(end_date)

    # Get valid intervals for daily
    intervals = list(set(intervals) & set(DAILY_INTERVALS))
    print("Found {} symbols".format(num_symbols))

    def download_one_symbol(symbol_i, symbol):
        print("[{}/{}] - start download daily {} klines ".format(symbol_i, num_symbols, symbol))
        for interval in intervals:
            print("[{}/{}] - start download daily {} klines for interval {}".format(symbol_i, num_symbols, symbol, interval))
            for date in dates:
                print("[{}/{}] - start download daily {} klines for date {}".format(symbol_i, num_symbols, symbol, date))
                current_date = convert_to_date_object(date)
                if current_date >= start_date and current_date <= end_date:
                    path = get_path(trading_type, "klines", "daily", symbol, interval)
                    file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, pd.to_datetime(date).strftime("%Y-%m-%d"))
                    download_file(path, file_name, date_range, folder, overwrite)

                    if checksum == 1:
                        checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
                        checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
                        download_file(checksum_path, checksum_file_name, date_range, folder, overwrite)

    from joblib import Parallel, delayed
    Parallel(n_jobs=njobs, backend="threading")(delayed(download_one_symbol)(symbol_i, symbol) for symbol_i, symbol in enumerate(symbols))


if __name__ == "__main__":
    parser = get_parser('klines')
    args = parser.parse_args(sys.argv[1:])

    if not args.symbols:
        print("fetching all symbols from exchange")
        symbols = get_all_symbols(args.type)
        num_symbols = len(symbols)
    else:
        symbols = args.symbols
        num_symbols = len(symbols)
    print("found {} symbols".format(num_symbols))

    if args.symbol_endswith:
        symbols = [symbol for symbol in symbols if symbol.endswith(args.symbol_endswith)]
        num_symbols = len(symbols)
    print("found {} symbols with suffix {}".format(num_symbols, args.symbol_endswith))

    dates = pd.date_range(args.startDate, args.endDate)

    if args.skip_daily == 0:
        download_daily_klines(args.type, symbols, num_symbols, args.intervals, dates,
                              args.startDate, args.endDate, args.folder, args.checksum, args.njobs, overwrite=args.overwrite)
