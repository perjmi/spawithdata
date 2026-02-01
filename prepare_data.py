#!/usr/bin/env python3
"""
Prepare OHLC data from multiple sources for the SPA.
1. DAX Futures from CSV files (combined into single source)
2. Dukascopy instruments via DB_yield.py

Handles timezone differences:
- US instruments (DOW, Nasdaq, SP500): America/New_York, trading hours 9:30-16:00
- EU instruments (DAX, FTSE, DAX Futures): Europe/London, trading hours 8:00-16:30
"""

import pandas as pd
import numpy as np
import glob
import json
import os
import sys
from datetime import datetime, timedelta
import pytz

# Add parent dax directory to path for DB_yield import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dax'))

try:
    from DB_yield import Db
    DB_YIELD_AVAILABLE = True
except ImportError as e:
    print(f"Warning: DB_yield not available: {e}")
    DB_YIELD_AVAILABLE = False

# Configuration
DATA_DIR = '../data'
OUTPUT_FILE = 'data/ohlc_data.json'

# Dukascopy instruments to fetch
DUKAS_INSTRUMENTS = {
    'DOW': 'usa30idxusd',
    'Nasdaq': 'usatechidxusd',
    'SP500': 'usa500idxusd',
    'DAX': 'deuidxeur',
    'FTSE': 'gbridxgbp'
}

# Timezone and trading hours per instrument
INSTRUMENT_CONFIG = {
    # US instruments - New York timezone
    'usa30idxusd': {
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0
    },
    'usatechidxusd': {
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0
    },
    'usa500idxusd': {
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0
    },
    # EU instruments - London timezone
    'deuidxeur': {
        'timezone': 'Europe/London',
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 16, 'end_minute': 30
    },
    'gbridxgbp': {
        'timezone': 'Europe/London',
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 16, 'end_minute': 30
    },
    # DAX Futures - London timezone
    'dax_futures': {
        'timezone': 'Europe/London',
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 17, 'end_minute': 0
    }
}


def read_all_futures_csv(directory):
    """Read all DAX futures CSV files and combine into single DataFrame."""
    csv_files = glob.glob(os.path.join(directory, 'DAX FUTURES*.csv'))

    if not csv_files:
        print(f"No DAX FUTURES CSV files found in {directory}")
        return pd.DataFrame()

    all_data = []
    for csv_file in sorted(csv_files):
        print(f"  Reading {os.path.basename(csv_file)}...")
        df = pd.read_csv(csv_file)

        # Extract contract info for reference but combine all into one source
        filename = os.path.basename(csv_file)
        contract_name = filename.replace('.csv', '')
        df['contract'] = contract_name

        all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total futures records: {len(combined)}")

    return combined


def parse_futures_datetime(df):
    """Parse Date and Time columns for futures data into timezone-aware datetime."""
    london_tz = pytz.timezone('Europe/London')

    # Format varies: MM/DD/YY or MM/DD/YYYY
    df['DateTime'] = pd.to_datetime(
        df['Date'] + ' ' + df['Time'],
        format='mixed',
        dayfirst=False
    )

    # Localize to London time
    df['DateTime'] = df['DateTime'].dt.tz_localize(
        london_tz,
        ambiguous='NaT',
        nonexistent='NaT'
    )

    df = df.dropna(subset=['DateTime'])
    return df


def filter_trading_hours(df, config):
    """Filter data to trading hours based on instrument config."""
    tz = pytz.timezone(config['timezone'])

    # Convert to local timezone if needed
    if df['DateTime'].dt.tz is not None:
        df = df.copy()
        df['local_time'] = df['DateTime'].dt.tz_convert(tz)
    else:
        df['local_time'] = df['DateTime']

    start_minutes = config['start_hour'] * 60 + config['start_minute']
    end_minutes = config['end_hour'] * 60 + config['end_minute']

    mask = (
        (df['local_time'].dt.hour * 60 + df['local_time'].dt.minute >= start_minutes) &
        (df['local_time'].dt.hour * 60 + df['local_time'].dt.minute < end_minutes) &
        (df['local_time'].dt.weekday < 5)  # Monday-Friday only
    )

    result = df[mask].copy()
    if 'local_time' in result.columns:
        result = result.drop(columns=['local_time'])

    return result


def aggregate_to_5min(df):
    """Aggregate 1-minute bars to 5-minute bars."""
    df = df.copy()
    df = df.set_index('DateTime')

    ohlc = df.resample('5min').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    }).dropna()

    ohlc = ohlc.reset_index()
    return ohlc


def classify_gap_size(pct_change):
    """Classify gap size into categories."""
    abs_pct = abs(pct_change)
    if abs_pct < 0.1:
        return '0-0.1%'
    elif abs_pct < 0.25:
        return '0.1%-0.25%'
    elif abs_pct < 0.5:
        return '0.25%-0.5%'
    elif abs_pct < 1.0:
        return '0.5%-1.0%'
    else:
        return '1.0%+'


def calculate_bar_directions(ohlc_df):
    """Calculate direction (UP/DOWN/FLAT) for each bar."""
    directions = []
    for _, row in ohlc_df.iterrows():
        if row['Close'] > row['Open']:
            directions.append('UP')
        elif row['Close'] < row['Open']:
            directions.append('DOWN')
        else:
            directions.append('FLAT')
    return directions


def calculate_body_ratios(ohlc_df):
    """Calculate body-to-range ratio for each bar.

    Body = abs(close - open)
    Range = high - low
    Ratio = body / range * 100

    Categories:
    - '<25%': Small body (doji-like)
    - '25-50%': Medium-small body
    - '50-75%': Medium-large body
    - '>75%': Large body (marubozu-like)
    """
    ratios = []
    for _, row in ohlc_df.iterrows():
        high_low_range = row['High'] - row['Low']
        if high_low_range > 0:
            body = abs(row['Close'] - row['Open'])
            ratio = (body / high_low_range) * 100

            if ratio < 25:
                ratios.append('<25%')
            elif ratio < 50:
                ratios.append('25-50%')
            elif ratio < 75:
                ratios.append('50-75%')
            else:
                ratios.append('>75%')
        else:
            # No range (same high/low) - treat as small body
            ratios.append('<25%')
    return ratios


def process_source_data(data, source_name, config):
    """Process data for a single source and return trading days with OHLC data."""
    data = data.copy()

    # Get timezone for this source
    tz = pytz.timezone(config['timezone'])

    # Convert to local timezone for grouping by trading day
    data['local_time'] = data['DateTime'].dt.tz_convert(tz)
    data['trading_date'] = data['local_time'].dt.date

    trading_days = sorted(data['trading_date'].unique())

    results = []
    prev_day_stats = None

    for day in trading_days:
        day_data = data[data['trading_date'] == day].copy()

        # Filter to trading hours
        day_data = filter_trading_hours(day_data, config)

        if len(day_data) < 10:  # Need at least 10 1-min bars
            continue

        # Aggregate to 5-minute bars
        ohlc = aggregate_to_5min(day_data)

        if len(ohlc) < 5:  # Need at least 5 5-min bars
            continue

        # Calculate day statistics
        day_open = ohlc.iloc[0]['Open']
        day_high = ohlc['High'].max()
        day_low = ohlc['Low'].min()
        day_close = ohlc.iloc[-1]['Close']

        # Get close near end of day for prev_close calculation
        end_hour = config['end_hour']
        close_bars = ohlc[ohlc['DateTime'].dt.tz_convert(tz).dt.hour == end_hour - 1]
        if len(close_bars) > 0:
            prev_close = close_bars.iloc[-1]['Close']
        else:
            prev_close = day_close

        # Calculate gap characteristics
        gap_direction = 'N/A'
        gap_size_class = 'N/A'
        open_above_prev_high = None
        close_below_prev_low = None

        if prev_day_stats is not None:
            # Check days gap (allow up to 5 days for weekends/holidays)
            days_diff = (day - prev_day_stats['date']).days
            if days_diff <= 5:
                prev_close_val = prev_day_stats['close']
                gap_pct = ((day_open - prev_close_val) / prev_close_val) * 100

                if day_open > prev_close_val:
                    gap_direction = 'GAP UP'
                elif day_open < prev_close_val:
                    gap_direction = 'GAP DOWN'
                else:
                    gap_direction = 'FLAT'

                gap_size_class = classify_gap_size(gap_pct)

                # Previous day comparison
                open_above_prev_high = bool(day_open > prev_day_stats['high'])
                close_below_prev_low = bool(day_close < prev_day_stats['low'])

        # Convert OHLC to compact format: [timestamp_ms, open, high, low, close]
        bars = []
        for _, row in ohlc.iterrows():
            timestamp_ms = int(row['DateTime'].timestamp() * 1000)
            bars.append([
                timestamp_ms,
                round(row['Open'], 2),
                round(row['High'], 2),
                round(row['Low'], 2),
                round(row['Close'], 2)
            ])

        # Calculate bar directions and body ratios
        bar_dirs = calculate_bar_directions(ohlc)
        body_ratios = calculate_body_ratios(ohlc)

        results.append({
            'date': day.strftime('%Y%m%d'),
            'prevClose': round(prev_day_stats['close'], 2) if prev_day_stats else None,
            'prevHigh': round(prev_day_stats['high'], 2) if prev_day_stats else None,
            'prevLow': round(prev_day_stats['low'], 2) if prev_day_stats else None,
            'gapDirection': gap_direction,
            'gapSizeClass': gap_size_class,
            'openAbovePrevHigh': open_above_prev_high,
            'closeBelowPrevLow': close_below_prev_low,
            'bars': bars,
            'barDirections': bar_dirs,
            'bodyRatios': body_ratios
        })

        # Store for next iteration
        prev_day_stats = {
            'date': day,
            'close': prev_close,
            'high': day_high,
            'low': day_low
        }

    return results


def fetch_and_process_dukascopy_data(instrument_code, display_name, start_date, end_date, config):
    """Fetch data from Dukascopy year by year and process immediately to save memory."""
    if not DB_YIELD_AVAILABLE:
        print(f"  Skipping {display_name}: DB_yield not available", flush=True)
        return []

    print(f"  Fetching {display_name} ({instrument_code}) from Dukascopy...", flush=True)

    all_trading_days = []
    prev_day_stats = None
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    for year in range(start_year, end_year + 1):
        year_start = f"{year}-01-01"
        year_end = f"{year}-12-31"

        print(f"    Fetching {year}...", flush=True)

        try:
            db = Db(
                instrument=instrument_code,
                start_date=year_start,
                end_date=year_end,
                freq='5min',
                method='dukascopy'
            )

            data = db.dataslice(datefrom=year_start, dateto=year_end, delvolume=True)

            if data is None or data.empty:
                print(f"    No data for {year}", flush=True)
                continue

            # Rename columns to match our format
            data = data.rename(columns={
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close'
            })

            # Use utc_time as DateTime
            if 'utc_time' in data.columns:
                data['DateTime'] = data['utc_time']
                data = data.drop(columns=['utc_time', 'timestamp'], errors='ignore')

            print(f"    {year}: {len(data)} records", flush=True)

            # Process this year's data immediately
            year_days, prev_day_stats = process_year_data(data, config, prev_day_stats)
            all_trading_days.extend(year_days)
            print(f"    {year}: {len(year_days)} trading days processed", flush=True)

            # Free memory
            del data

        except Exception as e:
            print(f"    Error fetching {year}: {e}", flush=True)
            continue

    print(f"  Total: {len(all_trading_days)} trading days for {display_name}", flush=True)
    return all_trading_days


def process_year_data(data, config, prev_day_stats):
    """Process a single year's data and return trading days."""
    tz = pytz.timezone(config['timezone'])

    # Convert to local timezone for grouping by trading day
    data = data.copy()
    data['local_time'] = data['DateTime'].dt.tz_convert(tz)
    data['trading_date'] = data['local_time'].dt.date

    trading_days = sorted(data['trading_date'].unique())
    results = []

    for day in trading_days:
        day_data = data[data['trading_date'] == day].copy()

        # Filter to trading hours
        day_data = filter_trading_hours(day_data, config)

        if len(day_data) < 10:
            continue

        # Aggregate to 5-minute bars
        ohlc = aggregate_to_5min(day_data)

        if len(ohlc) < 5:
            continue

        # Calculate day statistics
        day_open = ohlc.iloc[0]['Open']
        day_high = ohlc['High'].max()
        day_low = ohlc['Low'].min()
        day_close = ohlc.iloc[-1]['Close']

        # Get close near end of day
        end_hour = config['end_hour']
        close_bars = ohlc[ohlc['DateTime'].dt.tz_convert(tz).dt.hour == end_hour - 1]
        if len(close_bars) > 0:
            prev_close = close_bars.iloc[-1]['Close']
        else:
            prev_close = day_close

        # Calculate gap characteristics
        gap_direction = 'N/A'
        gap_size_class = 'N/A'
        open_above_prev_high = None
        close_below_prev_low = None

        if prev_day_stats is not None:
            days_diff = (day - prev_day_stats['date']).days
            if days_diff <= 5:
                prev_close_val = prev_day_stats['close']
                gap_pct = ((day_open - prev_close_val) / prev_close_val) * 100

                if day_open > prev_close_val:
                    gap_direction = 'GAP UP'
                elif day_open < prev_close_val:
                    gap_direction = 'GAP DOWN'
                else:
                    gap_direction = 'FLAT'

                gap_size_class = classify_gap_size(gap_pct)
                open_above_prev_high = bool(day_open > prev_day_stats['high'])
                close_below_prev_low = bool(day_close < prev_day_stats['low'])

        # Convert OHLC to compact format
        bars = []
        for _, row in ohlc.iterrows():
            timestamp_ms = int(row['DateTime'].timestamp() * 1000)
            bars.append([
                timestamp_ms,
                round(row['Open'], 2),
                round(row['High'], 2),
                round(row['Low'], 2),
                round(row['Close'], 2)
            ])

        bar_dirs = calculate_bar_directions(ohlc)
        body_ratios = calculate_body_ratios(ohlc)

        results.append({
            'date': day.strftime('%Y%m%d'),
            'prevClose': round(prev_day_stats['close'], 2) if prev_day_stats else None,
            'prevHigh': round(prev_day_stats['high'], 2) if prev_day_stats else None,
            'prevLow': round(prev_day_stats['low'], 2) if prev_day_stats else None,
            'gapDirection': gap_direction,
            'gapSizeClass': gap_size_class,
            'openAbovePrevHigh': open_above_prev_high,
            'closeBelowPrevLow': close_below_prev_low,
            'bars': bars,
            'barDirections': bar_dirs,
            'bodyRatios': body_ratios
        })

        prev_day_stats = {
            'date': day,
            'close': prev_close,
            'high': day_high,
            'low': day_low
        }

    return results, prev_day_stats


def main():
    import sys
    # Force unbuffered output
    sys.stdout.reconfigure(line_buffering=True)

    print("=" * 60, flush=True)
    print("Multi-Source OHLC Data Preparation", flush=True)
    print("=" * 60, flush=True)

    output_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'baseFrequency': '5min',
            'sources': []
        },
        'sources': []
    }

    total_days = 0

    # ========== DAX FUTURES (combined) ==========
    print("\n[1/2] Processing DAX Futures (combined from all contracts)...")

    futures_data = read_all_futures_csv(DATA_DIR)

    if not futures_data.empty:
        # Parse datetime
        futures_data = parse_futures_datetime(futures_data)

        # Rename columns
        futures_data = futures_data.rename(columns={
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close'
        })

        # Sort by datetime
        futures_data = futures_data.sort_values('DateTime')

        # Process as single source
        config = INSTRUMENT_CONFIG['dax_futures']
        trading_days = process_source_data(futures_data, 'DAX Futures', config)

        if trading_days:
            output_data['sources'].append({
                'name': 'DAX Futures',
                'timezone': config['timezone'],
                'tradingHours': f"{config['start_hour']:02d}:{config['start_minute']:02d}-{config['end_hour']:02d}:{config['end_minute']:02d}",
                'tradingDays': trading_days
            })
            output_data['metadata']['sources'].append('DAX Futures')
            total_days += len(trading_days)
            print(f"  DAX Futures: {len(trading_days)} trading days")

    # ========== DUKASCOPY INSTRUMENTS ==========
    print("\n[2/2] Processing Dukascopy instruments...")

    start_date = '2020-01-01'
    end_date = '2025-12-31'

    for display_name, instrument_code in DUKAS_INSTRUMENTS.items():
        print(f"\n  Processing {display_name}...", flush=True)

        config = INSTRUMENT_CONFIG[instrument_code]
        trading_days = fetch_and_process_dukascopy_data(instrument_code, display_name, start_date, end_date, config)

        if trading_days:
            output_data['sources'].append({
                'name': display_name,
                'timezone': config['timezone'],
                'tradingHours': f"{config['start_hour']:02d}:{config['start_minute']:02d}-{config['end_hour']:02d}:{config['end_minute']:02d}",
                'tradingDays': trading_days
            })
            output_data['metadata']['sources'].append(display_name)
            total_days += len(trading_days)
            print(f"  {display_name}: {len(trading_days)} trading days")

    output_data['metadata']['totalSources'] = len(output_data['sources'])
    output_data['metadata']['totalTradingDays'] = total_days

    # Write JSON output
    print(f"\n[3/3] Writing JSON output to {OUTPUT_FILE}...")

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, separators=(',', ':'))

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"Output file size: {file_size / 1024 / 1024:.2f} MB")

    print("\n" + "=" * 60)
    print("Done!")
    print(f"Total sources: {len(output_data['sources'])}")
    print(f"Total trading days: {total_days}")
    for source in output_data['sources']:
        print(f"  - {source['name']}: {len(source['tradingDays'])} days ({source['timezone']})")
    print("=" * 60)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Prepare OHLC data for SPA')
    parser.add_argument('--futures-only', action='store_true',
                        help='Only process DAX Futures (skip Dukascopy)')
    args = parser.parse_args()

    if args.futures_only:
        # Quick mode - only DAX Futures
        print("=" * 60, flush=True)
        print("Quick Mode: DAX Futures Only", flush=True)
        print("=" * 60, flush=True)

        output_data = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'baseFrequency': '5min',
                'sources': []
            },
            'sources': []
        }

        print("\nProcessing DAX Futures...", flush=True)
        futures_data = read_all_futures_csv(DATA_DIR)

        if not futures_data.empty:
            futures_data = parse_futures_datetime(futures_data)
            futures_data = futures_data.rename(columns={
                'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close'
            })
            futures_data = futures_data.sort_values('DateTime')

            config = INSTRUMENT_CONFIG['dax_futures']
            trading_days = process_source_data(futures_data, 'DAX Futures', config)

            if trading_days:
                output_data['sources'].append({
                    'name': 'DAX Futures',
                    'timezone': config['timezone'],
                    'tradingHours': f"{config['start_hour']:02d}:{config['start_minute']:02d}-{config['end_hour']:02d}:{config['end_minute']:02d}",
                    'tradingDays': trading_days
                })
                output_data['metadata']['sources'].append('DAX Futures')
                print(f"  DAX Futures: {len(trading_days)} trading days", flush=True)

        output_data['metadata']['totalSources'] = len(output_data['sources'])
        output_data['metadata']['totalTradingDays'] = len(trading_days) if trading_days else 0

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(output_data, f, separators=(',', ':'))

        file_size = os.path.getsize(OUTPUT_FILE)
        print(f"\nOutput file: {OUTPUT_FILE} ({file_size / 1024 / 1024:.2f} MB)", flush=True)
        print("Done!", flush=True)
    else:
        main()
