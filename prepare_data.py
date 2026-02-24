#!/usr/bin/env python3
"""
Prepare OHLC data from zip file sources for the SPA.

Data sources (all from zip files in ../data/):
1. DAX INDEX.zip - DAX Futures CSV files (combined into single source)
2. FTSE INDEX.zip - FTSE futures CSV files (combined into single source)
3. US DOW NASDAQ SP500.zip - Individual CSV files for DOW, Nasdaq, SP500

All CSV times are in London timezone (Europe/London).

Handles timezone differences:
- US instruments (DOW, Nasdaq, SP500): America/New_York, trading hours 9:30-16:00
- EU instruments (DAX Futures, FTSE): Europe/London, trading hours vary
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import zipfile
import io
from datetime import datetime, timedelta
import pytz

# Configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'data', 'ohlc_data.json')

# Zip files and their contents
ZIP_FILES = {
    'DAX INDEX.zip': {
        'sources': {
            'DAX Futures': {
                'file_pattern': 'DAX FUTURES',  # Match all DAX FUTURES*.csv
                'combine': True,  # Combine multiple CSVs into one source
                'timezone': 'Europe/London',
                'start_hour': 8, 'start_minute': 0,
                'end_hour': 17, 'end_minute': 0,
                'price_divisor': 1,
            }
        }
    },
    'FTSE INDEX.zip': {
        'sources': {
            'FTSE': {
                'file_pattern': 'FTSE',  # Match all FTSE*.csv
                'combine': True,
                'timezone': 'Europe/London',
                'start_hour': 8, 'start_minute': 0,
                'end_hour': 16, 'end_minute': 30,
                'price_divisor': 1,
            }
        }
    },
    'US DOW NASDAQ SP500.zip': {
        'sources': {
            'DOW': {
                'filename': 'Dow Jones from 2019 with volume.csv',
                'combine': False,
                'timezone': 'America/New_York',
                'start_hour': 9, 'start_minute': 30,
                'end_hour': 16, 'end_minute': 0,
                'price_divisor': 1,
            },
            'Nasdaq': {
                'filename': 'Nasdaq from 2019 with volume.csv',
                'combine': False,
                'timezone': 'America/New_York',
                'start_hour': 9, 'start_minute': 30,
                'end_hour': 16, 'end_minute': 0,
                'price_divisor': 100,  # Values are scaled by 100x
            },
            'SP500': {
                'filename': 'SP500 from 2019 with volume.csv',
                'combine': False,
                'timezone': 'America/New_York',
                'start_hour': 9, 'start_minute': 30,
                'end_hour': 16, 'end_minute': 0,
                'price_divisor': 100,  # Values are scaled by 100x
            },
        }
    },
}

# Order of sources in output
SOURCE_ORDER = ['DAX Futures', 'DOW', 'Nasdaq', 'SP500', 'FTSE']


def read_csvs_from_zip(zip_path, file_pattern=None, filename=None):
    """Read CSV file(s) from a zip archive.

    Args:
        zip_path: Path to the zip file
        file_pattern: Pattern to match multiple CSV files (for combined sources)
        filename: Exact filename for a single CSV file

    Returns:
        pandas DataFrame with combined data
    """
    all_data = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        csv_files = []

        if filename:
            # Find the exact file (may be in a subdirectory)
            for name in zf.namelist():
                if name.endswith(filename):
                    csv_files = [name]
                    break
            if not csv_files:
                print(f"  WARNING: File '{filename}' not found in {zip_path}")
                return pd.DataFrame()
        elif file_pattern:
            # Find all matching CSV files
            csv_files = sorted([
                name for name in zf.namelist()
                if name.endswith('.csv') and file_pattern in os.path.basename(name)
            ])

        if not csv_files:
            print(f"  WARNING: No matching files found in {zip_path}")
            return pd.DataFrame()

        for csv_file in csv_files:
            print(f"    Reading {os.path.basename(csv_file)}...", flush=True)
            with zf.open(csv_file) as f:
                try:
                    df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'))
                    # Drop rows where all OHLC values are NaN (trailing empty rows)
                    df = df.dropna(subset=['Open', 'High', 'Low', 'Close'], how='all')
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    print(f"    ERROR reading {csv_file}: {e}", flush=True)

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    print(f"    Total records: {len(combined)}", flush=True)
    return combined


def parse_london_datetime(df):
    """Parse Date and Time columns into London timezone-aware datetime.

    All CSV data uses London time (GMT/BST).
    """
    london_tz = pytz.timezone('Europe/London')

    # Format varies: MM/DD/YY or MM/DD/YYYY
    df = df.copy()
    df['DateTime'] = pd.to_datetime(
        df['Date'].astype(str) + ' ' + df['Time'].astype(str),
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


def apply_price_divisor(df, divisor):
    """Divide OHLC prices by a divisor to correct scaling."""
    if divisor != 1:
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = df[col] / divisor
    return df


def filter_trading_hours(df, config):
    """Filter data to trading hours based on instrument config."""
    tz = pytz.timezone(config['timezone'])

    # Convert to instrument's local timezone
    df = df.copy()
    df['local_time'] = df['DateTime'].dt.tz_convert(tz)

    start_minutes = config['start_hour'] * 60 + config['start_minute']
    end_minutes = config['end_hour'] * 60 + config['end_minute']

    mask = (
        (df['local_time'].dt.hour * 60 + df['local_time'].dt.minute >= start_minutes) &
        (df['local_time'].dt.hour * 60 + df['local_time'].dt.minute < end_minutes) &
        (df['local_time'].dt.weekday < 5)  # Monday-Friday only
    )

    result = df[mask].copy()
    result = result.drop(columns=['local_time'])
    return result


def aggregate_to_5min(df):
    """Aggregate 1-minute bars to 5-minute bars."""
    df = df.copy()
    df = df.set_index('DateTime')

    ohlc = df[['Open', 'High', 'Low', 'Close']].resample('5min').agg({
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
    """Calculate body-to-range ratio category for each bar."""
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

        # Convert OHLC to compact format: [timestamp_ms, open, high, low, close]
        bars = []
        for _, row in ohlc.iterrows():
            timestamp_ms = int(row['DateTime'].timestamp() * 1000)
            bars.append([
                timestamp_ms,
                float(round(row['Open'], 2)),
                float(round(row['High'], 2)),
                float(round(row['Low'], 2)),
                float(round(row['Close'], 2))
            ])

        bar_dirs = calculate_bar_directions(ohlc)
        body_ratios = calculate_body_ratios(ohlc)

        results.append({
            'date': day.strftime('%Y%m%d'),
            'prevClose': float(round(prev_day_stats['close'], 2)) if prev_day_stats else None,
            'prevHigh': float(round(prev_day_stats['high'], 2)) if prev_day_stats else None,
            'prevLow': float(round(prev_day_stats['low'], 2)) if prev_day_stats else None,
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
            'close': float(prev_close),
            'high': float(day_high),
            'low': float(day_low)
        }

    return results


def main():
    sys.stdout.reconfigure(line_buffering=True)

    print("=" * 60, flush=True)
    print("Multi-Source OHLC Data Preparation (from ZIP files)", flush=True)
    print("=" * 60, flush=True)

    output_data = {
        'metadata': {
            'generated': datetime.now().isoformat(),
            'baseFrequency': '5min',
            'sources': []
        },
        'sources': []
    }

    # Collect all processed sources
    all_sources = {}
    total_days = 0
    step = 0
    total_steps = sum(len(z['sources']) for z in ZIP_FILES.values())

    for zip_name, zip_config in ZIP_FILES.items():
        zip_path = os.path.join(DATA_DIR, zip_name)

        if not os.path.exists(zip_path):
            print(f"\nWARNING: {zip_path} not found, skipping", flush=True)
            continue

        print(f"\nProcessing {zip_name}...", flush=True)

        for source_name, source_config in zip_config['sources'].items():
            step += 1
            print(f"\n  [{step}/{total_steps}] {source_name}...", flush=True)

            # Read CSV data from zip
            if source_config['combine']:
                raw_data = read_csvs_from_zip(
                    zip_path,
                    file_pattern=source_config['file_pattern']
                )
            else:
                raw_data = read_csvs_from_zip(
                    zip_path,
                    filename=source_config['filename']
                )

            if raw_data.empty:
                print(f"  No data for {source_name}, skipping", flush=True)
                continue

            # Parse datetime (all times are London time)
            print(f"    Parsing timestamps...", flush=True)
            raw_data = parse_london_datetime(raw_data)

            # Apply price scaling
            raw_data = apply_price_divisor(raw_data, source_config['price_divisor'])

            # Sort and drop duplicate timestamps (from overlapping contracts)
            raw_data = raw_data.sort_values('DateTime')
            raw_data = raw_data.drop_duplicates(subset=['DateTime'], keep='first')
            print(f"    Unique 1-min bars: {len(raw_data)}", flush=True)

            # Process into trading days
            print(f"    Processing trading days...", flush=True)
            trading_days = process_source_data(raw_data, source_name, source_config)

            if trading_days:
                all_sources[source_name] = {
                    'name': source_name,
                    'timezone': source_config['timezone'],
                    'tradingHours': f"{source_config['start_hour']:02d}:{source_config['start_minute']:02d}-{source_config['end_hour']:02d}:{source_config['end_minute']:02d}",
                    'tradingDays': trading_days
                }
                total_days += len(trading_days)
                print(f"    {source_name}: {len(trading_days)} trading days", flush=True)

            # Free memory
            del raw_data

    # Output sources in defined order
    for source_name in SOURCE_ORDER:
        if source_name in all_sources:
            output_data['sources'].append(all_sources[source_name])
            output_data['metadata']['sources'].append(source_name)

    output_data['metadata']['totalSources'] = len(output_data['sources'])
    output_data['metadata']['totalTradingDays'] = total_days

    # Write JSON output
    print(f"\nWriting JSON output to {OUTPUT_FILE}...", flush=True)
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, separators=(',', ':'))

    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"Output file size: {file_size / 1024 / 1024:.2f} MB", flush=True)

    print("\n" + "=" * 60)
    print("Done!")
    print(f"Total sources: {len(output_data['sources'])}")
    print(f"Total trading days: {total_days}")
    for source in output_data['sources']:
        print(f"  - {source['name']}: {len(source['tradingDays'])} days ({source['timezone']})")
    print("=" * 60)


if __name__ == '__main__':
    main()
