#!/usr/bin/env python3
"""
Prepare OHLC data using DB_yield.py with Dukascopy as the data source.

Fetches raw tick data via dukascopy-node, aggregates to 5-min bars,
filters to trading hours, and outputs data/ohlc_data.json for the SPA.
"""

import pandas as pd
import json
import os
import sys
from datetime import datetime, timedelta
import pytz
from DB_yield import Db

# Configuration
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'data', 'ohlc_data.json')
CHUNK_DAYS = 30
END_DATE = datetime.now().strftime('%Y-%m-%d')

SOURCES = [
    {
        'name': 'DAX',
        'instrument': 'deuidxeur',
        'timezone': 'Europe/London',
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 16, 'end_minute': 30,
        'start_date': '2019-01-01',
    },
    {
        'name': 'DOW',
        'instrument': 'usa30idxusd',
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0,
        'start_date': '2019-01-01',
    },
    {
        'name': 'Nasdaq',
        'instrument': 'usatechidxusd',
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0,
        'start_date': '2019-01-01',
    },
    {
        'name': 'SP500',
        'instrument': 'usa500idxusd',
        'timezone': 'America/New_York',
        'start_hour': 9, 'start_minute': 30,
        'end_hour': 16, 'end_minute': 0,
        'start_date': '2019-01-01',
    },
    {
        'name': 'FTSE',
        'instrument': 'gbridxgbp',
        'timezone': 'Europe/London',
        'start_hour': 8, 'start_minute': 0,
        'end_hour': 16, 'end_minute': 30,
        'start_date': '2019-01-01',
    },
]


def filter_trading_hours(df, config):
    """Filter 5-min bars to trading hours in the instrument's local timezone."""
    tz = pytz.timezone(config['timezone'])
    df = df.copy()
    # aggregate() produces a naive UTC index — localize then convert
    df.index = df.index.tz_localize('UTC').tz_convert(tz)

    start_minutes = config['start_hour'] * 60 + config['start_minute']
    end_minutes = config['end_hour'] * 60 + config['end_minute']

    mask = (
        (df.index.hour * 60 + df.index.minute >= start_minutes) &
        (df.index.hour * 60 + df.index.minute < end_minutes) &
        (df.index.weekday < 5)
    )
    return df[mask]


def classify_gap_size(pct_change):
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


def process_trading_days(ohlc_5min, config, initial_prev_day_stats=None):
    """Convert filtered 5-min OHLC into per-day entries matching the SPA JSON schema."""
    ohlc_5min = ohlc_5min.copy()
    ohlc_5min['trading_date'] = ohlc_5min.index.date
    trading_days = sorted(ohlc_5min['trading_date'].unique())

    results = []
    prev_day_stats = initial_prev_day_stats

    for day in trading_days:
        day_data = ohlc_5min[ohlc_5min['trading_date'] == day].drop(columns=['trading_date'])

        if len(day_data) < 5:
            continue

        day_open = day_data.iloc[0]['open']
        day_high = day_data['high'].max()
        day_low = day_data['low'].min()
        day_close = day_data.iloc[-1]['close']

        if prev_day_stats is None:
            prev_day_stats = {
                'date': day,
                'close': float(day_close),
                'high': float(day_high),
                'low': float(day_low),
            }
            continue

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

        bars = []
        bar_directions = []
        body_ratios = []

        for idx, row in day_data.iterrows():
            timestamp_ms = int(idx.timestamp() * 1000)
            o = float(round(row['open'], 2))
            h = float(round(row['high'], 2))
            l = float(round(row['low'], 2))
            c = float(round(row['close'], 2))
            bars.append([timestamp_ms, o, h, l, c])

            # Direction
            if c > o:
                bar_directions.append('UP')
            elif c < o:
                bar_directions.append('DOWN')
            else:
                bar_directions.append('FLAT')

            # Body ratio
            hl_range = h - l
            if hl_range > 0:
                ratio = (abs(c - o) / hl_range) * 100
                if ratio < 25:
                    body_ratios.append('<25%')
                elif ratio < 50:
                    body_ratios.append('25-50%')
                elif ratio < 75:
                    body_ratios.append('50-75%')
                else:
                    body_ratios.append('>75%')
            else:
                body_ratios.append('<25%')

        results.append({
            'date': day.strftime('%Y%m%d'),
            'prevClose': float(round(prev_day_stats['close'], 2)),
            'prevHigh': float(round(prev_day_stats['high'], 2)),
            'prevLow': float(round(prev_day_stats['low'], 2)),
            'gapDirection': gap_direction,
            'gapSizeClass': gap_size_class,
            'openAbovePrevHigh': open_above_prev_high,
            'closeBelowPrevLow': close_below_prev_low,
            'bars': bars,
            'barDirections': bar_directions,
            'bodyRatios': body_ratios,
        })

        prev_day_stats = {
            'date': day,
            'close': float(day_close),
            'high': float(day_high),
            'low': float(day_low),
        }

    return results


def load_existing_data():
    """Load existing ohlc_data.json if present, return data and per-source last-date map."""
    if not os.path.exists(OUTPUT_FILE):
        return None, {}

    try:
        with open(OUTPUT_FILE, 'r') as f:
            data = json.load(f)

        last_dates = {}
        for source in data.get('sources', []):
            days = source.get('tradingDays', [])
            if days:
                last_date_str = days[-1]['date']  # YYYYMMDD
                last_dates[source['name']] = last_date_str

        return data, last_dates
    except Exception as e:
        print(f"  Warning: could not load existing data: {e}", flush=True)
        return None, {}


def main():
    sys.stdout.reconfigure(line_buffering=True)

    print("=" * 60)
    print("OHLC Data Preparation (Dukascopy via DB_yield)")
    print("=" * 60)

    existing_data, last_dates = load_existing_data()

    if existing_data:
        print(f"Found existing data with {len(existing_data.get('sources', []))} sources")
        for name, last_date in last_dates.items():
            print(f"  {name}: last date {last_date}")
        output_data = existing_data
    else:
        print("No existing data found, building from scratch")
        output_data = {
            'metadata': {
                'generated': datetime.now().isoformat(),
                'baseFrequency': '5min',
                'sources': [],
            },
            'sources': [],
        }

    # Build a lookup for existing sources
    existing_sources = {s['name']: s for s in output_data.get('sources', [])}

    for i, config in enumerate(SOURCES):
        source_name = config['name']
        print(f"\n[{i+1}/{len(SOURCES)}] {source_name} ({config['instrument']})...", flush=True)

        # Determine start date: day after last existing date, or config start
        if source_name in last_dates:
            last_dt = datetime.strptime(last_dates[source_name], '%Y%m%d')
            fetch_start = (last_dt + timedelta(days=1)).strftime('%Y-%m-%d')
            if fetch_start >= END_DATE:
                print(f"  Already up to date (last: {last_dates[source_name]})", flush=True)
                continue
            print(f"  Incremental fetch from {fetch_start}", flush=True)
        else:
            fetch_start = config['start_date']
            print(f"  Full fetch from {fetch_start}", flush=True)

        db = Db(
            instrument=config['instrument'],
            start_date=fetch_start,
            end_date=END_DATE,
            freq='5min',
            method='dukascopy',
        )

        # Fetch in chunks to keep memory reasonable
        start = pd.to_datetime(fetch_start)
        end = pd.to_datetime(END_DATE)
        all_ohlc = []
        current = start

        while current < end:
            chunk_end = min(current + timedelta(days=CHUNK_DAYS), end)
            print(f"  Fetching {current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}...", flush=True)

            try:
                raw = db.dataslice(datefrom=current, dateto=chunk_end, delvolume=True)
                if raw is not None and len(raw) > 0:
                    ohlc = db.aggregate(raw)
                    all_ohlc.append(ohlc)
            except Exception as e:
                print(f"    Error: {e}", flush=True)

            current = chunk_end

        if not all_ohlc:
            print(f"  No new data for {source_name}", flush=True)
            continue

        # Combine chunks, deduplicate, sort
        combined = pd.concat(all_ohlc)
        combined = combined[~combined.index.duplicated(keep='first')]
        combined = combined.sort_index()

        # Filter to trading hours
        filtered = filter_trading_hours(combined, config)
        print(f"  5-min bars after filtering: {len(filtered)}", flush=True)

        # Get prev_day_stats from existing data for gap calculation continuity
        prev_day_stats_for_new = None
        if source_name in existing_sources:
            existing_days = existing_sources[source_name].get('tradingDays', [])
            if existing_days:
                last_day = existing_days[-1]
                last_bars = last_day['bars']
                if last_bars:
                    last_bar = last_bars[-1]
                    day_bars_open = last_bars[0][1]
                    day_high = max(b[2] for b in last_bars)
                    day_low = min(b[3] for b in last_bars)
                    day_close = last_bar[4]
                    prev_day_stats_for_new = {
                        'date': datetime.strptime(last_day['date'], '%Y%m%d').date(),
                        'close': day_close,
                        'high': day_high,
                        'low': day_low,
                    }

        # Process into trading days
        trading_days = process_trading_days(filtered, config, prev_day_stats_for_new)

        if trading_days:
            if source_name in existing_sources:
                # Append new days to existing source
                existing_sources[source_name]['tradingDays'].extend(trading_days)
                print(f"  Appended {len(trading_days)} new days (total: {len(existing_sources[source_name]['tradingDays'])})", flush=True)
            else:
                # New source
                new_source = {
                    'name': source_name,
                    'timezone': config['timezone'],
                    'tradingHours': f"{config['start_hour']:02d}:{config['start_minute']:02d}-{config['end_hour']:02d}:{config['end_minute']:02d}",
                    'tradingDays': trading_days,
                }
                output_data['sources'].append(new_source)
                existing_sources[source_name] = new_source
                if source_name not in output_data['metadata']['sources']:
                    output_data['metadata']['sources'].append(source_name)
                print(f"  Added new source with {len(trading_days)} days", flush=True)

        # Free memory
        del combined, filtered, all_ohlc

    # Update metadata
    total_days = sum(len(s.get('tradingDays', [])) for s in output_data['sources'])
    output_data['metadata']['totalSources'] = len(output_data['sources'])
    output_data['metadata']['totalTradingDays'] = total_days
    output_data['metadata']['generated'] = datetime.now().isoformat()
    output_data['metadata']['sources'] = [s['name'] for s in output_data['sources']]

    # Write JSON output
    print(f"\nWriting JSON output to {OUTPUT_FILE}...", flush=True)
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
    main()
