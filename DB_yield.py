import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz
import subprocess
import json
import os
import time
import numpy as np

class Db:
    def __init__(self,instrument='usatechidxusd',start_date = '2021-04-01',end_date = '2024-02-20',freq='5min', method='sqlite'):
        self.method=method
        self.instrument=instrument
        self.start_date=start_date
        self.end_date=end_date
        self.freq=freq
        self.instrumentstart = {
            "ussc2000idxusd": 2019,
            "usa30idxusd": 2014,
            "usatechidxusd": 2014,
            "usa500idxusd": 2014,
            "fraidxeur": 2014,
            "deuidxeur": 2014,
            "gbridxgbp": 2014,
            "eurusd": 2014,
            "audusd": 2014,
            "gbpusd": 2014,
            "usdjpy": 2014,
            "eurgbp": 2014,
            "chiidxusd": 2018,
            "hkgidxhkd": 2014,
            "jpnidxjpy": 2014,
            "ausidxaud": 2015,
            "indidxusd": 2018,
            "sgdidxsgd": 2018,
            "xauusd": 2014
        }


    def instrument2text(self):
        index_dict = {
            "ussc2000idxusd": "USA Small Cap 2000",
            "usa30idxusd": "USA 30",
            "usatechidxusd": "USA 100 Technical",
            "usa500idxusd": "USA 500",
            "fraidxeur": "France 40",
            "deuidxeur": "Germany 40",
            "gbridxgbp": "UK 100",
            "espidxeur": "Spain 35",
            "cheidxchf": "Switzerland 20",
            "itaidxeur": "Italy 40",
            "nldidxeur": "Netherland 25",
            "plnidxpln": "Poland 20",
            "audusd": "audusd",
            "eurusd": "eurusd",
            "gbpusd": "gbpusd",
            "usdjpy": "usdjpy",
            "eurgbp": "eurgbp",
            "chiidxusd": "China A50",
            "hkgidxhkd": "Hong Kong 40",
            "jpnidxjpy": "Japan 225",
            "ausidxaud": "Australia 200",
            "indidxusd": "India 50",
            "sgdidxsgd": "Singapore Blue Chip",
            "btcusd": "Bitcoin vs US Dollar",
            "ethusd": "Ether vs US Dollar",
            "xauusd": "Spot gold"
        }
        return index_dict[self.instrument]

    def dataslice(self, datefrom=None, dateto=None, delvolume=True, csv_path=None):
        # If a csv_path is provided, load OHLC data from the CSV file
        if csv_path is not None:
            df = pd.read_csv(csv_path, parse_dates=True, index_col=0)
            # Optionally filter by date range if provided
            if datefrom is not None:
                datefrom = pd.to_datetime(datefrom)
                df = df[df.index >= datefrom]
            if dateto is not None:
                dateto = pd.to_datetime(dateto)
                df = df[df.index <= dateto]
            if delvolume and 'volume' in df.columns:
                df = df.drop(columns=['volume'])
            return df

        # Use loaded OHLC data from CSV if available
        if hasattr(self, 'ohlc_data') and self.ohlc_data is not None:
            df = self.ohlc_data.copy()
            # Optionally filter by date range if provided
            if datefrom is not None:
                datefrom = pd.to_datetime(datefrom)
                df = df[df.index >= datefrom]
            if dateto is not None:
                dateto = pd.to_datetime(dateto)
                df = df[df.index <= dateto]
            if delvolume and 'volume' in df.columns:
                df = df.drop(columns=['volume'])
            return df

        if datefrom is None:
            datefrom = self.start_date
        if dateto is None:
            dateto = self.end_date
        # Convert datefrom and dateto to pandas datetime if they are strings or datetime objects
        if isinstance(datefrom, (str, datetime)):
            datefrom = pd.to_datetime(datefrom)
        if isinstance(dateto, (str, datetime)):
            dateto = pd.to_datetime(dateto)

        if self.method == 'dukascopy':
            # Convert datefrom and dateto to strings in the format 'YYYY-MM-DD'
            datefrom_str = datefrom.strftime('%Y-%m-%d')
            dateto_str = dateto.strftime('%Y-%m-%d')

            # Generate a unique directory name
            unique_dir = f"/data/download/{self.instrument}_{int(time.time())}_{np.random.randint(100000, 999999)}"
            os.makedirs(unique_dir, exist_ok=True)

            # Construct the expected filename
            expected_filename = f"{self.instrument}-s1-bid-{datefrom_str}-{dateto_str}.json"
            filepath = os.path.join(unique_dir, expected_filename)

            # Use dukascopy-node CLI to fetch data
            command = [
                'npx', 'dukascopy-node',
                '--instrument', self.instrument,
                '-from', datefrom_str,
                '-to', dateto_str,
                '--format', 'json',
                '-t', 's1',
                '-v',
                '-ch',
                '-chpath', '/data/.dukascopy-cache',
                '-dir', unique_dir
            ]

            # Execute the dukascopy-node command
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode != 0:
                print(result)
                raise Exception(f"Error fetching data: {result.stderr}")

            # Check if the expected file exists
            if not os.path.exists(filepath):
                raise Exception(f"Expected JSON file {expected_filename} not found in the directory {unique_dir}.")
            
            # Load the data from the JSON file
            #print("Loading data from", filepath)
            data = pd.read_json(filepath)
            #print("Data loaded successfully.")


            # Convert the timestamp column to datetime with error handling
            #data['utc_time'] = pd.to_datetime(data['timestamp'], unit='ms', errors='coerce')
            #invalid_rows = data['utc_time'].isna()
            #if invalid_rows.any():
            #    print(f"Warning: Skipping {invalid_rows.sum()} rows with invalid timestamps.")
            #    data = data[~invalid_rows]

            # Delete the JSON file and the unique directory
            os.remove(filepath)
            os.rmdir(unique_dir)
        elif self.method == 'sqlite':
            self.conn=sqlite3.connect("/data/data.db",cached_statements=10)
            self.cursor=self.conn.cursor()
            # Convert datetime to Unix timestamp in milliseconds
            unixstart = datefrom.round('D').timestamp() * 1000
            unixslut = (dateto.round('D') + pd.Timedelta(days=1)).round('D').timestamp() * 1000

            sql = "select * from " + self.instrument + " where timestamp>" + str(round(unixstart)) + " and timestamp<=" + str(round(unixslut))
            res = self.cursor.execute(sql)
            cols = [column[0] for column in res.description]
            data = pd.DataFrame.from_records(data=res.fetchall(), columns=cols)
        elif self.method == 'download':
            downloadflag=True
            # Generate a unique directory name for downloads
            unique_dir = f"download_{self.instrument}_{int(time.time())}_{np.random.randint(100000, 999999)}"
            os.makedirs(unique_dir, exist_ok=True)
            filename = f"{self.instrument}.db"
            filepath = os.path.join(unique_dir, filename)
            if os.path.exists(filepath):
                creationtime=os.path.getctime(filepath)
                if creationtime>datetime.now().timestamp()-86400:
                    downloadflag=False
            if downloadflag:
                #print("Downloading data")
                com = f"rclone copy jottacloud:tradedata/db/{filename}.gz {unique_dir}/"
                os.system(com)
                com = f"gunzip -f {os.path.join(unique_dir, filename)}.gz"
                os.system(com)
            self.conn=sqlite3.connect(filepath,cached_statements=10)
            self.cursor=self.conn.cursor()
            unixstart = datefrom.round('D').timestamp() * 1000
            unixslut = (dateto.round('D') + pd.Timedelta(days=1)).round('D').timestamp() * 1000
            sql = "select * from ohlc where timestamp>" + str(round(unixstart)) + " and timestamp<=" + str(round(unixslut))
            res = self.cursor.execute(sql)
            cols = [column[0] for column in res.description]
            data = pd.DataFrame.from_records(data=res.fetchall(), columns=cols)
        else:
            raise ValueError("Invalid method. Choose 'dukascopy', 'download' or 'sqlite'.")

        # Convert timestamps and other columns to appropriate types
        #print("Data shape is",data.shape," for ",self.instrument," from ",datefrom," to ",dateto," with method ",self.method)
        data = data.sort_values(by='timestamp')
        try:
            data['utc_time'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC')
        except Exception as e:
            print(f"Error with direct conversion: {e}. Trying alternative method.")
            data['utc_time'] = pd.to_datetime(data['timestamp'].astype(float), unit='ms').dt.tz_localize('UTC')
        data['open'] = data['open'].astype(float)
        data['high'] = data['high'].astype(float)
        data['low'] = data['low'].astype(float)
        data['close'] = data['close'].astype(float)
        data['volume'] = data['volume'].astype(float)
        if delvolume:
            del data['volume']
        return data

    def is_market_open_old(self,dt: datetime,convertto='Europe/London') -> bool:
        if convertto=='Europe/London':
            dt = dt.astimezone(pytz.timezone('Europe/London'))
            return 0 <= dt.weekday() <= 4 and 8*60 <= dt.hour*60+dt.minute < 16*60+30
        if convertto=='America/New_York':
            dt = dt.astimezone(pytz.timezone('America/New_York'))
            return 0 <= dt.weekday() <= 4 and 9*60+30 <= dt.hour*60+dt.minute < 16*60
        
    def is_market_open(self,dt: datetime,convertto="") -> bool:
        if convertto=="":
            convertto=self.market()
        if convertto=='Asia/Tokyo':
            # Convert the datetime to Japan Standard Time (JST)
            dt = dt.astimezone(pytz.timezone('Asia/Tokyo'))    
            # Calculate the time in minutes since midnight
            time_in_minutes = dt.hour * 60 + dt.minute
            # Check if the time is within the morning or afternoon trading sessions
            morning_session = 9 * 60 <= time_in_minutes < 11 * 60 + 30
            afternoon_session = 12 * 60 + 30 <= time_in_minutes < 15 * 60
            return 0 <= dt.weekday() <= 4 and (morning_session or afternoon_session)
        if convertto=='Asia/Hong_Kong':
            dt = dt.astimezone(pytz.timezone('Asia/Hong_Kong'))
            # Check if the day is a weekday (Monday=0, ..., Friday=4)
            # Calculate the time in minutes since midnight
            time_in_minutes = dt.hour * 60 + dt.minute        
            # Check if the time is within the morning or afternoon trading sessions
            morning_session = 9 * 60 + 30 <= time_in_minutes < 12 * 60
            afternoon_session = 13 * 60 <= time_in_minutes < 16 * 60
            return 0 <= dt.weekday() <= 4 and (morning_session or afternoon_session)
        if convertto=='Asia/Shanghai':
            dt = dt.astimezone(pytz.timezone('Asia/Shanghai'))
            time_in_minutes = dt.hour * 60 + dt.minute        
            # Check if the time is within the morning or afternoon trading sessions
            morning_session = 9 * 60 + 30 <= time_in_minutes < 11 * 60 + 30
            afternoon_session = 13 * 60 <= time_in_minutes < 15 * 60
            return 0 <= dt.weekday() <= 4 and (morning_session or afternoon_session)
        if convertto=='Australia/Sydney':
            dt = dt.astimezone(pytz.timezone('Australia/Sydney'))
            time_in_minutes = dt.hour * 60 + dt.minute
            trading_session = 10 * 60 <= time_in_minutes < 16 * 60
            return 0 <= dt.weekday() <= 4 and trading_session
        if convertto=='Asia/Singapore':
            dt = dt.astimezone(pytz.timezone('Asia/Singapore'))
            time_in_minutes = dt.hour * 60 + dt.minute        
            morning_session = 9 * 60 <= time_in_minutes < 12 * 60
            afternoon_session = 13 * 60 <= time_in_minutes < 17 * 60        
            return 0 <= dt.weekday() <= 4 and (morning_session or afternoon_session)
        if convertto=='Asia/Kolkata':
            dt = dt.astimezone(pytz.timezone('Asia/Kolkata'))
            time_in_minutes = dt.hour * 60 + dt.minute
            trading_session = 9 * 60 <= time_in_minutes < 15 * 60 + 30#including premarket
            return 0 <= dt.weekday() <= 4 and trading_session
        if convertto=='Europe/London':
            dt = dt.astimezone(pytz.timezone('Europe/London'))
            return 0 <= dt.weekday() <= 4 and 8*60 <= dt.hour*60+dt.minute < 16*60+30
        if convertto=='America/New_York':
            dt = dt.astimezone(pytz.timezone('America/New_York'))
            return 0 <= dt.weekday() <= 4 and 9*60+30 <= dt.hour*60+dt.minute < 16*60
    
    def market(self):
        if self.instrument in ['usatechidxusd','usa30idxusd','usa500idxusd','ussc2000idxusd']:
            return 'America/New_York'
        elif self.instrument in ['jpnidxjpy']:
            return 'Asia/Tokyo'
        elif self.instrument in ['hkgidxhkd']:
            return 'Asia/Hong_Kong'
        elif self.instrument in ['chiidxusd']:
            return 'Asia/Shanghai'
        elif self.instrument in ['sgdidxsgd']:
            return 'Asia/Singapore'
        elif self.instrument in ['ausidxaud']:
            return 'Australia/Sydney'
        elif self.instrument in ['indidxusd']:
            return 'Asia/Kolkata'
        else:
            return 'Europe/London'


    def aggregate(self,df,prefix=''):
        df=df.copy()
        try:
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
        except Exception as e:
            print(f"Error converting timestamp without astype: {e}. Retrying with astype.")
            df['Date'] = pd.to_datetime(df['timestamp'].astype(float), unit='ms')
        #print("Date max is",df['Date'].max())
        df.set_index('Date',inplace=True)
        df.drop(['timestamp'],axis=1,inplace=True)
        if 'volume' in df.columns:
            df = df.resample(self.freq).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
        else:
            df = df.resample(self.freq).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'})        
        df=df.ffill()    
        df.dropna(inplace=True)
        df=df.add_prefix(prefix)
        return df
        
    def get_ohlc_data(self, instrument):
        frequency = frequency = '%Y-%m-%d %H:%M'

        conn = self.conn
        
        # Define the SQL query for aggregation        

        query = f"""
            WITH period_data AS (
                SELECT 
                    timestamp,
                    strftime('{frequency}', datetime(CAST(timestamp AS INTEGER)/1000, 'unixepoch')) AS period,
                    CAST(open AS REAL) AS open,
                    CAST(high AS REAL) AS high,
                    CAST(low AS REAL) AS low,
                    CAST(close AS REAL) AS close,
                    CAST(volume AS REAL) AS volume,
                    ROW_NUMBER() OVER (PARTITION BY strftime('{frequency}', datetime(CAST(timestamp AS INTEGER)/1000, 'unixepoch')) ORDER BY timestamp ASC) AS row_num_asc,
                    ROW_NUMBER() OVER (PARTITION BY strftime('{frequency}', datetime(CAST(timestamp AS INTEGER)/1000, 'unixepoch')) ORDER BY timestamp DESC) AS row_num_desc
                FROM {instrument}
            )
            SELECT 
                period,
                MAX(CASE WHEN row_num_asc = 1 THEN open END) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                MAX(CASE WHEN row_num_desc = 1 THEN close END) AS close,
                SUM(volume) AS volume,
                SUM(volume * close) AS vclose
            FROM period_data
            GROUP BY period
            ORDER BY period
        """
        # Execute the query and read the data into a pandas DataFrame
        df = pd.read_sql(query, conn)
        
        # Convert period to datetime and set it as the index
        df['period'] = pd.to_datetime(df['period'])
        df.set_index('period', inplace=True)
        
        return df

    def signalsrange(self,volume=False,marketopen=True,start_date=None, end_date=None):
        if start_date is None:
            start_date=self.start_date
        if end_date is None:
            end_date=self.end_date
        date_range = pd.date_range(start=start_date, end=end_date, freq='1D')
        date_range=date_range.tz_localize('UTC')
        lastdate=date_range[0].date()
        for date in date_range:
            if not volume:
                data=self.dataslice(date-timedelta(days=3),date+timedelta(days=3))
            else:
                data=self.dataslice(date-timedelta(days=3),date+timedelta(days=3),delvolume=False)
            data['utc_time'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize('UTC')
            if marketopen:
                data=data[data['utc_time'].map(lambda x: self.is_market_open(x,convertto='Europe/London') or self.is_market_open(x,convertto='America/New_York'))]
            if data.shape[0]==0:
                continue
            ohlc=self.aggregate(data)
            oldfreq=self.freq
            self.freq='5min'
            ohlc5=self.aggregate(data)
            self.freq=oldfreq
            s=set(data['utc_time'].map(lambda z:z.date()))
            s=[x for x in s if x>=lastdate]
            s=sorted(s)
            for dato in s:
                #print("dato is",dato)
                #print("lastdate is",lastdate)
                #print("ohlc shape is",ohlc[ohlc.index.date==dato].shape[0])
                if(ohlc5[ohlc5.index.date==dato].shape[0]==288):
                    if dato>lastdate:
                        lastdate=dato
                        data=data[data['utc_time'].apply(lambda z:z.date())==dato]
                        if marketopen:
                            data=data[data['utc_time'].map(lambda x: self.is_market_open(x,convertto='Europe/London') or self.is_market_open(x,convertto='America/New_York'))]
                        ohlc=self.aggregate(data)
                        yield data,ohlc

def main():
    print("Hej")

if __name__ == "__main__":
    # Example usage of the signalsrange function
    db = Db(instrument='gbridxgbp', start_date=datetime(2023, 1, 1), end_date=datetime(2023, 6, 1),method='dukascopy')
#    data=db.dataslice('2020-01-01','2020-01-05')
    for data, ohlc in db.signalsrange(volume=True, marketopen=True):
        print("Data slice:")
        print(data.head())
        print("OHLC data:")
        print(ohlc.head())
