import pandas as pd
import datetime as dt

# CSV 파일 읽기
df = pd.read_csv('VIX_History.csv')
name = {'Date': 'BASE_DT', 'OPEN': 'OPEN_PRICE', 'HIGH': 'HIGH_PRICE', 'LOW': 'LOW_PRICE', 'CLOSE': 'CLOSE_PRICE'}
df['DATE'] = df['DATE'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
df.rename(columns=name, inplace=True)
df['CHANGE_PERCENT'] = ((df['CLOSE_PRICE'] - df['OPEN_PRICE']) / df['OPEN_PRICE'] * 100).apply(lambda x: f'{x:.2f}%')
df.to_csv('fixed_VIX_History.csv', index=False)