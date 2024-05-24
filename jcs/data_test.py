import requests
import pandas as pd

# Alpha Vantage API Key 설정
api_key = '7ELZFXXARZAM9EWN'

# 종목 리스트
symbols = {
    'NASDAQ': 'IXIC',
    'S&P 500': 'GSPC',
    'CBOE VIX': 'VIX'
    # 'Tesla': 'TSLA',
    # 'Nvidia': 'NVDA',
    # 'Disney': 'DIS'
}

# 재무 데이터 가져오기 (분기별 데이터)
def fetch_alpha_vantage_financial_data(symbol, api_key):
    balance_sheet_url = f'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={symbol}&apikey={api_key}'
    income_statement_url = f'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={symbol}&apikey={api_key}'
    cash_flow_url = f'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={symbol}&apikey={api_key}'

    balance_sheet_data = requests.get(balance_sheet_url).json()
    income_statement_data = requests.get(income_statement_url).json()
    cash_flow_data = requests.get(cash_flow_url).json()

    return balance_sheet_data, income_statement_data, cash_flow_data

# 빈 데이터프레임 생성
all_financial_ratios = pd.DataFrame()

for name, symbol in symbols.items():
    balance_sheet_data, income_statement_data, cash_flow_data = fetch_alpha_vantage_financial_data(symbol, api_key)

    if 'quarterlyReports' not in balance_sheet_data or 'quarterlyReports' not in income_statement_data or 'quarterlyReports' not in cash_flow_data:
        print(f"Error fetching data for {name} ({symbol}). Check your API key or subscription plan.")
        continue

    # 데이터프레임으로 변환
    balance_sheet_df = pd.DataFrame(balance_sheet_data['quarterlyReports'])
    income_statement_df = pd.DataFrame(income_statement_data['quarterlyReports'])
    cash_flow_df = pd.DataFrame(cash_flow_data['quarterlyReports'])

    # 날짜 형식을 datetime으로 변환
    balance_sheet_df['fiscalDateEnding'] = pd.to_datetime(balance_sheet_df['fiscalDateEnding'])
    income_statement_df['fiscalDateEnding'] = pd.to_datetime(income_statement_df['fiscalDateEnding'])
    cash_flow_df['fiscalDateEnding'] = pd.to_datetime(cash_flow_df['fiscalDateEnding'])

    # 결측치 처리 및 float 변환
    for column in balance_sheet_df.columns:
        if column != 'fiscalDateEnding':
            balance_sheet_df[column] = pd.to_numeric(balance_sheet_df[column], errors='coerce')
    
    for column in income_statement_df.columns:
        if column != 'fiscalDateEnding':
            income_statement_df[column] = pd.to_numeric(income_statement_df[column], errors='coerce')
    
    for column in cash_flow_df.columns:
        if column != 'fiscalDateEnding':
            cash_flow_df[column] = pd.to_numeric(cash_flow_df[column], errors='coerce')

    # 필요한 재무 비율 계산
    balance_sheet_df['Debt Ratio'] = balance_sheet_df['totalLiabilities'] / balance_sheet_df['totalAssets']
    balance_sheet_df['Current Ratio'] = balance_sheet_df['totalCurrentAssets'] / balance_sheet_df['totalCurrentLiabilities']
    balance_sheet_df['Quick Ratio'] = (balance_sheet_df['totalCurrentAssets'] - balance_sheet_df['inventory']) / balance_sheet_df['totalCurrentLiabilities']
    balance_sheet_df['Equity Ratio'] = balance_sheet_df['totalShareholderEquity'] / balance_sheet_df['totalAssets']
    balance_sheet_df['Retained Earnings to Total Assets'] = balance_sheet_df['retainedEarnings'] / balance_sheet_df['totalAssets']

    income_statement_df['Interest Coverage Ratio'] = income_statement_df['operatingIncome'] / income_statement_df['interestExpense']

    # 자산회전율 (Asset Turnover Ratio) 계산
    balance_sheet_df['avgTotalAssets'] = (balance_sheet_df['totalAssets'] + balance_sheet_df['totalAssets'].shift(1)) / 2
    income_statement_df['Asset Turnover Ratio'] = income_statement_df['totalRevenue'] / balance_sheet_df['avgTotalAssets']

    # 운전자본 회전율 (Working Capital Turnover) 계산
    balance_sheet_df['Working Capital'] = balance_sheet_df['totalCurrentAssets'] - balance_sheet_df['totalCurrentLiabilities']
    income_statement_df['Working Capital Turnover'] = income_statement_df['totalRevenue'] / balance_sheet_df['Working Capital']

    # 필요한 데이터 선택하여 하나의 DataFrame으로 합치기
    financial_ratios = pd.DataFrame({
        'COMPANY_NAME': name,
        'BASE_DT': balance_sheet_df['fiscalDateEnding'],
        'DEBT_RT': balance_sheet_df['Debt Ratio'],
        'CRRT_RT': balance_sheet_df['Current Ratio'],
        'SMPL_CRRT_RT': balance_sheet_df['Quick Ratio'],
        'CPTL_RT': balance_sheet_df['Equity Ratio'],
        'TOT_AST_RTND_ERNNG_RT': balance_sheet_df['Retained Earnings to Total Assets'],
        'INTRS_CVRG_RT': income_statement_df['Interest Coverage Ratio'],
        'AST_TRN_RT': income_statement_df['Asset Turnover Ratio'],
        'OPR_AST_TRN_RT': income_statement_df['Working Capital Turnover'],
        'TOT_LBLTS': balance_sheet_df['totalLiabilities'],
        'TOT_AST': balance_sheet_df['totalAssets'],
        'TOT_CRRT_AST': balance_sheet_df['totalCurrentAssets'],
        'TOT_CRRT_LBLTS': balance_sheet_df['totalCurrentLiabilities'],
        'TOT_CPTL': balance_sheet_df['totalShareholderEquity'],
        'INV_STOCK': balance_sheet_df['inventory'],
        'RTND_ERNNG': balance_sheet_df['retainedEarnings'],
        'OPRTN_PRFT': income_statement_df['operatingIncome'],
        'INTRS_EXPNS': income_statement_df['interestExpense'],
        'TOT_REVENUE': income_statement_df['totalRevenue']
    })

    # 모든 종목의 데이터를 하나의 DataFrame으로 결합
    all_financial_ratios = pd.concat([all_financial_ratios, financial_ratios], ignore_index=True)

# CSV 파일로 저장 (날짜 형식을 지정)
all_financial_ratios.to_csv('financial_ratios_all_symbols_11.csv', index=False, encoding='utf-8', date_format='%Y-%m-%d')