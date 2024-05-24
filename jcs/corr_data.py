import pandas as pd
import redshift_connector

# Redshift 연결 정보 설정
host = 'devcourse-stock-detail-workgroup.381492204072.ap-northeast-2.redshift-serverless.amazonaws.com'
user = 'admin'
password = 'StockDetail1234!'
database = 'dev'
port = 5439


# Redshift 클러스터에 연결
conn = redshift_connector.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port
)

# SQL 쿼리를 통해 데이터 가져오기
query = """
WITH closest_dates AS (
    SELECT 
        T1.company_name,
        T1.base_dt AS finance_base_dt,
        T2.base_dt AS stock_base_dt,
        T2.end_stock_price as stock_price,
        ABS(T1.base_dt - T2.base_dt) AS min_diff,
        ROW_NUMBER() OVER (PARTITION BY T1.company_name, T1.base_dt ORDER BY ABS(T1.base_dt - T2.base_dt)) AS rn
    FROM 
        raw_data.wh_finance_info AS T1
    INNER JOIN 
        raw_data.wh_stock_price_info AS T2
    ON 
        UPPER(T1.company_name) = UPPER(T2.company_name)
)
SELECT 
    T1.base_dt, 
    cd.stock_base_dt, 
    T1.company_name,
    cd.stock_price,
    T1.debt_rt,
    T1.crrt_rt,
    T1.smpl_crrt_rt,
    T1.cptl_rt,
    T1.intrs_cvrg_rt,
    T1.tot_ast_rtnd_ernng_rt,
    T1.ast_trn_rt,
    T1.opr_ast_trn_rt
FROM 
    raw_data.wh_finance_info AS T1
INNER JOIN 
    closest_dates AS cd
ON 
    UPPER(T1.company_name) = UPPER(cd.company_name)
    AND T1.base_dt = cd.finance_base_dt
    AND cd.rn = 1
ORDER BY 
    T1.base_dt, 
    T1.company_name;
"""

# 데이터 가져오기
df = pd.read_sql(query, conn)

# 수치형 열 선택
numeric_df = df.select_dtypes(include=['number'])

# stock_price와 다른 수치형 변수들 간의 상관계수 계산
stock_price_corr = numeric_df.corrwith(numeric_df['stock_price'])

correlation_df = stock_price_corr.reset_index()
correlation_df.columns = ['variable_2', 'correlation']
correlation_df['variable_1'] = 'stock_price'

# 컬럼 순서 변경
correlation_df = correlation_df[['variable_1', 'variable_2', 'correlation']]

# 결과 출력
print(correlation_df)

# 상관계수 결과를 CSV 파일로 저장
correlation_df.to_csv('correlation_output.csv', index=False, encoding='utf-8')