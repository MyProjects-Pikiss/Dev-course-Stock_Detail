import pandas as pd
import redshift_connector
import statsmodels.api as sm

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

# 결측값 확인
print(numeric_df.isnull().sum())

# 결측값 처리 (예: 평균값으로 대체)
numeric_df = numeric_df.fillna(numeric_df.mean())

# 종속 변수와 독립 변수 설정
X = numeric_df[['debt_rt', 'crrt_rt', 'smpl_crrt_rt', 'cptl_rt', 'intrs_cvrg_rt', 'tot_ast_rtnd_ernng_rt', 'ast_trn_rt', 'opr_ast_trn_rt']]
y = numeric_df['stock_price']

# 상수항 추가
X = sm.add_constant(X)

# 회귀 모델 피팅
model = sm.OLS(y, X).fit()

# 결과 출력
print(model.summary())

# 회귀 결과를 CSV 파일로 저장
summary_df = pd.DataFrame(model.summary().tables[1].data)
summary_df.columns = summary_df.iloc[0]
summary_df = summary_df[1:]

# 부동소수점 처리
summary_df.iloc[:, 1:] = summary_df.iloc[:, 1:].applymap(lambda x: '{:.6f}'.format(float(x)))

summary_df.to_csv('regression_output.csv', index=False, encoding='utf-8')