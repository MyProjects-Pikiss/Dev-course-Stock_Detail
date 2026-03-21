# Stock_Detail

> 주요 주가지수, 국채금리, Reddit 감정 데이터, 기업 재무지표를 통합해 상관관계와 회귀 분석을 수행하고 Superset 대시보드로 시각화한 금융 데이터 프로젝트

## Overview
Stock_Detail은 시장 지수, 국채금리, 커뮤니티 감정, 기업 재무지표를 하나의 분석 흐름으로 통합해 주가와의 관계를 입체적으로 살펴보기 위해 만든 프로젝트입니다.

단순히 가격 흐름만 보는 것이 아니라, 미국 2년·10년 국채 금리, Reddit 감정 비율, 국가별 대표 지수, 기업 재무건전성 지표를 함께 분석하고 Amazon Redshift에 적재한 뒤 Apache Superset 대시보드로 시각화했습니다.

## Why
- 미국 증시에 투자할 때 금리, 시장 심리, 재무건전성이 실제로 어떤 영향을 주는지 한눈에 보고 싶었습니다.
- 정형 데이터와 비정형 데이터를 함께 다루는 데이터 분석 프로젝트를 통해 AWS, Docker, Redshift, Superset 활용 흐름을 검증하고 싶었습니다.
- 여러 지표를 개별적으로 확인하는 대신 하나의 데이터 파이프라인과 대시보드로 통합하고자 했습니다.

## Key Analyses
- 미국 2년/10년 국채 금리와 다우, 나스닥, S&P 지수 간 상관관계 분석
- Reddit 댓글 감정 분류 결과와 S&P500, Nasdaq, VIX의 연관성 분석
- 국가별 대표 주가지수의 주간 변동률 기반 상관계수 시각화
- 기업 재무건전성 지표와 주가 변동성의 상관 및 다중회귀분석

## Data Sources
- Investing.com CSV 데이터
- CBOE VIX 데이터
- Reddit API
- Alpha Vantage API

## Tech Stack
- Language: Python, SQL
- Data Processing: Pandas, Statsmodels, requests
- NLP: Hugging Face Transformers, PRAW
- Data Warehouse: AWS S3, Amazon Redshift
- Visualization: Apache Superset
- Infra: Docker

## Data Pipeline
1. 주가지수, 국채금리, Reddit 댓글, 기업 재무 데이터를 수집합니다.
2. Python으로 전처리하고 분석 가능한 형태로 정규화합니다.
3. 데이터를 S3와 Redshift에 적재합니다.
4. Redshift에서 상관관계·회귀분석용 테이블을 구성합니다.
5. Superset 대시보드에서 주요 지표를 시각화합니다.

## Data Model
### raw_data
- Reddit 댓글 데이터
- 주가 및 지수 데이터
- 기업 재무 데이터
- 미국 2년 / 10년 국채 데이터

### analytics
- 주요 주가 지수 및 긍정 댓글 비율 테이블
- VIX와 긍정 댓글 비율 테이블
- 종가 통합 테이블
- 금리/지수 상관계수 테이블
- 주가 변동률 테이블
- 상관분석 / 회귀분석 결과 테이블

## Dashboard Highlights
- Reddit 긍정 비율과 S&P500, Nasdaq, VIX 변화 비교
- 기업 재무건전성 지표와 주가 변동성 관계 시각화
- 국가별 대표 지수 비교 및 상관계수 분석
- 주요 시장 지표를 하나의 화면에서 탐색할 수 있는 Superset 대시보드 구성

## Outcome
이 프로젝트를 통해 금리, 시장 심리, 재무지표, 주가 간 관계를 하나의 분석 흐름으로 연결했고, Superset 대시보드로 결과를 직관적으로 탐색할 수 있는 환경을 구축했습니다.
