# Korea Market Data Pipeline

한국 주식 시장 데이터를 수집, 변환, 적재하는 End-to-End ETL 파이프라인입니다.

Apache Airflow로 파이프라인을 오케스트레이션하고, PostgreSQL에 3계층(raw/staging/mart) 구조로 데이터를 관리하며, FastAPI로 REST API를 제공합니다.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                            │
│   ┌──────────────┐    ┌──────────────┐                      │
│   │ Naver Finance│    │   yfinance   │                      │
│   │  (크롤링)    │    │   (API)      │                      │
│   └──────┬───────┘    └──────┬───────┘                      │
└──────────┼───────────────────┼──────────────────────────────┘
           │                   │
           ▼                   ▼
┌─────────────────────────────────────────────────────────────┐
│              Apache Airflow (Orchestration)                 │
│   ┌─────────────────────┐  ┌────────────────────┐           │
│   │ naver_market_summary│  │ daily_stock_price  │           │
│   │   DAG (16:00)       │  │   DAG (17:00)      │           │
│   └────────┬────────────┘  └────────┬───────────┘           │
│            │                        │                       │
│            ▼                        ▼                       │
│   ┌─────────────────────────────────────────┐               │
│   │       market_aggregation DAG (18:00)    │               │
│   └─────────────────────┬───────────────────┘               │
│                         │                                   │
│   ┌─────────────────────▼───────────────────┐               │
│   │         Data Quality Checks             │               │
│   │  (row_count, null_ratio, freshness)     │               │
│   └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│                   PostgreSQL Database                       │
│                                                             │
│   ┌─────────┐     ┌───────────┐     ┌──────────┐            │
│   │   raw   │───▶│(transform)│ ──▶│   mart   │            │
│   │         │     │           │     │          │            │
│   │ • naver │     │  dedup    │     │ • dim    │            │
│   │   market│     │  clean    │     │   _stock │            │
│   │ • daily │     │  enrich   │     │ • fact   │            │
│   │   price │     │           │     │   _daily │            │
│   │ • sector│     │           │     │ • agg    │            │
│   │   info  │     │           │     │   _market│            │
│   └─────────┘     └───────────┘     └──────────┘            │
│                                                             │
│   ┌─────────────────────────────────────────┐               │
│   │         data_quality_log                │               │
│   └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI (Data Serving)                   │
│                                                             │
│   GET /stocks               종목 마스터 목록                 │
│   GET /stocks/{code}        종목 상세 정보                   │
│   GET /stocks/{code}/prices 종목 일봉 가격                   │
│   GET /markets/summary      시장별 집계                      │
│   GET /quality/logs         데이터 품질 로그                 │
│                                                             │
│   Swagger UI: http://localhost:8000/docs                    │
└─────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Apache Airflow 2.8 |
| Database | PostgreSQL 15 |
| API | FastAPI + Uvicorn |
| Data Processing | pandas, NumPy |
| Data Sources | Naver Finance (BeautifulSoup), yfinance |
| Infrastructure | Docker Compose |
| Language | Python 3.11 |

## Project Structure

```
korea-market-data-pipeline/
├── docker-compose.yml          # Airflow + PostgreSQL + FastAPI 컨테이너
├── Dockerfile                  # Airflow 이미지 (DAG 의존성 포함)
├── Dockerfile.api              # FastAPI 이미지
├── dags/
│   ├── common/
│   │   ├── db.py               # DB 연결 유틸리티
│   │   └── quality.py          # 데이터 품질 체크 유틸리티
│   ├── dag_naver_market_summary.py  # 네이버 금융 크롤링 DAG
│   ├── dag_daily_price.py           # yfinance 일봉 수집 DAG
│   └── dag_market_aggregation.py    # 시장 집계 DAG
├── app/
│   ├── main.py                 # FastAPI 엔드포인트
│   ├── config.py               # DB 설정
│   └── schemas.py              # Pydantic 응답 모델
├── scripts/sql/
│   └── init.sql                # DB 초기화 스키마 (raw/mart/quality)
├── tests/
│   ├── test_schemas.py         # 스키마 유닛 테스트
│   └── test_quality.py         # 품질 체크 유닛 테스트
├── requirements-airflow.txt
├── requirements-api.txt
└── .env.example
```

## Database Schema

### Raw Layer (`raw` schema)
원천 데이터를 가공 없이 적재합니다. `ingested_at` 타임스탬프로 적재 이력을 관리합니다.

- **`raw.naver_market_summary`**: 네이버 금융 시가총액 데이터 (종목명, ROE, PER, 시가총액 등)
- **`raw.daily_price`**: yfinance 일봉 OHLCV 데이터
- **`raw.sector_info`**: 섹터/업종 분류 데이터

### Mart Layer (`mart` schema)
비즈니스 로직이 적용된 서빙용 데이터입니다.

- **`mart.dim_stock`**: 종목 마스터 (차원 테이블)
- **`mart.fact_daily_price`**: 일봉 가격 (팩트 테이블, 등락률 포함)
- **`mart.agg_market_summary`**: 시장별 집계 (평균 PER/PBR/ROE, 총 거래량)

### Data Quality (`public.data_quality_log`)
파이프라인 실행 시 데이터 품질 검사 결과를 기록합니다.

- **row_count**: 최소 행 수 검증
- **null_ratio**: NULL 비율 검증
- **freshness**: 데이터 freshness 검증

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `naver_market_summary` | 평일 16:00 | 네이버 금융 KOSPI/KOSDAQ 전 종목 크롤링 → raw 적재 → dim_stock 변환 |
| `daily_stock_price` | 평일 17:00 | yfinance 주요 20종목 일봉 수집 → raw 적재 → fact_daily_price 변환 |
| `market_aggregation` | 평일 18:00 | dim_stock + fact_daily_price → agg_market_summary 집계 |

각 DAG은 `Extract → Transform → Quality Check` 3단계로 구성됩니다.

## Quick Start

### 1. 환경 설정

```bash
cp .env.example .env
# 필요시 .env 파일의 설정 값 수정
```

### 2. 전체 시스템 실행

```bash
docker-compose up -d
```

### 3. 서비스 접속

| Service | URL |
|---------|-----|
| Airflow Webserver | http://localhost:8080 (admin / admin) |
| FastAPI Swagger | http://localhost:8000/docs |
| PostgreSQL | localhost:5432 (pipeline / pipeline123) |

### 4. DAG 수동 실행 (테스트)

Airflow UI에서 각 DAG을 수동 트리거하거나 CLI로 실행:

```bash
docker-compose exec airflow-scheduler airflow dags trigger naver_market_summary
docker-compose exec airflow-scheduler airflow dags trigger daily_stock_price
docker-compose exec airflow-scheduler airflow dags trigger market_aggregation
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | 상태 체크 |
| GET | `/stocks` | 종목 목록 (시장 필터, 시가총액 정렬) |
| GET | `/stocks/{code}` | 종목 상세 정보 |
| GET | `/stocks/{code}/prices` | 일봉 가격 데이터 (날짜 범위 필터) |
| GET | `/markets/summary` | 시장별 집계 데이터 |
| GET | `/quality/logs` | 데이터 품질 검사 로그 |

### Example Requests

```bash
# 종목 목록 조회
curl http://localhost:8000/stocks?market_type=KOSPI&limit=10

# 삼성전자 가격 데이터 조회
curl http://localhost:8000/stocks/005930/prices?limit=5

# 시장 요약 조회
curl http://localhost:8000/markets/summary

# 데이터 품질 로그 (실패만)
curl http://localhost:8000/quality/logs?status=FAIL
```

## Data Quality Monitoring

각 DAG 실행 후 자동으로 데이터 품질 검사가 수행됩니다:

- **Row Count Check**: 테이블에 최소 행 수 이상 데이터가 있는지 확인
- **Null Ratio Check**: 주요 컬럼의 NULL 비율이 임계값 이하인지 확인
- **Freshness Check**: 가장 최근 데이터가 설정된 시간 내에 있는지 확인

검사 결과는 `data_quality_log` 테이블에 기록되며, `/quality/logs` API로 조회할 수 있습니다.

## License

This project is for educational and portfolio purposes.
