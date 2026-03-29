# Architecture

## App Layer
- `app.py`
  - Streamlit UI
  - page routing
  - cache orchestration

## Service Layer
- `ipo_pipeline.py`
  - sample/live/local KIND/strategy data union
- `ipo_scrapers.py`
  - KIND / 38 parsing and normalization
- `dart_client.py`
  - corp code cache + recent filings lookup
  - estkRs / document.xml access
- `dart_ipo_parser.py`
  - 투자설명서/증권신고서 본문 + 지분증권 주요정보 결합 파서
  - 확약/유통가능물량/기존주주비율/우리사주 실권 자동 추출
- `kis_client.py`
  - KIS authentication and price/history access
- `market_service.py`
  - market snapshot/history abstraction
- `alert_engine.py`
  - unlock / move / technical alerts
- `strategy_bridge.py`
  - backtest + unlock candidate ranking
- `lockup_strategy_service.py`
  - term별 진입룰/보유일수/배수필터 추론
  - 주문시트 / 실행보드 / 과거 예시 생성
- `scoring.py`
  - subscription / listing / unlock pressure scoring
- `backtest_repository.py`
  - summary / annual / trades / skip data access

## Storage Layer
- `data/cache/`
  - KIND/38 live cache
  - DART corp code cache
  - DART 원문 ZIP 및 파싱 snapshot cache
- `data/uploads/`
  - user uploaded KIND files
- `data/backtest/`
  - precomputed backtest results

## External Integration Strategy
- Official source first where practical (KIND, DART, KIS)
- 38 schedule used as secondary subscription source
- YahooHTTP used as lightweight market fallback
- local KIND export supported because browser-side export is often operationally more stable than scraping alone
