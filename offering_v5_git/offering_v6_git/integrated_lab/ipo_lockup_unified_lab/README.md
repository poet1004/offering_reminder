# IPO Lockup Unified Lab

이 폴더는 지금까지 분리되어 있던 작업을 한 프로젝트로 묶은 통합 스캐폴드입니다.

핵심 목표는 두 갈래입니다.

1. **기존 합성 버킷 일봉 백테스트 유지**
   - KIND 로컬 EXCEL/HTML-xls 파서
   - 15D / 1M / 3M / 6M / 1Y 합성 unlock 이벤트
   - KIS 일봉 preload 기반 daily backtest

2. **실제 unlock turnover 연구 파이프라인 추가**
   - DART 공시에서 실제 unlock 이벤트 복원
   - 외부 분봉 CSV 또는 키움 수집 DB 적재
   - `unlock_shares` 대비 누적거래량 1x / 2x 등 threshold 엔트리 신호 생성
   - minute 엔트리 + KIS daily exit backtest
   - benchmark 대비 trade-window beta proxy 계산

---

## 폴더/파일 요약

- `ipo_lockup_program.py`
  - 기존 fastdaily 러너
  - 합성 버킷 dataset / daily backtest 담당

- `dart_unlock_events_builder.py`
  - OpenDART 공시에서 실제 unlock 이벤트 생성
  - 출력: `unlock_events_dart.csv`

- `unlock_events_to_backtest_input.py`
  - unlock 이벤트에 `ipo_price`, `market`, `lead_manager` 등 master 정보를 합쳐서
    backtest 입력형식 CSV로 변환

- `kiwoom_minute_pipeline.py`
  - minute DB 스키마
  - unlock 이벤트 기반 minute 수집 job 생성
  - 외부 CSV를 DB에 import
  - **주의:** 키움 OpenAPI+ 실제 수집 세션 구현은 stub 상태
  - 대신 **유튜브/직접수집 CSV를 import**해서 바로 다음 단계로 갈 수 있게 해둠

- `turnover_signal_engine.py`
  - minute DB + unlock 이벤트 CSV를 읽어서
    `cum_volume / unlock_shares >= 1x, 2x` 같은 turnover 엔트리 시그널 생성

- `turnover_daily_backtest.py`
  - turnover 엔트리 가격은 minute signal에서 사용
  - 청산은 KIS 일봉 종가로 `hold_days_by_term` 만큼 보유 후 exit

- `trade_window_beta.py`
  - trades CSV와 benchmark CSV를 비교해서
    strategy / term / signal_name 기준 beta proxy 계산

- `run_lockup_lab_wizard.py`
  - 메뉴형 실행기

---

## 빠른 사용 순서

### A. 기존 합성 버킷 daily backtest

1. KIND `신규상장기업현황.xls`, `종목별공모가대비주가등락률현황.xls` 준비
2. `python ipo_lockup_program.py build-dataset ...`
3. `python ipo_lockup_program.py backtest ...`

또는 `run_lockup_lab_wizard.py` 메뉴 2 / 3 사용

---

### B. 실제 unlock turnover 연구 파이프라인

#### 1) DART unlock 이벤트 생성
```bash
python dart_unlock_events_builder.py ^
  --master-csv workspace\dataset_out\filtered_master.csv ^
  --dart-key-file dart_key.txt ^
  --out-csv workspace\unlock_out\unlock_events_dart.csv ^
  --cache-dir workspace\cache_dart
```

#### 2) backtest 입력형식으로 변환
```bash
python unlock_events_to_backtest_input.py ^
  --unlock-csv workspace\unlock_out\unlock_events_dart.csv ^
  --master-csv workspace\dataset_out\filtered_master.csv ^
  --out-csv workspace\unlock_out\unlock_events_backtest_input.csv
```

#### 3) minute DB 초기화 + job 생성
```bash
python kiwoom_minute_pipeline.py --db-path workspace\data\curated\lockup_minute.db init-db

python kiwoom_minute_pipeline.py --db-path workspace\data\curated\lockup_minute.db enqueue-from-unlock ^
  --unlock-csv workspace\unlock_out\unlock_events_backtest_input.csv ^
  --interval-min 5 ^
  --pre-days 2 ^
  --post-days 5
```

#### 4) 외부 minute CSV import
유튜브/키움/대신/직접 수집 CSV를 DB에 넣을 수 있습니다.

단일 파일:
```bash
python kiwoom_minute_pipeline.py --db-path workspace\data\curated\lockup_minute.db import-minute-csv ^
  --csv-path C:\data\476830_5m.csv ^
  --interval-min 5 ^
  --symbol 476830
```

glob 패턴:
```bash
python kiwoom_minute_pipeline.py --db-path workspace\data\curated\lockup_minute.db import-minute-glob ^
  --glob "C:\data\minute\*.csv" ^
  --interval-min 5
```

#### 5) turnover signal 생성
```bash
python turnover_signal_engine.py ^
  --unlock-csv workspace\unlock_out\unlock_events_backtest_input.csv ^
  --db-path workspace\data\curated\lockup_minute.db ^
  --out-csv workspace\signal_out\turnover_signals.csv ^
  --miss-csv workspace\signal_out\turnover_signals_misses.csv ^
  --interval-min 5 ^
  --multiples 1,2 ^
  --price-filter reclaim_open_or_vwap ^
  --max-days-after 5 ^
  --aggregate-by type ^
  --cum-scope through_window
```

#### 6) turnover daily backtest
```bash
python turnover_daily_backtest.py ^
  --signals-csv workspace\signal_out\turnover_signals.csv ^
  --config turnover_backtest_config_example.json ^
  --key-file real_key.txt ^
  --cache-dir C:\Users\%USERNAME%\Desktop\한국투자증권\kis_cache ^
  --out-dir workspace\turnover_backtest_out
```

#### 7) benchmark 대비 beta proxy
```bash
python trade_window_beta.py ^
  --trades-csv workspace\turnover_backtest_out\all_trades.csv ^
  --benchmark-csv workspace\analysis_out\benchmark.csv ^
  --out-summary-csv workspace\analysis_out\trade_window_beta_summary.csv
```

---

## signal engine 설계 의도

`turnover_signal_engine.py`는 다음 구조를 가정합니다.

- 분모: 실제 unlock shares
- 분자: unlock 이후 누적 거래량
- 엔트리: `cum_volume >= unlock_shares * multiple`
- 가격 필터:
  - `none`
  - `reclaim_open`
  - `reclaim_vwap`
  - `reclaim_open_or_vwap`
  - `open_and_vwap`
  - `range_top40`

즉 네가 원래 원하던
**"기계적 매도 물량이 실제로 어느 정도 소화된 뒤 진입"**
을 테스트하기 위한 최소 골격입니다.

---

## CSV import 컬럼 규칙

`kiwoom_minute_pipeline.py import-minute-csv`는 아래 계열 컬럼을 최대한 자동 인식합니다.

- symbol / 종목코드 / 단축코드 / shcode
- datetime / 일시 / 체결시간
- 또는 date(일자) + time(시간)
- open(시가), high(고가), low(저가), close(종가/현재가), volume(거래량)
- amount(거래대금)은 있으면 사용, 없으면 `close * volume`으로 VWAP 근사

즉 CSV 구조가 조금 달라도 꽤 유연하게 ingest하도록 해두었습니다.

---

## 현재 제한 / 솔직한 상태

1. `ipo_lockup_program.py`는 실제 unlock turnover가 아니라 기존 합성 버킷 daily 백테스트입니다.
2. `kiwoom_minute_pipeline.py`의 실제 OpenAPI+ 세션은 **stub**입니다.
3. 대신 **minute CSV import 경로**를 넣어놨기 때문에,
   영상/블로그 코드로 받은 분봉을 바로 DB에 넣고 다음 단계(signal/backtest)로 갈 수 있습니다.
4. `trade_window_beta.py`는 **정식 일별 포트폴리오 beta**가 아니라
   **trade-window beta proxy**입니다.

---

## 추천 운영 순서

실제론 아래 순서가 가장 현실적입니다.

1. 기존 합성 버킷 daily로 universe / hold day 감 잡기
2. DART unlock 이벤트 생성
3. minute CSV 몇 종목만 import해서 turnover signal 품질 검증
4. 잘 맞으면 분봉 수집 대상을 늘리기
5. turnover backtest 결과와 기존 daily 결과 비교
6. benchmark beta proxy로 레짐 민감도 체크

---

## 설치

```bash
python -m pip install -U pandas numpy requests lxml html5lib openpyxl mojito2
```

또는 `00_install_packages.bat` 실행

---

## 실행기

- `01_run_lockup_lab_wizard.bat`
- 또는
```bash
python run_lockup_lab_wizard.py
```
