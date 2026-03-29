# 공모주 레이더 Pro

청약 → 상장 → 보호예수 해제 → 자동매수 전략 브릿지까지 한 번에 보는 **공모주 전용 Streamlit 앱**입니다.

이번 버전은 단순 MVP를 넘어서 아래를 실제로 붙였습니다.

- 샘플 데이터 + 로컬 KIND 파일 + KIND/38 캐시를 병합하는 **데이터 허브**
- `synthetic_ipo_events.csv`를 읽는 **전략 연동 레이어**
- 기존 백테스트 결과를 읽는 **전략 브릿지 / skip 사유 분석**
- 종목별 DART 지표 + unlock 캘린더 + 주문시트를 묶은 **락업 매수전략 실행 보드**
- 키움/CSV 기반 분봉 연구 산출물을 읽는 **5분봉 브리지 / 자동매수 브리지**
- DART corp code 캐시와 최근 공시 조회를 위한 **DART 연동 포인트**
- DART 지분증권 주요정보 API + 공시 원문 ZIP(document.xml) 기반 **IPO 본문 자동추출**
- 지수/선물/원자재/환율을 보는 **시장 보드**
- CSV 내보내기와 로컬 캐시 갱신용 **운영 스크립트**

## 포함 화면

### 1) 대시보드
- 청약 / 상장 / 보호예수 해제 / 알림 후보 카운트
- 시장 스냅샷 + 시장 분위기
- 30일 타임라인
- 청약 우선순위 / 상장 체크리스트 / 전략 브릿지 상위 후보

### 2) 딜 탐색기
- 종목 통합 필터링
- 샘플/실데이터/전략 데이터 출처 구분
- 상세 정보 / 공시 / 기술신호 확인

### 3) 청약
- 청약 일정
- 증권사 / 기관경쟁률 / 청약경쟁률 live
- 공모가 / 희망가 밴드
- 비례청약 손익분기 계산기
- 청약 점수(휴리스틱)

### 4) 상장
- 확약 비율
- 우리사주 실권
- 상장일 유통가능물량
- 기존주주비율
- 현재가 / 기술신호 / MA20 / MA60 / RSI14
- KIS 연결 시 장기 일봉 기반 기술신호 확장

### 5) 락업 매수전략
- unlock 캘린더 + DART 보강값 + 백테스트 규칙을 한 화면에서 결합
- term별 진입룰 / 보유일수 / 공모가 대비 최소배수 필터 표시
- 종목별 실행 타임라인, 과거 거래 예시, skip 사유 확인
- 주문시트 CSV / 자동화 연결용 JSON 프리뷰 제공

### 6) 5분봉 브리지
- 별도 unified lab workspace의 `unlock_out / signal_out / turnover_backtest_out / lockup_minute.db`를 자동 탐지
- 향후 unlock 후보와 분봉 수집 큐 상태, signal hit 여부, turnover 백테스트 성과를 한 화면에서 확인
- signal hits / misses / minute queue / bar coverage / beta proxy 요약
- 선택 후보별 turnover signal / trade CSV export

### 7) 전략 브릿지
- 기존 보호예수 해제 자동매수 백테스트의 기간별 edge 요약
- 향후 unlock 일정과 히스토리컬 edge를 조합한 후보 랭킹
- 월별 unlock 분포

### 8) 보호예수/알림
- unlock 캘린더
- unlock pressure score
- 이례적 가격변동 / 기술신호 / unlock 임박 알림 후보
- CSV export

### 9) 시장
- KOSPI / KOSDAQ / S&P500 / NASDAQ
- 선물 / WTI / Gold / USDKRW
- 기간별 차트

### 10) 백테스트
- 버전 비교
- 요약 / 연도별 / 거래 로그
- skip summary / skip reasons

### 11) DART 자동추출
- 종목별 투자설명서 / 증권신고서 자동 선택
- 확약 비율, 상장일 유통가능물량, 기존주주비율, 구주매출 비중 자동 추출
- 우리사주 실권, DART 접수번호, 뷰어 링크, 근거 문장/테이블 표시
- 기존 앱 값 대비 덮어쓰기 프리뷰

### 12) 데이터 허브
- KIND / 38 캐시 갱신
- 로컬 KIND 엑셀·CSV 업로드
- 소스 상태 / 캐시 인벤토리 / raw table preview
- DART corp code 캐시 갱신
- DART IPO 지표 배치 추출
- 배치 추출 결과를 `data/uploads/dart_enriched_latest.csv`로 저장해 앱에 반영

## 실행 방법

```bash
pip install -r requirements.txt
streamlit run app.py
```

윈도우에서는 `run_app.bat`로 실행해도 됩니다.

이번 버전부터는 프로젝트 루트의 `.env` 또는 `.env.local`을 자동으로 읽습니다.

## 권장 환경변수

```bash
KIS_APP_KEY=...
KIS_APP_SECRET=...
KIS_ENV=real
DART_API_KEY=...
```

## 로컬 테스트 빠른 시작

### 1) 사전 점검만 실행
```bash
python scripts/preflight_check.py
```

### 2) 테스트 준비를 한 번에 실행
```bash
python scripts/prepare_local_test.py --workspace data/sample_unified_lab_workspace
```

### 3) Unified Lab zip을 먼저 풀고 싶을 때
```bash
python scripts/import_unified_lab_zip.py --zip-path C:/path/to/workspace.zip --clean
```

### 4) Windows 원클릭
```bat
run_prepare_local_test.bat
```

자세한 순서는 `LOCAL_TEST_RUNBOOK.md`를 보면 됩니다.

## 권장 운영 흐름

1. KIND에서 `신규상장기업현황` 또는 `공모가대비주가정보` 엑셀/CSV를 내려받습니다.
2. 앱의 **데이터 허브**에서 업로드하거나 경로를 직접 지정합니다.
3. `synthetic_ipo_events.csv` 경로를 사이드바에 넣어 전략 unlock 데이터를 붙입니다.
4. 필요 시 **KIND / 38 캐시 새로고침**으로 최신 스냅샷을 갱신합니다.
5. 필요하면 **5분봉 lab workspace 경로**에 별도 unified lab 결과 폴더를 연결합니다. 앱에는 `data/sample_unified_lab_workspace` 예시가 포함되어 있어 바로 테스트할 수 있습니다.
6. **DART 자동추출** 또는 **데이터 허브 > DART IPO 지표 배치 추출**로 공시 본문 지표를 보강합니다.
7. 배치 결과를 저장하면 `data/uploads/dart_enriched_latest.csv`가 생성되고 앱 로딩 시 자동으로 overlay 됩니다.
8. **락업 매수전략**에서 종목별 실행 플랜과 주문시트를 확인합니다.
9. **5분봉 브리지**에서 minute queue / turnover signal / turnover backtest를 기존 전략 보드와 함께 확인합니다.
10. **전략 브릿지**와 **보호예수/알림**으로 전체 후보 분포와 알림을 점검합니다.

## 외부 전략 데이터 연동

아래 파일을 넣으면 보호예수 해제 일정이 실전 전략 데이터로 바뀝니다.

- `dataset_out/synthetic_ipo_events.csv`
- `workspace/unlock_out/unlock_events_backtest_input.csv`

기존 lock-up 전략 프로젝트 출력물을 그대로 읽도록 되어 있습니다.

## 5분봉 / Unified Lab 브리지

별도 분봉 연구 폴더를 아래 구조로 두면 앱이 자동 탐지합니다.

```text
workspace/
├─ unlock_out/unlock_events_backtest_input.csv
├─ signal_out/turnover_signals.csv
├─ signal_out/turnover_signals_misses.csv
├─ turnover_backtest_out/summary_all.csv
├─ turnover_backtest_out/all_trades.csv
└─ data/curated/lockup_minute.db
```

이 프로젝트에는 바로 테스트 가능한 예시 workspace가 포함되어 있습니다.

- `data/sample_unified_lab_workspace/`

## CLI 스크립트

### 캐시 갱신
```bash
python scripts/refresh_live_cache.py
```

### 알림 CSV 내보내기
```bash
python scripts/export_alerts.py --external-unlock dataset_out/synthetic_ipo_events.csv
```

### 락업 전략 주문시트 내보내기
```bash
python scripts/export_lockup_strategy_plan.py --version 1.0 --external-unlock dataset_out/synthetic_ipo_events.csv
```

### DART IPO 자동추출
```bash
python scripts/analyze_dart_ipo.py --corp-name 기업명
python scripts/analyze_dart_ipo.py --input data/sample_ipo_events.csv --max-items 5 --only-missing
```

### 통합 execution bridge export
```bash
python scripts/export_unified_execution_bridge.py --workspace data/sample_unified_lab_workspace --version 2.0
```

### preflight 체크
```bash
python scripts/preflight_check.py
```

### 로컬 테스트 준비 일괄 실행
```bash
python scripts/prepare_local_test.py --workspace data/sample_unified_lab_workspace
```

### Unified Lab zip 가져오기
```bash
python scripts/import_unified_lab_zip.py --zip-path C:/path/to/workspace.zip --clean
```

### 스모크 테스트
```bash
python scripts/smoke_test.py
```

## 디렉터리

```text
gongmoju_radar_unified_bridge/
├─ app.py
├─ data/
│  ├─ sample_ipo_events.csv
│  ├─ sample_market_snapshot.csv
│  ├─ sample_market_history.csv
│  ├─ backtest/
│  ├─ cache/
│  └─ uploads/
├─ scripts/
│  ├─ refresh_live_cache.py
│  ├─ export_alerts.py
│  ├─ export_lockup_strategy_plan.py
│  ├─ export_unified_execution_bridge.py
│  └─ smoke_test.py
├─ src/
│  ├─ utils.py
│  └─ services/
│     ├─ calculations.py
│     ├─ kis_client.py
│     ├─ dart_client.py
│     ├─ ipo_scrapers.py
│     ├─ ipo_repository.py
│     ├─ ipo_pipeline.py
│     ├─ live_cache.py
│     ├─ scoring.py
│     ├─ alert_engine.py
│     ├─ strategy_bridge.py
│     ├─ lockup_strategy_service.py
│     ├─ market_service.py
│     ├─ backtest_repository.py
│     └─ unified_lab_bridge.py
├─ requirements.txt
├─ .env.example
├─ LOCAL_TEST_RUNBOOK.md
├─ run_app.bat
├─ run_app.sh
├─ run_preflight.bat
└─ run_prepare_local_test.bat
```

## 메모

- API 키 없이도 샘플 데이터로 즉시 동작합니다.
- `scripts/prepare_local_test.py`를 돌리면 `data/runtime/` 아래에 preflight report, 실행계획 CSV, payload JSON, 드라이런 CSV가 생성됩니다.
- `실데이터 시도` 모드에서는 네트워크 연결 상태에 따라 KIND/38/YahooHTTP/KIS 시도를 하고, 실패하면 캐시나 샘플로 내려옵니다.
- Streamlit/네트워크가 없는 환경에서는 UI를 직접 띄우지 못하므로, 포함된 `scripts/smoke_test.py`로 서비스 레이어를 먼저 점검하는 구성이 좋습니다.
- DART 본문 파서는 공시 원문 구조가 종목마다 조금씩 달라 휴리스틱을 사용합니다. 따라서 추출값 옆에 근거 문장/테이블을 함께 보여주도록 구성했습니다.


# 공모주 레이더 + Lockup Unified Lab 올인원

이 프로젝트는 Streamlit 앱과 `ipo_lockup_unified_lab`를 같은 폴더 안에 묶은 버전입니다.

구성
- 앱: `app.py`
- 통합 lab: `integrated_lab/ipo_lockup_unified_lab`
- 통합 workspace: `integrated_lab/ipo_lockup_unified_lab/workspace`

처음 할 일
1. `python scripts/sync_env_to_lab_keys.py`
2. `python scripts/prepare_integrated_lab_workspace.py`
3. 앱 실행: `streamlit run app.py`
4. 분봉/락업 연구 산출물 생성: `run_integrated_lab_wizard.bat`

메모
- 앱은 통합 workspace를 자동 탐지합니다.
- 아직 산출물이 없으면 락업 매수전략 / 5분봉 브리지 / 전략 브릿지 페이지는 비어 있을 수 있습니다.
- `real_key.txt`, `practice_key.txt`, `dart_key.txt`는 `.env.local`에서 자동 생성됩니다.
