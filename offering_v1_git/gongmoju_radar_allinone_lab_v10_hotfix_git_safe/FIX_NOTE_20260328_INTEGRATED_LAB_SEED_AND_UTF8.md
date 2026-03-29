# 2026-03-28 integrated lab seed/38/UTF-8 fix

이번 수정의 핵심은 세 가지입니다.

1. `scripts/export_ipo_seed_to_lab.py`
   - 기존에는 `refresh_live_cache(fetch_kind=True, fetch_38=False)`로 실행되어, KIND가 비어 있으면 seed export가 0행으로 끝났습니다.
   - 이제 38도 함께 갱신하고, cache bundle이 비면 direct live bundle, 마지막으로 direct 38 fallback까지 시도합니다.

2. `src/services/ipo_scrapers.py`
   - 38 스케줄 수집을 desktop 페이지 하나에만 의존하지 않고 모바일 `https://m.38.co.kr/ipo/fund.php`를 우선 시도합니다.
   - 모바일/desktop detail link를 모두 인식하게 했습니다.
   - 38 파싱 결과가 비어 있으면 조용히 0행으로 끝내지 않고 예외 메시지로 드러나게 했습니다.

3. integrated lab wizard / dataset builder
   - `run_integrated_lab_wizard.bat`에 UTF-8 codepage/env를 설정해 한글 경고가 깨지지 않도록 했습니다.
   - `run_lockup_lab_wizard.py`도 stdout/stderr를 UTF-8로 재설정합니다.
   - menu 2는 원래 `dataset_out`만 만드는 단계입니다. `backtest_out`이 비어 있는 것은 정상이며, backtest 결과는 menu 3 이후에 생성됩니다.
   - `ipo_lockup_program.py`는 KIND corpList 결과가 비어 있어도 local seed가 있으면 계속 진행합니다.
   - 기본 config는 38 보조 조회를 더 빠르게 돌도록 `ipo_end_page=30`, `fetch_38_detail_pages=false`로 조정했습니다.

## 기대되는 변화

- integrated lab 실행 시 seed export가 더 이상 KIND 0행 때문에 바로 막히지 않습니다.
- 앱/38 데이터가 살아 있으면 `integrated_lab/ipo_lockup_unified_lab/kind_master.csv`가 자동 생성될 가능성이 커집니다.
- menu 2 실행 후에는 `workspace/dataset_out/*`가 생성되고, menu 3 실행 후에야 `workspace/backtest_out/*`가 생성됩니다.
- 콘솔의 `[경고]` 한글 문구가 이전보다 덜 깨집니다.
