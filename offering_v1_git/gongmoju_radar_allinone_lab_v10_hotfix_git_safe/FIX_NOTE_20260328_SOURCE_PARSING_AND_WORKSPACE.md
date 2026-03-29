# FIX NOTE — 2026-03-28 (source parsing / workspace / unlock bridge)

이번 수정에서 손본 핵심은 세 가지입니다.

1. **38 상세 파서 오염 방지**
   - 38 상세 페이지의 네비게이션/메뉴 문구가 `업종`, `종목코드` 같은 필드로 잘못 들어가던 문제를 막았습니다.
   - `업종`/`시장`/`주관사` 값이 메뉴 블롭처럼 보이면 버립니다.
   - `종목코드`는 **6자리 숫자 코드만** 인정합니다.
   - 상세 페이지의 회사명과 일정표 회사명이 다르면 상세 오버레이를 적용하지 않습니다.

2. **KIND / 38 표 선택 로직 강화**
   - `pd.read_html()` 결과 중에서 실제 IPO 표보다 메뉴 테이블을 고르는 일이 있어, 표 점수 로직을 다시 짰습니다.
   - 헤더 일치도를 크게 반영하고, 메뉴 키워드가 많은 넓은 테이블에는 패널티를 주도록 바꿨습니다.

3. **Unlock / Unified Lab workspace 자동 탐지 확대**
   - 기존엔 `workspace/`만 주로 찾았지만, 이제 다음도 자동 탐지합니다.
     - `ipo_lockup_unified_lab/workspace`
     - `ipo_lockup_runner_fastdaily/workspace`
     - `data/imports/*`
     - runtime import manifest에 기록된 최근 import 경로
   - 따라서 옆 프로젝트를 같은 상위 폴더에 풀어뒀으면 자동 연결될 가능성이 훨씬 높아졌습니다.

## 여전히 비어 있을 수 있는 경우

- 옆 프로젝트에서 아직 `workspace/unlock_out/unlock_events_backtest_input.csv`를 만들지 않은 경우
- `workspace/signal_out/turnover_signals.csv` / `workspace/turnover_backtest_out/*`가 아직 없는 경우
- 실데이터 사이트 응답이 일시 실패한 경우
