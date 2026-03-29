# 2026-03-28 Stage / NA Hotfix

핵심 수정
- `bool(pd.NA)`로 대시보드가 죽는 문제 수정
- `coalesce()`가 `pd.NA`, `pd.NaT`를 실제 값처럼 취급해 실데이터를 덮어쓰던 병합 버그 수정
- 38 일정 문자열에서 `03.11(수) ~ 12(목)`, `2026.03.20(금)` 같은 형식을 파싱하도록 보강
- 최종 issue frame에서 날짜 기준으로 stage를 재계산하도록 수정
  - 청약예정 / 청약중 / 청약완료 / 상장예정 / 상장후
- 최종 bundle 단계에서만 `market` 빈값을 `미상`으로 채우도록 조정

원인 요약
1. `ipo_repository.alert_candidates()`에서 `bool(pd.NA)` 호출
2. `merge_live_sources()` / `_overlay_issues()` 내부 `coalesce()`가 `pd.NA`를 우선 채택
3. 38 날짜 포맷 일부를 못 읽어서 `subscription_start/end`가 비는 케이스 존재
4. 병합 후 stage를 재판정하지 않아 소스별 stage가 뒤섞임

검증
- `python -m compileall app.py src scripts`
- `python scripts/smoke_test.py`
- 추가 회귀 테스트 포함
  - nullable overlay
  - safe_bool / coalesce with pd.NA
  - 38 날짜 파서 확장 포맷
