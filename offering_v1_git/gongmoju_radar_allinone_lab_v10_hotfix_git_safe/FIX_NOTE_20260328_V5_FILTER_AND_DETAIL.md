# Fix note — 2026-03-28 v5

이번 수정의 핵심:

- 38 청약일정 파서에서 페이지 본문/메뉴/`[공모뉴스]` 같은 비정상 행을 강하게 필터링
- 기존 cache/live bundle을 다시 읽을 때도 `clean_issue_frame()`으로 junk row 제거
- 38 상세 파서에서 아래 필드 추가 추출
  - `total_offer_shares`
  - `new_shares`
  - `selling_shares`
  - `secondary_sale_ratio`
  - `post_listing_total_shares`
- DART corp lookup를 이름 정규화/부분일치 기준으로 보강
- 종목 개요 화면에 “이 값들은 38 상세/DART 분석 후 채워질 수 있음” 안내 추가
- `CACHE_REV`를 `20260328_v5`로 올림
- issue gap 진단 스크립트 추가
  - `python scripts/diagnose_issue_gaps.py`

검증:

- `python -m compileall app.py src scripts integrated_lab`
- `python scripts/smoke_test.py`
- 업로드된 `subscriptions.csv`로 junk row 제거 재현 확인
