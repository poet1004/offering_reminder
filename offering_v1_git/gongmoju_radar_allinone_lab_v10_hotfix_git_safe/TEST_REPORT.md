# 검증 메모 (v8)

실행한 검증:
- `python -m compileall app.py src scripts integrated_lab` 통과
- `python scripts/smoke_test.py` 통과
  - 38 일정 중복 컬럼 파싱
  - 38 상세 페이지 pair-table 파싱
  - 38 메뉴 블롭 제거
  - 38 회사정보 텍스트 fallback 파싱
  - `issue_recency_sort` 정렬 회귀 테스트
  - DART 파서 fixture
  - KIND 공모기업 / 공모가대비주가정보 fixture
  - workspace 자동탐지 fixture
  - 전략 보드 / execution runtime fixture

참고:
- 이 샌드박스에는 `streamlit` 패키지가 없어 `streamlit run app.py` 자체는 여기서 직접 띄우지 못함
- 대신 앱/서비스/스크립트 전부 문법 검증과 서비스 레벨 스모크 테스트를 돌림
