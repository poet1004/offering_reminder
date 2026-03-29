# v8 검증/수정 요약

이번 버전에서 고친 핵심:
- 딜 탐색기 크래시: `issue_recency_sort` 누락 수정
- 탐색기 기본 정렬: 최신/예정 IPO 우선 정렬로 변경
- 38 상세 수집: `forum/board`, `ipo.htm`, `o=cinfo` 링크까지 detail 후보로 인식
- 38 텍스트 fallback 파싱: 시장/업종/희망공모가/공모주식수/주간사 등 pair-table이 아닌 페이지에서도 보조 추출
- DART 자동 보강 범위 확대: 최근 IPO 후보 중 결측이 큰 종목 우선 보강
- Streamlit Community Cloud 배포 편의: `st.secrets` -> 환경변수 동기화, `.streamlit/secrets.toml.example`, `.gitignore`, 배포 가이드 추가
- `.env.local` 등은 실제 키 제거 후 placeholder로 정리

검증:
- `python -m compileall app.py src scripts integrated_lab` 통과
- `python scripts/smoke_test.py` 통과
