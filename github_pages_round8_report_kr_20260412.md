# GitHub Pages 라운드 8 패치

반영 내용
- 정적 웹의 기준일을 브라우저 현재시각보다 `feed.upstreamUpdatedAt / feed.generatedAt` 우선으로 사용하도록 수정
- 달력의 청약 일정을 청약 시작일 1회만 표시하고, 상세에는 `청약 04.14~04.15`처럼 범위가 보이도록 수정
- 가까운 일정/달력 상세의 `미상` 시장 문구를 제거하고 주관사 텍스트를 우선 표기하도록 수정
- 데일리 브리핑 4카드는 기본 2열, 넓은 화면(1180px+)에서는 4열로만 보이도록 수정
- 모바일 달력의 토/일 열 폭을 줄여 컨테이너보다 살짝 넓어지던 현상을 완화
- 상장 현황 상단에 `시세/기관/확약 데이터 미반영` 안내가 자동으로 나오도록 추가
- GitHub Actions `mobile-feed-pages`가 공공데이터 갱신 결과를 `ipo_git/data/runtime/official_api_status.json`으로 저장하도록 수정
- `build_pages_site.py`가 `official_api_status.json`을 정적 사이트 `data/official-api-status.json`으로 같이 내보내도록 수정

검증
- `node --check ipo_git/static_pages/assets/app.js` 통과
- `python3 -m py_compile ipo_git/scripts/build_pages_site.py ipo_git/scripts/refresh_official_api_cache.py ipo_git/scripts/export_mobile_feed.py` 통과
- `python3 ipo_git/scripts/build_pages_site.py --repo ipo_git --output _site_test` 성공
