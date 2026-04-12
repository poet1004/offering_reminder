# GitHub Pages 배포본 라운드 2 수정

반영 내용
- 모바일 레이아웃 개선
  - 상단 내비를 가로 스크롤 칩형으로 변경
  - 요약 카드 2열 고정
  - 모바일 카드/섹션 패딩 축소
  - 시장/대시보드 카드 가독성 개선
- 대시보드 요약 카드 교체
  - 종목 수 / 이벤트 수 제거
  - 30일 내 청약 / 30일 내 상장 / 환율 / 시장 분위기 4개로 교체
- `곧 청약 시작` 오류 수정
  - 브라우저의 실제 오늘 날짜 기준으로 0~45일 내 청약만 표시
  - 더 이상 2025년 12월 과거 일정이 상단에 고정되지 않음
- `청산/상장폐지 가능` 문구 제거
  - 명시적 상장폐지일이 있을 때만 `상장폐지`
  - 오래된 스팩인데 시세가 없을 때는 `상장상태 미확인`
- 상장 표 페이지네이션 추가
  - 10개씩 표시
  - 페이지 버튼으로 이동
- 쇼츠 스튜디오 제거
- GitHub Actions에 공식 API 캐시 갱신 단계 추가
  - `refresh_official_api_cache.py --data-dir ipo_git/data`
  - `PUBLIC_DATA_SERVICE_KEY` / `KSD_PUBLIC_DATA_SERVICE_KEY` / `DART_API_KEY` secrets 지원

포함 파일
- `.github/workflows/mobile-feed-pages.yml`
- `ipo_git/static_pages/index.html`
- `ipo_git/static_pages/assets/app.js`
- `ipo_git/static_pages/assets/app.css`
- `ipo_git/scripts/refresh_official_api_cache.py`
- `ipo_git/src/services/public_data_client.py`

적용 위치
- **저장소 루트**에 덮어쓰기

주의
- 현재 배포본의 빈 데이터는 아직 공식 API 캐시가 충분히 채워지지 않아서 남아 있는 부분이 큼
- 이 패치 후에는 GitHub Secrets에 `PUBLIC_DATA_SERVICE_KEY`를 넣고 `mobile-feed-pages` 워크플로를 다시 실행해야 반영폭이 커짐
