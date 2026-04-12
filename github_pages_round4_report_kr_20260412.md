이번 패치에서 반영한 내용

- PC 기본 폭을 줄이고 전체 컨테이너 max-width를 980px로 조정
- 모바일 카드/그리드 반응형 CSS를 더 강하게 적용
- 브라우저 캐시 때문에 예전 CSS/JS가 남지 않도록 app.css/app.js에 버전 쿼리 추가
- 공모주 달력 섹션 포함
- 대시보드의 `곧 청약 시작`은 오늘 기준 45일 내 일정만 표시하고, 없으면 빈 상태 메시지 표시
- 상장은 10개, 청약/보호예수/딜 탐색기는 8개씩 페이지네이션
- 데이터 상태 화면에서 KIND 보조 캐시 0행 경고(kind_listing_live, kind_public_offering_live, kind_pubprice_live)는 숨김 처리
- GitHub Actions `mobile-feed-pages` 워크플로에 `refresh_live_cache.py` 단계를 추가해 KIND/38 라이브 캐시도 같이 갱신 시도
- 기존 공공데이터포털 시크릿 이름은 그대로 사용: `PUBLIC_DATA_SERVICE_KEY`(공공데이터포털), `DART_API_KEY`(Open DART)

적용 위치

- 저장소 루트에 덮어쓰기
- 이후 `mobile-feed-pages` 워크플로를 새로 실행
- 그 다음 `github-pages-static`이 자동 또는 수동으로 배포
