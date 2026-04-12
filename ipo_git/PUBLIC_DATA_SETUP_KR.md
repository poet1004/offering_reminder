# 공공데이터포털 / KSD 공식 API 연동 메모

이 버전은 HTML 스크래핑 보강보다 `공식 API → 로컬 캐시 → 앱 표시` 흐름을 우선하도록 패치되어 있습니다.

## 로컬 실행용 기본값
- 로컬 패키지에는 `.env.local`에 공공데이터포털 인증키가 들어 있습니다.
- Git 업로드용 패키지에는 실제 키가 들어 있지 않습니다.
- Git 저장소에는 `.env.local`을 올리지 마세요.

## 처음 할 일
1. 앱을 종료합니다.
2. 프로젝트 루트에서 `run_refresh_official_api_cache.bat`를 실행합니다.
3. 필요하면 `run_refresh_live_cache.bat` 또는 `run_refresh_and_export_mobile_feed.bat`를 실행합니다.
4. 앱을 다시 켜고 `데이터 다시 읽기`를 누릅니다.

## 생성되는 공식 캐시
- `data/cache/official_ksd_name_lookup_live.csv`
- `data/cache/official_ksd_market_codes_live.csv`
- `data/cache/official_ksd_listing_info_live.csv`
- `data/cache/official_ksd_corp_basic_live.csv`
- `data/cache/official_ksd_shareholder_summary_live.csv`
- `data/cache/official_issue_overlay_live.csv`

## 현재 우선순위
1. 공식 API(KSD / 공공데이터)
2. DART / 가격 캐시
3. KIND / 38 보조 데이터
4. HTML 실시간 fetch

## 참고
- 승인 전이거나 호출 한도를 넘기면 공식 캐시가 비어 있을 수 있습니다.
- 그 경우 앱은 기존 캐시와 fallback 소스를 계속 사용합니다.
