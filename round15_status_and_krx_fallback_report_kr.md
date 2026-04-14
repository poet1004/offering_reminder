# 라운드 15 패치: runtime JSON 안정화 + KRX 공식 API fallback

## 무엇을 고쳤나
- `build_pages_site.py`
  - runtime JSON이 비어 있거나 깨져 있어도 Pages 빌드가 죽지 않고 fallback JSON으로 계속 진행하도록 수정
- `mobile-feed-pages.yml`
  - `live_cache_status.json`, `official_api_status.json`, `official_api_probe.json`, `mobile_feed_verify.json`을 커밋 전 강제로 JSON 검증/정규화
- `refresh_official_api_cache.py`
  - KSD(SEIBro) 실패 시에도 `금융위원회_KRX상장종목정보` API로 종목명/심볼/시장구분을 채우는 fallback 추가
  - `official_krx_listed_info_live` 캐시 추가
  - KRX 결과를 `official_ksd_name_lookup_live`, `official_ksd_market_codes_live`에 병합
- `write_official_api_status.py`
  - `official_krx_listed_info_live`도 상태 집계에 포함
- `probe_public_data_api.py`
  - `KRX는 정상인데 KSD StockSvc만 SERVICE KEY IS NOT REGISTERED ERROR`인 경우 진단 문구 추가
- `export_mobile_feed.py`
  - 공식 캐시 목록에 `official_krx_listed_info_live` 추가
  - DART 자동 오버레이 최대 대상 확대(60 -> 180)
- `verify_mobile_feed.py`
  - `official_krx_listed_info_live`, `signal` 커버리지 점검 추가

## 왜 필요한가
- 현재 실패 원인은 두 개였다.
  1. `github-pages-static`가 비어 있거나 손상된 runtime JSON을 읽다가 `JSONDecodeError`로 중단
  2. KSD StockSvc는 여전히 `SERVICE KEY IS NOT REGISTERED ERROR`인데, KRX `GetKrxListedInfoService`는 정상 응답
- 그래서 KSD가 안 될 때도 KRX 데이터로 최소한 종목 매핑/시장구분은 살리고,
  Pages 빌드는 깨지지 않게 만드는 것이 우선이었다.

## 적용 후 기대 효과
- `github-pages-static`는 runtime JSON이 비어 있어도 더 이상 바로 죽지 않음
- `official-api-status.json`에서 KRX fallback rows가 잡히면 `cachePopulated`가 true로 바뀔 수 있음
- `official-api-probe.json`에서 KRX와 KSD 상태가 분리되어 더 명확히 보임
- 종목명/심볼/시장구분 매핑이 늘어나 현재가 매칭이 일부 개선될 수 있음

## 주의
- 이 패치가 있어도 `KSD StockSvc` 자체가 계속 `SERVICE KEY IS NOT REGISTERED ERROR`를 반환하면,
  KSD 고유 데이터(기업정보/주주분포/의무보호예수 상세)는 여전히 못 채울 수 있음.
- 그 경우는 코드 문제가 아니라 제공기관 백엔드 등록/동기화 문제 가능성이 크다.
