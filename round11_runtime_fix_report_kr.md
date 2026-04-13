# GitHub Pages / runtime 상태 저장 및 공공데이터 API 진단 패치

핵심 수정:
- `ipo_git/.gitignore`에서 `data/runtime/`를 전부 무시하던 규칙을 유지하되, Pages에 필요한 상태 JSON은 예외 처리.
- `mobile-feed-pages.yml`에서 `git add -f ipo_git/data/runtime/*.json`으로 상태 JSON을 강제 커밋.
- `probe_public_data_api.py` 추가: `getShotnByMartN1`, `getStkIsinByNmN1`, `getIssucoCustnoByNm`를 삼성전자 기준으로 직접 프로브하고 결과를 `official_api_probe.json`으로 저장.
- `build_pages_site.py`가 `official_api_probe.json`도 `_site/data/official-api-probe.json`으로 복사.
- `public_data_client.py`가 HTTP/HTTPS를 모두 시도하고 XML parse 실패 시 다음 조합으로 계속 진행하도록 보강.

기대 효과:
- `/data/official-api-status.json`, `/data/mobile-feed-verify.json`, `/data/live-cache-status.json`이 더 이상 `missing`으로 남지 않음.
- `/data/official-api-probe.json`으로 “키는 있는데 응답이 빈 건지 / 서비스 승인 문제인지 / 프로토콜 문제인지”를 더 쉽게 확인 가능.
