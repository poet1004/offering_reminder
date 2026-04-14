# round12 data pipeline fix

핵심 진단
- KSD 공공데이터 API probe는 `SERVICE KEY IS NOT REGISTERED ERROR`를 반환했다.
- 즉 GitHub Actions에서 키 값은 읽히지만, 현재 시점에는 KSD StockSvc/CorpSvc에서 해당 키를 등록된 키로 인정하지 않고 있다.
- 반면 mobile feed는 이미 `currentPrice` 714건, `ma20` 162건 등 일부 보강은 성공하고 있다.
- `returnPct` 0건은 소스 부재보다 export 단계 계산 누락이 더 큰 원인이었다.
- `institutionalCompetitionRatio` / `lockupCommitmentRatio` / `existingShareholderRatio` 0건은 KSD 0행 + KIND 0행 + 38 HTTPS 실패가 겹친 결과다.

이번 패치
1. `mobile-feed-pages.yml`
   - `refresh_live_cache.py`에 `--refresh-dart-corp` 추가
   - mobile feed build 단계에 `DART_API_KEY` env 전달
   - `ipo_scrapers.py` 변경 시 워크플로 재실행되도록 path trigger 추가
2. `export_mobile_feed.py`
   - `returnPct` 계산 추가
   - `signal` 노출/계산 추가
   - 최근/핵심 종목에 대해 DART 자동 오버레이 적용
   - 기술지표 overlay가 `signal`도 채우도록 수정
3. `verify_mobile_feed.py`
   - `returnPct`, `signal`, `dart_corp_codes`까지 검증/집계
4. `ipo_scrapers.py`
   - 38.co.kr HTTPS 실패 시 HTTP fallback 재시도 추가

로컬 확인
- `python -m py_compile` 통과
- `build_item()` helper로 `returnPct=20`, `signal=상승` 확인
