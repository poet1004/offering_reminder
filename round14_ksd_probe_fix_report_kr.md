# 라운드 14 패치: KSD 승인 진단 정확도 보강

## 왜 필요한가
- 기존 프로브는 `getShotnByMartN1`, `getStkIsinByNmN1`, `CorpSvc`까지 같이 때려서
  실제 `[승인] 한국예탁결제원_주식정보서비스`가 살아 있는지 단정하기 어려웠습니다.
- 이번 패치는 data.go.kr 페이지에 실제 서비스URL로 노출되는 `StockSvc/getKDRSecnInfo`를 먼저 때려
  주식정보서비스 승인/연동 여부를 분리해서 보여줍니다.

## 바뀌는 점
- `official-api-probe.json`에 `scopes.ksdStock` 추가
- KSD 주식정보서비스는 `getKDRSecnInfo?ServiceKey=...&caltotMartTpcd=12`로 직접 검증
- KRX 상장종목정보 검증은 그대로 유지
- 수동 확인용 URL 템플릿(`manualTests`)도 같이 남김

## 해석 방법
- `scopes.krx.ok = true` + `scopes.ksdStock.ok = false`
  - KRX 키는 정상, KSD 주식정보서비스만 미동기화/미반영/서비스측 문제 가능성 큼
- `scopes.ksdStock.ok = true`
  - KSD 주식정보서비스 연결은 정상
  - 그 경우 이전 실패는 메서드 선택이나 파이프라인 병합 로직 문제 쪽이 더 유력
