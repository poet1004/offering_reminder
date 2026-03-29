# FIX NOTE 2026-03-29 V10 HOTFIX

## 수정 내용
1. 딜 탐색기 KeyError 수정
   - `issue_recency_sort()` 가 점수 컬럼을 잃지 않도록 보완
2. 38 상세 보강 수정
   - `detail_url` 이 표준화 중간에 사라지지 않도록 수정
3. 앱 초기 번들 로딩 수정
   - `IPODataHub` 에 `DartClient.from_env()` 를 전달하도록 수정
4. 로컬 KIND 시드 우선순위 조정
   - `kind_ipo_master.csv` 를 `kind_master.csv` 보다 먼저 사용하도록 변경
5. 테스트 보강
   - 점수 컬럼 보존, 38 상세 보강, 로컬 KIND 시드 탐지 회귀 테스트 추가
