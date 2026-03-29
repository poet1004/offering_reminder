# 데이터 소스 혼선 핫픽스

이번 핫픽스에서 바뀐 점

- 기본 데이터 모드를 `실데이터 우선`으로 변경했습니다.
- 내장 샘플 workspace 자동연결을 기본적으로 끄도록 바꿨습니다.
- 실데이터가 없을 때는 가짜 공모주 이름을 섞어 보여주지 않고, 빈 상태/안내 메시지를 보여줍니다.
- 시장 페이지도 실데이터를 못 불러오면 샘플 지수를 자동으로 띄우지 않도록 바꿨습니다.
- 대시보드에 샘플/실데이터 경고 배너와 종목 수 분리를 추가했습니다.

집에서 확인할 때 권장 순서

1. 앱 사이드바에서 `데이터 모드 = 실데이터 우선`
2. `내장 샘플 workspace 자동연결` 끄기
3. `python scripts/refresh_live_cache.py`
4. `streamlit run app.py`

실데이터가 안 보이면

- `pip install -r requirements.txt`
- 특히 `YahooHTTP` 설치 여부 확인
- KIND export 파일이 있으면 `로컬 KIND export 경로`에 연결
- Unified Lab 실데이터 workspace가 있으면 `5분봉 lab workspace 경로`에 연결

샘플 화면이 필요하면

- `데이터 모드 = 샘플만`
- 또는 `내장 샘플 workspace 자동연결` 켜기


## 2026-03-28 추가 패치
- 실데이터 우선 모드에서 종목 데이터가 비어 있을 때 `subscription_score` / `listing_quality_score`가 없는 빈 DataFrame이 만들어져 대시보드 정렬에서 `KeyError`가 나던 문제를 수정했습니다.
- 점수 컬럼을 항상 보장하도록 `add_issue_scores()`와 `IPOScorer.add_scores()`를 보강했고, 대시보드/청약/상장 테이블 정렬도 안전 정렬로 바꿨습니다.
