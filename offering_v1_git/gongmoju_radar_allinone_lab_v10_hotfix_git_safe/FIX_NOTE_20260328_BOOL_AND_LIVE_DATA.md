# FIX_NOTE_20260328_BOOL_AND_LIVE_DATA

이번 전체 패키지에서 반영한 수정:

1. `safe_bool` import 누락 수정
   - `ipo_repository.py` 대시보드 alert 계산 시 `NameError: safe_bool`가 나던 문제 해결.

2. `pd.NA` boolean 평가 버그 제거
   - `infer_issue_stage()`의 `fallback or ""` 제거.
   - `normalize_name_key()`, `clean_column_label()`, `parse_date_text()`, `parse_date_range_text()`, `humanize_source()` 등에서 `pd.NA`가 섞여도 안전하게 처리.
   - `safe_float()`가 `1,234.56:1`, `47,200 원 (0.21%)` 같은 문자열도 숫자로 읽도록 보강.

3. IPO 라이브 데이터 소스 보강
   - 기존: `KIND 신규상장기업현황` + `38 공모주 일정`
   - 추가: `KIND 공모기업현황`, `KIND 공모가대비주가정보`
   - 목적:
     - `subscription_start/end` 빈칸 감소
     - `listing_date` 보강
     - `current_price` 보강
     - `market`/`underwriters`/`offer_price` 보강

4. 38 상세 페이지 파싱 추가
   - 스케줄 표에서 상세 링크를 찾아 상세 페이지까지 읽음.
   - 보강 대상:
     - 시장구분
     - 종목코드
     - 업종
     - 청약일정
     - 상장일
     - 기관경쟁률
     - 의무보유확약
     - 현재가

5. 통합 병합 로직 개선
   - 병합 순서:
     - KIND 신규상장
     - KIND 공모기업
     - 38 일정/상세
     - KIND 공모가대비주가정보
   - `미상`/`pd.NA`가 정상값을 덮어쓰지 않도록 수정.

6. Streamlit 캐시 버전 갱신
   - 내부 `CACHE_REV`를 올려 오래된 캐시 결과를 자동 우회.

실행 권장 순서:

```bash
python scripts/refresh_live_cache.py
streamlit run app.py
```

새 폴더에서 실행하는 것이 가장 안전하며, 이전 폴더에서 재실행한다면 한 번만 아래 명령을 같이 실행:

```bash
streamlit cache clear
```
