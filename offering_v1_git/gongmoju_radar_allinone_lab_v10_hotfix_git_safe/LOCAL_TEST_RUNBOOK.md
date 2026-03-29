# 로컬 테스트 런북

집에 가서 바로 확인할 때는 아래 순서대로 하면 됩니다.

## 1. 처음 한 번만

### Windows
```bat
run_prepare_local_test.bat
```

### macOS / Linux / WSL
```bash
./run_prepare_local_test.sh
```

이 단계에서 자동으로 하는 일:
- `requirements.txt` 설치
- `.env` 자동 로드 시도
- preflight 체크 실행
- smoke test 실행
- 전략 보드 / execution bridge / 주문시트 생성
- 드라이런 실행계획(`data/runtime`) 생성

## 2. Unified Lab 결과를 붙이고 싶으면

workspace 폴더가 따로 있으면:
```bash
python scripts/prepare_local_test.py --workspace C:\path\to\workspace
```

zip만 있으면:
```bash
python scripts/prepare_local_test.py --workspace-zip C:\path\to\workspace.zip --clean-import-dir
```

## 3. API 키 넣기

프로젝트 루트에 `.env` 파일을 두고 아래 형식으로 채웁니다.

```env
KIS_APP_KEY=...
KIS_APP_SECRET=...
KIS_ENV=real
DART_API_KEY=...
```

중요:
- 이번 버전부터 `.env`를 자동으로 읽습니다.
- 기존처럼 OS 환경변수로 직접 넣어도 그대로 동작합니다.

## 4. 앱 실행

### Windows
```bat
run_app.bat
```

### 공통
```bash
streamlit run app.py
```

## 5. 먼저 보면 좋은 파일

- `data/runtime/preflight_report.json`
- `data/runtime/prepare_latest_manifest.json`
- `data/runtime/runtime_v2_0_plan_YYYYMMDD.csv`
- `data/runtime/runtime_v2_0_payload_YYYYMMDD.json`
- `data/runtime/runtime_v2_0_dry_run_YYYYMMDD.csv`

## 6. 예상되는 경고

- `streamlit` 미설치: 앱만 아직 못 띄운 상태이므로 `pip install -r requirements.txt` 후 재실행
- `YahooHTTP` 요청 실패: 해외지수/선물/환율/원자재가 비어 있을 수 있음
- `missing_reference_price`: 실시간 현재가 또는 분봉 신호 진입가가 없어 수량 계산을 건너뜀
- `entry_date_passed`: 기준일이 지나간 후보라 드라이런에서 `LATE`로 분류됨

## 7. 가장 빠른 확인 루트

```bash
python scripts/preflight_check.py
python scripts/prepare_local_test.py --workspace data/sample_unified_lab_workspace
streamlit run app.py
```
