# 공모주 레이더 + Lockup Unified Lab 올인원

이 프로젝트는 Streamlit 앱과 `ipo_lockup_unified_lab`를 같은 폴더 안에 묶은 버전입니다.

구성
- 앱: `app.py`
- 통합 lab: `integrated_lab/ipo_lockup_unified_lab`
- 통합 workspace: `integrated_lab/ipo_lockup_unified_lab/workspace`

처음 할 일
1. `python scripts/sync_env_to_lab_keys.py`
2. `python scripts/prepare_integrated_lab_workspace.py`
3. 앱 실행: `streamlit run app.py`
4. 분봉/락업 연구 산출물 생성: `run_integrated_lab_wizard.bat`

메모
- 앱은 통합 workspace를 자동 탐지합니다.
- 아직 산출물이 없으면 락업 매수전략 / 5분봉 브리지 / 전략 브릿지 페이지는 비어 있을 수 있습니다.
- `real_key.txt`, `practice_key.txt`, `dart_key.txt`는 `.env.local`에서 자동 생성됩니다.


2026-03-28 v3
- run_integrated_lab_wizard.bat now uses an absolute Python path and leaves the console open with a pre-step log.
- 메뉴 2(build-dataset)는 앱 live/cache IPO bundle을 자동으로 integrated lab용 kind_master.csv로 내보낸 뒤 실행한다.
- KIND corpList 또는 로컬 KIND Excel이 없어도 app seed/38 보조 데이터로 dataset 생성을 계속 시도한다.
- workspace/logs 에 메뉴별 실행 로그가 저장된다.
