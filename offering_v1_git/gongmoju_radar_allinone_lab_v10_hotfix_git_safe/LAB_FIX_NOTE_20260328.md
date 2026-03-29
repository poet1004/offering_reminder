Lab 통합 수정 요약 (2026-03-28)

이번 수정의 핵심
- run_integrated_lab_wizard.bat 가 더 이상 상대경로 Python 때문에 바로 종료되지 않음
- integrated lab 메뉴 2(build-dataset)가 같은 프로젝트 안의 앱 live/cache IPO bundle을 자동으로 seed(kind_master.csv)로 내보낸 뒤 실행됨
- KIND 엑셀을 수동으로 내려받지 않아도 dataset 생성이 가능하도록 fallback 추가
- KIND/38 네트워크 실패 시 조기 종료하도록 바꿔서 장시간 멈춤을 줄임
- workspace/logs 에 메뉴별 실행 로그 저장

권장 실행 순서
1. run_app.bat 로 앱 확인
2. run_integrated_lab_wizard.bat 실행
3. 메뉴 2 -> dataset 생성
4. 메뉴 4 -> DART unlock 생성
5. 메뉴 5 -> backtest 입력형식 변환
6. 메뉴 6~10 순서대로 minute/signals/backtest/beta

실행 시 참고
- 메뉴 2는 먼저 scripts/export_ipo_seed_to_lab.py 를 자동 실행한다.
- live seed export 가 실패해도 기존 seed 파일이 있으면 build-dataset 은 계속 진행된다.
- build-dataset 이 실패하면 integrated_lab/ipo_lockup_unified_lab/workspace/logs 를 먼저 확인한다.
