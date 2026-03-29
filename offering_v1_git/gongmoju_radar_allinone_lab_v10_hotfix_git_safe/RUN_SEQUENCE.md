# 실행 순서 (v8)

## 0. 처음 1회
1. 프로젝트 압축 해제
2. `.env.local`에 KIS / DART 키 입력
3. 필요하면 `bootstrap_clean_venv.bat` 실행

## 1. 앱 기본 화면만 확인할 때
`run_app.bat`

이 단계만으로 정상이어야 하는 페이지:
- 대시보드
- 딜 탐색기
- 청약
- 상장
- 시장
- DART 자동추출

이 단계에서 비어 있어도 정상인 페이지:
- 락업 매수전략
- 전략 브릿지
- 5분봉 브리지

위 3개는 integrated lab 산출물이 있어야 채워집니다.

## 2. 락업 전략 후보까지 보려면
1. `run_integrated_lab_wizard.bat`
2. 메뉴 `2` 실행
3. `run_app.bat`

메뉴 `2`가 끝나면 아래 파일이 생겨야 합니다.
- `integrated_lab/ipo_lockup_unified_lab/workspace/dataset_out/synthetic_ipo_events.csv`

이 파일만 생겨도 앱은 내부 workspace를 자동 연결합니다.
사이드바의 `내장 데모 workspace 자동연결`은 **샘플 데모용**이므로 실제 integrated lab에는 필요 없습니다.

## 3. 5분봉 브리지까지 보려면
1. `run_integrated_lab_wizard.bat`
2. 메뉴 `2`
3. 메뉴 `4`
4. 메뉴 `5`
5. 메뉴 `6`
6. 메뉴 `7` (외부 minute CSV가 있을 때만)
7. 메뉴 `8`
8. 메뉴 `9`
9. 메뉴 `10`
10. `run_app.bat`

## 4. 어떤 버튼을 꼭 눌러야 하나?
- `run_app.bat`만 실행: 기본 IPO 앱 화면만 확인
- `run_integrated_lab_wizard.bat` + 메뉴 실행: 락업/전략/5분봉 데이터 생성
- `내장 데모 workspace 자동연결`: 실제 데이터가 아니라 샘플 데모를 보고 싶을 때만

## 5. 정상 여부 빠른 체크
- 딜 탐색기/청약/상장/시장/DART만 볼 거라면 `run_app.bat`만으로 데이터가 나와야 정상
- 락업 매수전략/전략 브릿지/5분봉 브리지가 0건이면 integrated lab 메뉴를 아직 안 돌렸을 가능성이 큼
- menu 2 뒤 `backtest_out`이 비어 있는 것은 정상. `backtest_out`은 menu 3 이후 생성됨
