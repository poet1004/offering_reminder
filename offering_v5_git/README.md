# 공모주 레이더

공모주 일정, 상장 종목, 보호예수 해제, 전략 연구, 쇼츠 자산 생성을 한곳에서 보는 Streamlit 앱입니다.

## 빠른 실행

### 로컬
- `run_refresh_live_cache.bat` 또는 `run_refresh_live_cache.sh`
- `run_app.bat` 또는 `run_app.sh`

### 쇼츠 자산 생성
- `run_generate_daily_shorts.bat` 또는 `run_generate_daily_shorts.sh`

## 주요 메뉴
- 대시보드: 달력 + 시장 스냅샷 중심
- 딜 탐색기: 종목 필터 탐색
- 청약 / 상장 / 보호예수
- 전략 연구실: 락업 전략, 5분봉 브리지, 전략 브릿지, 턴오버 전략
- 백테스트: 전략 성과 + 상장 후 보유 가정
- 쇼츠 스튜디오: 스크립트 초안 생성 → 편집 → 자산 생성
- 데이터 / 설정

## GitHub에 올릴 때
- 공개 저장소에는 `offering_v*_git.zip` 기준 파일만 사용하세요.
- 로컬 저장소 폴더에서는 `.git` 폴더만 남기고 나머지를 새 버전 파일로 교체한 뒤 커밋/푸시하면 됩니다.
- Streamlit Community Cloud 메인 파일 경로는 `app.py` 입니다.

## 문서
- `docs/DEPLOY.md`
- `docs/SHORTS.md`
- `docs/TROUBLESHOOTING.md`
- `docs/CHANGELOG.md`
