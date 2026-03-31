# 공모주 알리미

공모주 일정, 상장 종목, 보호예수 해제, 실험실 기능까지 한곳에서 보는 Streamlit 앱입니다.

## 빠른 실행

### 로컬
- `run_refresh_live_cache.bat` 또는 `run_refresh_live_cache.sh`
- `run_app.bat` 또는 `run_app.sh`

### 쇼츠 자산 생성
- `run_generate_daily_shorts.bat` 또는 `run_generate_daily_shorts.sh`

## 주요 메뉴
- 대시보드: 시장 요약 + 6개월 일정 캘린더
- 딜 탐색기: 종목 필터 탐색
- 청약 / 상장 / 보호예수
- 실험실: 전략 연구실 + 백테스트 + 쇼츠 스튜디오
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


## 배포 메모
- `data/cache`의 상위 CSV/meta 파일은 배포 부트스트랩용 일정/시장 캐시입니다.
- `run_refresh_live_cache.*` 실행 후 GitHub에 올릴 때는 `data/cache` 변경도 함께 커밋해야 Streamlit 배포본에 반영됩니다.
