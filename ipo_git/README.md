# 공모주 알리미

> Windows 설치 시에는 **Python 3.11 64-bit(x64)** 를 사용하세요. `pandas`가 source build로 내려가면 Visual Studio 빌드 오류가 납니다.

공모주 일정, 수요예측 결과, 38 IR 자료, Seibro 보호예수 해제물량, 전략 연구, 쇼츠 자산 생성을 한곳에서 보는 Streamlit 앱입니다.

## 배포 경로

- GitHub/Streamlit 기준 폴더명은 **ipo_git**으로 고정했습니다.
- Streamlit Community Cloud의 메인 파일 경로는 **`ipo_git/app.py`** 로 맞추면 됩니다.
- 같은 경로를 계속 유지하려면 다음 배포부터도 저장소 안의 `ipo_git` 폴더만 교체하세요.

## Python 버전 / 설치

- **권장: Python 3.11.x (64-bit)**
- 이 프로젝트는 기본 웹 앱 기준으로 **Python 3.11 설치를 권장**합니다.
- 가장 쉬운 방법은 `setup_py311.bat` 또는 `setup_py311.sh` 를 먼저 실행한 뒤 `run_app.bat` / `run_app.sh` 를 실행하는 것입니다.
- `integrated_lab`의 KIS 보조 스크립트까지 쓰려면 `requirements-optional.txt`도 추가 설치하세요.
- 자세한 절차는 `PYTHON_SETUP_KR.md`를 보세요.

## 빠른 실행

### 로컬
- `run_refresh_live_cache.bat` 또는 `run_refresh_live_cache.sh`
- `run_app.bat` 또는 `run_app.sh`

### 쇼츠 자산 생성
- `run_generate_daily_shorts.bat` 또는 `run_generate_daily_shorts.sh`

## 주요 메뉴
- 대시보드: 시장 요약 + 6개월 일정 캘린더 + 가까운 일정 카드
- 딜 탐색기: 종목 필터 탐색
- 청약 / 상장 / 보호예수: 수요예측 결과·IR 자료·해제물량까지 연계
- 실험실: 락업 실행보드 + 5분봉 브리지 + 턴오버 연구 + 백테스트 + 쇼츠 스튜디오
- 데이터 / 설정

## GitHub에 올릴 때
- 공개 저장소에는 `ipo_git.zip` 기준 파일만 사용하세요.
- 로컬 저장소 폴더에서는 `.git` 폴더만 남기고 나머지를 새 버전 파일로 교체한 뒤 커밋/푸시하면 됩니다.
- Streamlit Community Cloud 메인 파일 경로는 `ipo_git/app.py` 입니다.

## 문서
- `docs/DEPLOY.md`
- `docs/SHORTS.md`
- `docs/TROUBLESHOOTING.md`
- `docs/CHANGELOG.md`


## 배포 메모
- `data/cache`의 상위 CSV/meta 파일은 배포 부트스트랩용 일정/시장 캐시입니다.
- `run_refresh_live_cache.*` 실행 후 GitHub에 올릴 때는 `data/cache` 변경도 함께 커밋해야 Streamlit 배포본에 반영됩니다.


## 모바일 피드 내보내기

```bash
python scripts/export_mobile_feed.py --repo . --output data/mobile/mobile-feed.json
python scripts/export_mobile_feed.py --repo . --site-dir mobile-feed --site-base-url https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed
```

이제 모바일 피드는 `IPODataHub.load_bundle()` 기준으로 만들어져 상장일과 보호예수 해제일 이벤트까지 함께 포함됩니다.

Windows에서는 `run_export_mobile_feed.bat` 를 바로 실행해도 됩니다.


## 원클릭 파이프라인

모바일 피드를 GitHub/앱용으로 한 번에 갱신하려면 아래를 쓰면 됩니다.

- Windows: `run_refresh_and_export_mobile_feed.bat`
- macOS/Linux: `bash run_refresh_and_export_mobile_feed.sh`

내부적으로 아래 순서로 실행됩니다.

```bash
python scripts/refresh_and_export_mobile_feed.py
```

이 스크립트는 가능한 범위에서 live cache를 갱신하고, `data/mobile/mobile-feed.json` 과 repo 루트 `mobile-feed/` 정적 폴더를 함께 다시 만들고, 마지막으로 `scripts/verify_mobile_feed.py` 로 listing / unlock 이벤트 포함 여부까지 검증합니다.


## GitHub Pages 정적 웹

GitHub Pages는 정적 사이트 전용이라 Streamlit 앱 자체를 올리지 않고, 이 저장소는 `mobile-feed/mobile-feed.json` 을 읽는 정적 웹도 함께 생성하도록 바뀌었습니다.

- 정적 사이트 빌드: `run_build_pages_site.bat` 또는 `python scripts/build_pages_site.py --repo . --output _site`
- 전체 갱신 + 정적 사이트: `run_refresh_export_and_build_pages.bat` 또는 `python scripts/refresh_export_and_build_pages.py`
- 자세한 절차: `docs/GITHUB_PAGES_KR.md`

## 느린 환경에서의 기본 동작

목록 탭 성능을 위해 기본값은 **캐시/시드 우선**입니다.
실시간 HTML 보강까지 강제로 켜고 싶으면 `.env` 또는 secrets에 아래를 넣으세요.

```env
IPO_ALLOW_INLINE_FETCH=1
```
