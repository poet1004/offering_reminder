# GitHub Pages 정적 웹 전환 보고서

## 핵심 변경
- Streamlit 서버 앱과 별도로 GitHub Pages용 정적 웹을 추가
- `static_pages/` 에 정적 SPA(대시보드/청약/상장/보호예수/딜 탐색기/쇼츠 스튜디오/데이터 상태) 추가
- `scripts/build_pages_site.py` 로 `_site` 정적 출력 생성
- `scripts/refresh_export_and_build_pages.py` 로 캐시 갱신 + 모바일 피드 + 정적 사이트를 한 번에 생성
- `.github/workflows/github_pages_static.yml` 추가
- `docs/GITHUB_PAGES_KR.md` 배포 절차 문서 추가

## 로컬 명령
```bash
python scripts/build_pages_site.py --repo . --output _site
python scripts/refresh_export_and_build_pages.py --skip-refresh
```

## 생성물
- `_site/index.html`
- `_site/assets/app.css`
- `_site/assets/app.js`
- `_site/data/mobile-feed.json`
- `_site/data/backtest-summary.json`
- `_site/data/mobile-feed-verify.json`

## 검증
- `python -m py_compile scripts/build_pages_site.py scripts/refresh_export_and_build_pages.py`
- `node --check static_pages/assets/app.js`
- `python scripts/build_pages_site.py --repo . --output _site`
- `python scripts/refresh_export_and_build_pages.py --skip-refresh`
