# GitHub Pages 루트 저장소 패치 보고서

## 무엇을 고쳤나
- 현재 `poet1004/offering_reminder` 저장소 구조(루트에 `ipo_git/` 폴더가 있는 구조)에 맞춰 워크플로 경로를 다시 맞춤
- `mobile-feed-pages.yml` 추가/갱신
  - `python ipo_git/scripts/export_mobile_feed.py --repo ipo_git --output-dir mobile-feed ...` 형태로 실행
- `github-pages-static.yml` 추가
  - `mobile-feed/mobile-feed.json`을 받아 `_site` 정적 사이트를 생성하고 GitHub Pages에 배포
- `ipo_git/scripts/export_mobile_feed.py` 호환성 보강
  - 신형 인자: `--output-dir`, `--public-base-url`, `--fallback-base-url`
  - 구형 인자: `--site-dir`, `--site-base-url`, `--prefer-live`, `--no-cache`
  - 위 인자들을 모두 받아도 실패하지 않게 수정
- `ipo_git/scripts/build_pages_site.py` 수정
  - 피드 파일이 `ipo_git/` 바깥(`mobile-feed/mobile-feed.json`)에 있어도 정상 빌드
- `ipo_git/static_pages/` 포함
  - GitHub Pages용 정적 SPA

## 검증
로컬 테스트 기준으로 아래 명령이 모두 통과함.

```bash
python ipo_git/scripts/export_mobile_feed.py --repo ipo_git --output-dir mobile-feed --public-base-url https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed --fallback-base-url https://raw.githubusercontent.com/poet1004/offering_reminder/main/mobile-feed
python ipo_git/scripts/build_pages_site.py --repo ipo_git --feed mobile-feed/mobile-feed.json --output _site
python -m py_compile ipo_git/scripts/export_mobile_feed.py ipo_git/scripts/build_pages_site.py ipo_git/scripts/refresh_export_and_build_pages.py
```

## 적용 위치
이 압축은 **저장소 루트**에 그대로 덮어써야 함.

예상 루트 구조:
- `.github/workflows/...`
- `ipo_git/...`
- `mobile-feed/...`
