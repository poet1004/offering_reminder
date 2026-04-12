# 적용 방법

이 압축은 `poet1004/offering_reminder` 저장소 **루트**에 덮어쓰는 패치입니다.

## 1) 덮어쓸 파일
- `.github/workflows/mobile-feed-pages.yml`
- `.github/workflows/github-pages-static.yml`
- `ipo_git/scripts/export_mobile_feed.py`
- `ipo_git/scripts/build_pages_site.py`
- `ipo_git/scripts/refresh_export_and_build_pages.py`
- `ipo_git/static_pages/*`

## 2) GitHub 저장소에 반영
1. 저장소 루트에 압축을 풀어 덮어쓰기
2. Commit 후 Push
3. GitHub 저장소 `Settings > Pages` 에서 Source 를 **GitHub Actions** 로 변경
4. `Actions` 탭에서 `mobile-feed-pages` 를 먼저 한 번 실행
5. 성공 후 `github-pages-static` 를 실행하거나, 자동 실행을 기다림

## 3) 삭제할 필요 없는 것
- `mobile-feed/` 폴더는 **삭제하지 않아도 됩니다**.
- 워크플로가 새로 생성/갱신합니다.

## 4) 사이트 주소
- 기본 주소: `https://poet1004.github.io/offering_reminder/`
