# GitHub Pages 정적 배포 가이드

## 왜 구조를 바꿨나
GitHub Pages는 정적 HTML/CSS/JavaScript 파일을 배포하는 서비스라서 Streamlit 서버 앱을 그대로 올릴 수 없습니다.
이 저장소는 `mobile-feed/mobile-feed.json`과 백테스트 요약을 이용해 브라우저에서 바로 동작하는 정적 웹을 생성하도록 바뀌었습니다.

## 로컬에서 정적 사이트 만들기
```bash
python scripts/build_pages_site.py --repo . --output _site
```

또는

- Windows: `run_build_pages_site.bat`
- macOS/Linux: `run_build_pages_site.sh`

### 캐시 갱신 + 피드 생성 + 정적 사이트까지 한 번에
```bash
python scripts/refresh_export_and_build_pages.py
```

또는

- Windows: `run_refresh_export_and_build_pages.bat`
- macOS/Linux: `run_refresh_export_and_build_pages.sh`

## 로컬 미리보기
```bash
python -m http.server 8000 -d _site
```
브라우저에서 `http://localhost:8000` 을 열면 됩니다.

## GitHub Pages에 올리는 법
1. 이 git용 압축본으로 저장소 파일을 덮어쓴다.
2. `Commit to main` 후 `Push origin` 한다.
3. GitHub 저장소 `Settings > Pages` 로 이동한다.
4. `Build and deployment` 의 `Source` 를 `GitHub Actions` 로 바꾼다.
5. `Actions` 탭에서 `github-pages-static` 워크플로가 성공하면 사이트가 배포된다.

## 자동 배포 흐름
- `mobile-feed-sync` 가 모바일 피드를 갱신하면
- `github-pages-static` 가 그 결과를 받아 `_site` 를 만들고
- GitHub Pages에 배포합니다.

## 정적 웹에서 보이는 탭
- 대시보드
- 청약
- 상장
- 보호예수
- 딜 탐색기
- 쇼츠 스튜디오
- 데이터 상태

## 다른 정적 호스팅에도 쓸 수 있나
가능합니다. `_site` 폴더는 일반적인 정적 사이트 출력물이라서 다른 정적 호스팅에도 그대로 업로드할 수 있습니다.
