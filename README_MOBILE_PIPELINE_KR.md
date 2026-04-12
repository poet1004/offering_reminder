# offering_reminder 저장소에 모바일 피드 자동 배포 붙이는 방법

이 압축 파일은 저장소 루트 기준으로 덮어쓰기하는 용도입니다.

## 들어 있는 것
- `.github/workflows/mobile-feed-pages.yml`  ← Pages 없이 JSON을 생성해 저장소에 커밋
- `ipo_git/scripts/export_mobile_feed.py`
- `ipo_git/docs/MOBILE_FEED_PIPELINE_KR.md`
- 갱신된 `ipo_git` 폴더
- `mobile-feed/` 초기 출력물 샘플

## 이번 방식의 핵심
- GitHub Pages를 쓰지 않습니다.
- GitHub Actions가 `mobile-feed/` 폴더를 갱신해서 main 브랜치에 커밋합니다.
- 모바일 앱은 jsDelivr 주소를 기본으로 읽고, 실패하면 GitHub Raw 주소를 대체로 시도합니다.

## 적용 순서
1. 현재 GitHub 저장소 로컬 폴더를 엽니다.
2. 이 압축을 풀어 나온 파일들을 저장소 루트에 덮어씁니다.
3. GitHub Desktop에서 commit 후 push 합니다.
4. Actions 탭에서 `mobile-feed-cdn` 이 성공하면 아래 주소가 열립니다.

## 기본 주소 예시
- jsDelivr: `https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed/mobile-feed.json`
- GitHub Raw: `https://raw.githubusercontent.com/poet1004/offering_reminder/main/mobile-feed/mobile-feed.json`

## 앱 연결
모바일 앱 `.env` 에 아래를 넣거나 앱 설정 화면에 직접 입력합니다.

```bash
EXPO_PUBLIC_FEED_URL=https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed/mobile-feed.json
```
