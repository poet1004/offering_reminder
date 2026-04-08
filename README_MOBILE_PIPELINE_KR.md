# offering_reminder 저장소에 모바일 피드 자동 배포 붙이는 방법

이 압축 파일은 저장소 루트 기준으로 덮어쓰기하는 용도입니다.

## 들어 있는 것
- `.github/workflows/mobile-feed-pages.yml`
- `ipo_git/scripts/export_mobile_feed.py`
- `ipo_git/docs/MOBILE_FEED_PIPELINE_KR.md`
- 갱신된 `ipo_git` 폴더

## 적용 순서
1. 현재 GitHub 저장소 로컬 폴더를 엽니다.
2. 이 압축을 풀어 나온 파일들을 저장소 루트에 덮어씁니다.
3. GitHub Desktop에서 commit 후 push 합니다.
4. GitHub 저장소 Settings → Pages → Source 를 `GitHub Actions` 로 바꿉니다.
5. Actions 탭에서 `mobile-feed-pages` 가 성공하면 아래 주소가 열립니다.
   - `https://poet1004.github.io/offering_reminder/mobile-feed.json`

## 앱 연결
모바일 앱 `.env` 에 아래를 넣거나 앱 설정 화면에 직접 입력합니다.

```bash
EXPO_PUBLIC_FEED_URL=https://poet1004.github.io/offering_reminder/mobile-feed.json
```
