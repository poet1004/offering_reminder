# 모바일 피드 자동 배포

이 저장소는 `ipo_git/data/cache/*.csv`를 기준으로 모바일 앱용 JSON 피드를 자동 생성합니다.

## 이번 방식
- GitHub Pages를 쓰지 않습니다.
- GitHub Actions가 `mobile-feed/` 폴더를 갱신하고, main 브랜치에 자동 커밋합니다.
- 모바일 앱은 jsDelivr 주소를 기본으로 읽고, 필요하면 GitHub Raw 주소를 대체 경로로 사용합니다.

## 자동으로 갱신되는 파일
`mobile-feed/` 아래에 아래 파일들이 생깁니다.

- `mobile-feed/mobile-feed.json`
- `mobile-feed/health.json`
- `mobile-feed/index.html`
- `mobile-feed/README.md`

## 공개 주소 예시
- 기본: `https://cdn.jsdelivr.net/gh/<깃헙아이디>/<저장소명>@main/mobile-feed/mobile-feed.json`
- 대체: `https://raw.githubusercontent.com/<깃헙아이디>/<저장소명>/main/mobile-feed/mobile-feed.json`

예시:
- `https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed/mobile-feed.json`
- `https://raw.githubusercontent.com/poet1004/offering_reminder/main/mobile-feed/mobile-feed.json`

## 언제 갱신되나
아래 파일이 `main` 브랜치에 push 되면 자동 실행됩니다.

- `ipo_git/data/cache/**`
- `ipo_git/data/bootstrap_cache/**`
- `ipo_git/data/sample_ipo_events.csv`
- `ipo_git/data/sample_market_snapshot.csv`
- `ipo_git/scripts/export_mobile_feed.py`
- `.github/workflows/mobile-feed-pages.yml`

또는 **Actions → mobile-feed-cdn → Run workflow** 로 수동 실행할 수 있습니다.

## 로컬 테스트
```bash
python ipo_git/scripts/export_mobile_feed.py   --repo ipo_git   --output-dir mobile-feed   --public-base-url "https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed"   --fallback-base-url "https://raw.githubusercontent.com/poet1004/offering_reminder/main/mobile-feed"
```

그 다음 `mobile-feed/mobile-feed.json` 이 생성되면 정상입니다.

## 모바일 앱 연결
모바일 앱의 피드 URL은 아래 둘 중 하나로 넣으면 됩니다.

- 앱의 `.env` 에 `EXPO_PUBLIC_FEED_URL=https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed/mobile-feed.json`
- 앱 설정 화면에서 같은 주소를 직접 입력
