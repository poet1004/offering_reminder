# 모바일 피드 자동 배포

이 저장소는 `ipo_git/data/cache/*.csv`를 기준으로 모바일 앱용 JSON 피드를 자동 생성해 GitHub Pages에 올릴 수 있습니다.

## 한 번만 해둘 설정

1. GitHub 저장소에서 **Settings → Pages** 로 이동합니다.
2. **Build and deployment → Source** 를 `GitHub Actions` 로 바꿉니다.
3. `main` 브랜치에 이 파일들과 `.github/workflows/mobile-feed-pages.yml` 를 push 합니다.

## 자동으로 생성되는 파일

워크플로가 성공하면 아래 URL들이 생깁니다.

- `https://<깃헙아이디>.github.io/<저장소명>/mobile-feed.json`
- `https://<깃헙아이디>.github.io/<저장소명>/health.json`
- `https://<깃헙아이디>.github.io/<저장소명>/`

예시:

- `https://poet1004.github.io/offering_reminder/mobile-feed.json`

## 언제 갱신되나

아래 파일이 `main` 브랜치에 push 되면 자동 실행됩니다.

- `ipo_git/data/cache/**`
- `ipo_git/data/bootstrap_cache/**`
- `ipo_git/data/sample_ipo_events.csv`
- `ipo_git/data/sample_market_snapshot.csv`
- `ipo_git/scripts/export_mobile_feed.py`

또는 **Actions → mobile-feed-pages → Run workflow** 로 수동 실행할 수 있습니다.

## 로컬 테스트

```bash
python ipo_git/scripts/export_mobile_feed.py \
  --repo ipo_git \
  --site-dir mobile-pages \
  --site-base-url "https://poet1004.github.io/offering_reminder"
```

그 다음 `mobile-pages/mobile-feed.json` 이 생성되면 정상입니다.

## 모바일 앱 연결

모바일 앱의 피드 URL은 아래 둘 중 하나로 넣으면 됩니다.

- 앱의 `.env` 에 `EXPO_PUBLIC_FEED_URL=https://poet1004.github.io/offering_reminder/mobile-feed.json`
- 앱 설정 화면에서 같은 주소를 직접 입력
