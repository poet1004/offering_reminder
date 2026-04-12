# 모바일 피드 내보내기

이 저장소의 모바일 피드는 `IPODataHub.load_bundle()` 기준으로 생성됩니다. 따라서 청약 일정뿐 아니라 상장일, 보호예수 해제일, 시장 지표, 원본 캐시 신선도 시각도 함께 포함됩니다.

## 빠른 실행

- Windows: `run_export_mobile_feed.bat`
- macOS/Linux: `bash run_export_mobile_feed.sh`

## 수동 실행

```bash
python scripts/export_mobile_feed.py --repo . --output data/mobile/mobile-feed.json
```

jsDelivr / GitHub Raw에서 바로 읽을 수 있는 repo 루트 정적 폴더를 만들려면:

```bash
python scripts/export_mobile_feed.py --repo . --site-dir mobile-feed --site-base-url https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed
```

기존 보조 출력 폴더를 유지하고 싶다면 아래처럼도 가능합니다.

```bash
python scripts/export_mobile_feed.py --repo . --site-dir data/mobile/site --site-base-url https://example.invalid/mobile
```


## 권장 실행

실무에서는 아래 한 줄이 가장 편합니다.

```bash
python scripts/refresh_and_export_mobile_feed.py
```

이 명령은 다음을 한 번에 처리합니다.

- KIND / 38 / 시장 캐시 best-effort 갱신
- `data/mobile/mobile-feed.json` 생성
- repo 루트 `mobile-feed/` 정적 폴더 생성
- `scripts/verify_mobile_feed.py` 검증 리포트 저장

생성 리포트:

- `data/runtime/mobile_feed_pipeline_report.json`
- `data/runtime/mobile_feed_verify.json`

## GitHub 자동 갱신

`.github/workflows/mobile_feed_sync.yml` 이 들어 있어, 수동 실행과 스케줄 실행에서 모바일 피드를 다시 만들고 변경이 있으면 커밋까지 할 수 있습니다.

주의:
- KIND HTML 구조가 바뀌면 live 갱신이 일부 실패할 수 있습니다.
- 그래도 이번 버전은 0건 응답으로 기존 정상 캐시를 덮어쓰지 않도록 막아두었습니다.

## 피드 버전

현재 모바일 피드는 `schemaVersion = 2` 를 사용합니다. 앱은 이 값을 기준으로 구형 subscription-only 피드를 감지해 안내를 띄울 수 있습니다.
