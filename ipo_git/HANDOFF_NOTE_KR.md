# 다음 대화용 메모

이 버전은 `ipo v10` 기준 정리본입니다.

## 핵심 변경
- 목록 탭 성능 개선: 페이지 목록은 기본적으로 **캐시/시드 기반 보강만** 사용하고, 종목 선택 시에만 상세 보강을 강하게 수행합니다.
- 인라인 실시간 보강은 기본적으로 꺼져 있습니다. 필요하면 `.env` 또는 secrets에 아래를 넣으면 됩니다.

```env
IPO_ALLOW_INLINE_FETCH=1
```

기본값이 꺼져 있는 이유는 느린 네트워크/차단 환경에서 목록 탭 전체가 멈추는 현상을 줄이기 위해서입니다.

## 모바일 연동
- `scripts/export_mobile_feed.py`
- `run_export_mobile_feed.bat` / `run_export_mobile_feed.sh`

로 현재 저장소의 캐시를 모바일 앱용 `data/mobile/mobile-feed.json` 으로 변환할 수 있습니다.

## 실행
- 웹 앱: `run_app.bat`
- 캐시 새로고침: `run_refresh_live_cache.bat`
- 모바일 피드 내보내기: `run_export_mobile_feed.bat`
