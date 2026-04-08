이 패치는 모바일 피드 생성 전에 공개 소스 기반 시장 스냅샷을 먼저 새로고침합니다.

적용:
1. 압축을 풀어 저장소 루트에 덮어쓰기
2. Commit / Push
3. GitHub Actions에서 `mobile-feed-cdn` 수동 실행
4. 완료 후 다음 주소 확인
   - https://raw.githubusercontent.com/poet1004/offering_reminder/main/mobile-feed/mobile-feed.json
   - https://cdn.jsdelivr.net/gh/poet1004/offering_reminder@main/mobile-feed/mobile-feed.json
