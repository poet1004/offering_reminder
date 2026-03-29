# env 파일 안내

- `.env.local` : 현재 기본값. 실전 KIS + DART 키로 설정됨.
- `.env.real` : 실전용 고정본.
- `.env.practice` : 모의투자 기본본. 이 파일은 `KIS_ENV=demo` 로 맞춰져 있음.

전환 방법:
1. 모의투자를 쓰려면 `.env.practice` 내용을 `.env.local`로 복사
2. 실전을 쓰려면 `.env.real` 내용을 `.env.local`로 복사
3. 실행 전 `python scripts/preflight_check.py`로 확인
