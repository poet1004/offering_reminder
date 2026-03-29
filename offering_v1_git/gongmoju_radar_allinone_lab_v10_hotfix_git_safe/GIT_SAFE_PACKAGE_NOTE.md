# Git-safe package note

이 패키지는 GitHub / Streamlit Community Cloud 업로드용으로 정리된 버전입니다.

제거한 항목:
- `.env.local`, `.env.real`, `.env.practice`
- `.streamlit/secrets.toml`
- integrated_lab 키 파일(`real_key.txt`, `practice_key.txt`, `dart_key.txt`, `token.dat`)
- `data/runtime/`
- integrated_lab 대형 캐시(`workspace/cache_dart`, `workspace/cache_kis`, `workspace/logs`)
- 로컬 경로가 남아 있던 디버그 산출물

남겨둔 항목:
- `.env.example`
- `.streamlit/secrets.example.toml`
- 공모주 시드 데이터 및 전략용 workspace 산출물 일부
