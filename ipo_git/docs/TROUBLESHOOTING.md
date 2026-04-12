# 트러블슈팅

## 빈 CSV 오류
- 최신 버전은 빈 CSV/BOM-only CSV를 자동으로 빈 DataFrame으로 처리한다.
- 이전 폴더를 실행 중이면 오류가 남을 수 있으니, 예전 폴더를 닫고 새 폴더에서 다시 실행한다.

## Streamlit Cloud에서 오래 구워지는 경우
- `environment.yml`이 남아 있지 않은지 확인
- Main file path가 `ipo_git/app.py`인지 확인 (저장소 루트에 `ipo_git` 폴더째 올린 경우)
- Secrets가 올바른지 확인

## KIS 403
- `KIS_ENV`와 App Key/App Secret 조합이 맞는지 확인
- 로컬 `.env.local` 또는 Streamlit Secrets 값이 올바른지 확인

## 쇼츠 생성 오류
- 먼저 `스크립트 초안 생성`을 눌러 payload를 만든다.
- MP4는 선택 옵션이며, 영상 편집이 목적이면 ZIP과 스크립트만 먼저 받는 것을 권장한다.
