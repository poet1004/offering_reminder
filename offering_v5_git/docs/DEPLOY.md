# 배포 가이드

## GitHub Desktop으로 업데이트
1. 기존 저장소 폴더를 연다.
2. `.git` 폴더는 건드리지 않는다.
3. 새 버전 git용 압축을 풀고, 그 안의 파일들로 기존 프로젝트 파일을 덮어쓴다.
4. GitHub Desktop에서 변경사항을 확인한다.
5. `Commit to main` 후 `Push origin`을 누른다.

## Streamlit Community Cloud
- Repository: GitHub 저장소
- Branch: `main`
- Main file path: `app.py`
- Secrets: `.streamlit/secrets.toml` 내용을 Cloud Secrets에 입력
- `requirements.txt` 기준으로 배포

## 주의
- 공개 GitHub에는 개인 키가 들어 있는 로컬 전체본을 올리지 않는다.
- `environment.yml`은 Streamlit Cloud에서 혼선을 줄 수 있어 배포본에서는 제외했다.
