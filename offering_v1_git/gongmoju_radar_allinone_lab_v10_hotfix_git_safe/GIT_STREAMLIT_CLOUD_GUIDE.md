# Git + Streamlit Community Cloud 배포 가이드

## 1) 어떤 압축을 써야 하나요?
- **full_private**: 현재 로컬 키/환경파일까지 포함된 개인 보관용입니다. 절대 공개 저장소에 올리지 마세요.
- **git_safe**: GitHub/Streamlit Community Cloud 업로드용입니다. 비밀키는 제거되어 있고 예시 파일만 남겨뒀습니다.

## 2) GitHub에 처음 올리는 가장 쉬운 방법
1. GitHub에서 새 저장소를 만듭니다.
2. 이 프로젝트의 **git_safe** 압축을 풉니다.
3. 폴더 안에서 Git을 초기화합니다.
   - `git init`
   - `git add .`
   - `git commit -m "Initial commit"`
4. GitHub 저장소 주소를 연결합니다.
   - `git remote add origin <깃허브 저장소 주소>`
   - `git branch -M main`
   - `git push -u origin main`

## 3) Streamlit Community Cloud 배포 순서
1. GitHub에 올린 저장소를 Streamlit Community Cloud에 연결합니다.
2. **Main file path** 는 `app.py` 로 지정합니다.
3. 앱의 Secrets 화면에 `.env.local` 값들을 넣습니다.
   - `KIS_APP_KEY`
   - `KIS_APP_SECRET`
   - `KIS_ENV`
   - `KIS_ACCOUNT_NO`
   - `KIS_CANO`
   - `KIS_ACNT_PRDT_CD`
   - `DART_API_KEY`
4. 저장 후 Deploy 또는 Reboot 합니다.

## 4) GitHub에 올리면 안 되는 파일
- `.env.local`
- `.env.real`
- `.env.practice`
- `.streamlit/secrets.toml`
- `integrated_lab/ipo_lockup_unified_lab/real_key.txt`
- `integrated_lab/ipo_lockup_unified_lab/practice_key.txt`
- `integrated_lab/ipo_lockup_unified_lab/dart_key.txt`
- `integrated_lab/ipo_lockup_unified_lab/token.dat`

## 5) 이 배포본에서 정리한 것
- 딜 탐색기 점수 컬럼 누락 오류 수정
- 38 상세 링크 보강 경로 수정
- 앱 번들 로딩 시 DART 자동 보강 연결
- 로컬 KIND 시드 자동 탐지 우선순위를 `kind_ipo_master.csv` 쪽으로 조정
- Git 배포본에서는 비밀키/대형 캐시 제거

## 6) 업로드 후 점검 포인트
- 사이드바의 **로컬 KIND export 경로** 가 자동으로 `integrated_lab/.../kind_ipo_master.csv` 를 잡는지 확인
- 소스 모드는 기본적으로 **실데이터 우선** 권장
- 38/KIND 사이트 응답이 일시적으로 비면, 배포본 내 시드 데이터로 일부 화면이 유지되는지 확인
