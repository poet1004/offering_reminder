# Python 3.11 설치/실행 가이드

이 프로젝트의 기본 웹 앱은 **Python 3.11로 실행 가능**합니다. 다만 **Windows에서는 Python 3.11 64-bit(x64)** 를 써야 합니다.

## 왜 64-bit가 필요한가
`pandas==2.2.3`는 PyPI에 **CPython 3.11 / Windows x86-64 wheel** 이 올라와 있습니다. 반면, 현재 설치 로그에서 보인 것처럼 wheel이 잡히지 않으면 `pandas-2.2.3.tar.gz` 소스 배포본으로 내려가고, 그때는 Visual Studio 빌드 도구가 필요해집니다.

즉, `3.11`이라는 숫자 자체보다는 **3.11 x64인지**가 핵심입니다.

## 권장 버전
- **Python 3.11.x (Windows는 64-bit 필수)**
- 가상환경 `.venv` 사용 권장

## 가장 쉬운 설치
### Windows
1. Python 3.11 x64 설치
2. 프로젝트 폴더에서 `setup_py311.bat` 실행
3. 설치가 끝나면 `run_app.bat` 실행

### macOS / Linux
```bash
bash setup_py311.sh
bash run_app.sh
```

## 환경 진단
프로젝트 폴더에서 아래를 실행하면 현재 파이썬이 몇 비트인지 바로 확인할 수 있습니다.
```bash
python scripts/check_python_env.py
```

정상 예시:
```text
python_version=3.11.x
bits=64
status=OK
```

문제 예시:
```text
python_version=3.11.x
bits=32
status=BAD_ARCH
```

## 수동 설치
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install --only-binary=:all: numpy==1.26.4 pandas==2.2.3
python -m pip install --prefer-binary -r requirements.txt
```

## 선택 설치
`integrated_lab` 안의 KIS 보조 스크립트까지 쓰려면:
```bash
python -m pip install -r requirements-optional.txt
```

## 자주 헷갈리는 점
- `python --version` 과 `python -m pip --version` 이 **같은 가상환경**을 가리켜야 합니다.
- Windows에서는 가능하면 `py -3.11` 또는 가상환경의 `python` 을 쓰세요.
- `pandas-2.2.3.tar.gz`가 보이면, compatible wheel을 못 찾고 소스 빌드로 내려간 상황입니다.


## Windows에서 꼭 지켜야 할 점

- `python` 명령이 3.14를 가리켜도 괜찮습니다. 설치와 가상환경 생성은 반드시 `py -3.11` 기준으로 진행하세요.
- `setup_py311.bat`는 `.venv`를 만들고, 이후 모든 실행 배치파일은 `.venv\Scripts\python.exe`만 사용합니다.
- 설치 확인은 `py -3.11 -c "import sys; print(sys.version)"` 와 `.venv\Scripts\python.exe scripts\check_python_env.py` 로 하세요.


## 현재 컴퓨터가 Python 3.14만 잡힐 때

1. `py -0p`로 설치된 Python 목록을 확인합니다.
2. `py -3.11 -c "import sys; print(sys.version)"`가 실패하면 `install_python311_and_setup.bat`를 실행합니다.
3. 기존 `.venv`가 3.14로 만들어졌다면 새 스크립트가 자동으로 `.venv`를 지우고 3.11로 다시 만듭니다.
4. `pip install -r requirements.txt`를 직접 치지 말고, 반드시 `.venv\Scripts\python.exe -m pip ...` 또는 `setup_py311.bat`를 사용하세요.
