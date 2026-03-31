# 쇼츠 스튜디오 사용법

## 앱 안에서
1. `쇼츠 스튜디오` 메뉴로 이동
2. `스크립트 초안 생성` 클릭
3. 생성된 Scene 스크립트를 수정
4. `편집본으로 쇼츠 자산 생성` 클릭

## 생성물
- `slides/*.png`
- `narration_script.txt`
- `captions.srt`
- `shorts_manifest.csv`
- `shorts_payload.json`
- `editing_notes.md`
- 선택 시 `daily_shorts.mp4`
- 전체 묶음 `daily_shorts_assets.zip`

## 커맨드 실행
```bash
python scripts/generate_daily_shorts.py --source-mode "캐시 우선"
python scripts/generate_daily_shorts.py --source-mode "캐시 우선" --script-file edited_script.txt --with-video
```
