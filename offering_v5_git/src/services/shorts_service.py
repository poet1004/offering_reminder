from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.services.ipo_pipeline import IPODataBundle
from src.services.ipo_repository import IPORepository
from src.utils import fmt_pct, fmt_won, safe_float, today_kst


@dataclass(frozen=True)
class ShortsSlide:
    title: str
    subtitle: str
    bullets: tuple[str, ...]
    duration_sec: int = 4




def _safe_text(value: Any, default: str = "-") -> str:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    return text if text else default


class ShortsStudioService:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else None
        self.repo = IPORepository(self.base_dir)

    def build_daily_payload(
        self,
        bundle: IPODataBundle,
        issues: pd.DataFrame,
        today: pd.Timestamp | None = None,
        *,
        window_days: int = 7,
        source_label: str = "캐시 우선",
        market_snapshot: pd.DataFrame | None = None,
        market_source: str = "sample",
    ) -> dict[str, Any]:
        now = pd.Timestamp(today or today_kst()).normalize()
        horizon = now + pd.Timedelta(days=max(1, int(window_days)))
        issues_work = issues.copy() if isinstance(issues, pd.DataFrame) else pd.DataFrame()
        for col in ["subscription_start", "subscription_end", "listing_date"]:
            if col in issues_work.columns:
                issues_work[col] = pd.to_datetime(issues_work[col], errors="coerce")

        upcoming_sub = (
            issues_work[
                issues_work.get("subscription_start", pd.Series(dtype="datetime64[ns]", index=issues_work.index)).between(now, horizon, inclusive="both")
            ].copy()
            if not issues_work.empty and "subscription_start" in issues_work.columns
            else pd.DataFrame()
        )
        upcoming_list = (
            issues_work[
                issues_work.get("listing_date", pd.Series(dtype="datetime64[ns]", index=issues_work.index)).between(now, horizon, inclusive="both")
            ].copy()
            if not issues_work.empty and "listing_date" in issues_work.columns
            else pd.DataFrame()
        )
        upcoming_unlocks = self.repo.upcoming_unlocks(bundle.all_unlocks, now, window_days=max(1, int(window_days))) if bundle is not None else pd.DataFrame()
        featured_issues = self._build_featured_issues(issues_work, now, horizon)
        hold_examples = self.build_listing_hold_snapshot(issues_work, today=now, limit=5)
        market_frame = market_snapshot.copy() if isinstance(market_snapshot, pd.DataFrame) else pd.DataFrame()

        metrics = {
            "subscription_count": int(len(upcoming_sub)),
            "listing_count": int(len(upcoming_list)),
            "unlock_count": int(len(upcoming_unlocks)),
            "featured_count": int(len(featured_issues)),
            "hold_example_count": int(len(hold_examples)),
            "window_days": int(window_days),
            "source_label": source_label,
            "market_source": market_source,
        }
        return {
            "today": now,
            "metrics": metrics,
            "subscriptions": upcoming_sub.reset_index(drop=True),
            "listings": upcoming_list.reset_index(drop=True),
            "unlocks": upcoming_unlocks.reset_index(drop=True),
            "featured_issues": featured_issues.reset_index(drop=True),
            "hold_examples": hold_examples.reset_index(drop=True),
            "market_snapshot": market_frame.reset_index(drop=True),
        }

    def build_slides(self, payload: dict[str, Any], *, title: str | None = None) -> list[ShortsSlide]:
        today = pd.Timestamp(payload.get("today") or today_kst()).normalize()
        metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
        subs = payload.get("subscriptions", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        listings = payload.get("listings", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        unlocks = payload.get("unlocks", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        featured = payload.get("featured_issues", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        hold_examples = payload.get("hold_examples", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        market_snapshot = payload.get("market_snapshot", pd.DataFrame()) if isinstance(payload, dict) else pd.DataFrame()
        window_days = int(metrics.get("window_days", 7))
        source_label = str(metrics.get("source_label", "캐시 우선"))
        market_source = str(metrics.get("market_source", "sample"))
        main_title = title or f"공모주 레이더 데일리 {today.strftime('%Y-%m-%d')}"

        slides: list[ShortsSlide] = []
        slides.append(
            ShortsSlide(
                title=main_title,
                subtitle=f"향후 {window_days}일 일정 요약 · 데이터 모드 {source_label}",
                bullets=(
                    f"청약 일정 {metrics.get('subscription_count', 0)}건",
                    f"상장 일정 {metrics.get('listing_count', 0)}건",
                    f"보호예수 해제 {metrics.get('unlock_count', 0)}건",
                    f"관심 종목 포인트 {metrics.get('featured_count', 0)}건",
                ),
                duration_sec=4,
            )
        )
        slides.append(
            ShortsSlide(
                title="시장 한눈에 보기",
                subtitle=f"소스 {market_source}",
                bullets=tuple(self._market_lines(market_snapshot)),
                duration_sec=4,
            )
        )
        slides.append(
            ShortsSlide(
                title="다가오는 공모주 일정",
                subtitle=f"기준일 {today.strftime('%Y-%m-%d')} 이후 {window_days}일",
                bullets=tuple(self._schedule_lines(subs, listings, unlocks)),
                duration_sec=5,
            )
        )
        slides.append(
            ShortsSlide(
                title="주목 종목 포인트",
                subtitle="청약·상장 예정 종목 중심",
                bullets=tuple(self._featured_issue_lines(featured)),
                duration_sec=5,
            )
        )
        slides.append(
            ShortsSlide(
                title="상장일부터 지금까지 보유했다면",
                subtitle="현재가 기준 단순 보유 가정",
                bullets=tuple(self._hold_example_lines(hold_examples)),
                duration_sec=5,
            )
        )
        return slides

    def build_script(self, payload: dict[str, Any], *, title: str | None = None) -> str:
        slides = self.build_slides(payload, title=title)
        lines = ["# 공모주 데일리 쇼츠 스크립트", ""]
        for idx, slide in enumerate(slides, start=1):
            bullets = [bullet for bullet in slide.bullets if str(bullet or "").strip()]
            narration = self._default_narration(slide)
            lines.extend(
                [
                    f"[Scene {idx}]",
                    f"화면: {slide.title}",
                    f"자막: {slide.subtitle}",
                    *[f"자막: {bullet}" for bullet in bullets[:4]],
                    f"나레이션: {narration}",
                    "",
                ]
            )
        return "\n".join(lines).strip() + "\n"

    def generate_assets(
        self,
        payload: dict[str, Any],
        out_dir: Path | str,
        *,
        title: str | None = None,
        fps: int = 2,
        create_video: bool = False,
        create_zip: bool = True,
        script_text: str | None = None,
    ) -> dict[str, Any]:
        slides = self.build_slides(payload, title=title)
        out_path = Path(out_dir)
        slides_dir = out_path / "slides"
        slides_dir.mkdir(parents=True, exist_ok=True)

        scene_blocks = self._parse_script_text(script_text or self.build_script(payload, title=title), slides)
        manifest_rows: list[dict[str, Any]] = []
        image_paths: list[Path] = []
        for idx, slide in enumerate(slides, start=1):
            path = slides_dir / f"slide_{idx:02d}.png"
            self._render_slide_to_png(slide, path, index=idx, total=len(slides))
            image_paths.append(path)
            scene = scene_blocks[idx - 1] if idx - 1 < len(scene_blocks) else {"captions": [slide.title, slide.subtitle], "narration": self._default_narration(slide)}
            manifest_rows.append(
                {
                    "slide_no": idx,
                    "title": slide.title,
                    "subtitle": slide.subtitle,
                    "duration_sec": slide.duration_sec,
                    "file": str(path),
                    "bullets": "\n".join(slide.bullets),
                    "caption_preview": " | ".join(scene.get("captions", [])[:3]),
                    "narration": scene.get("narration", ""),
                }
            )
        manifest = pd.DataFrame(manifest_rows)
        manifest_path = out_path / "shorts_manifest.csv"
        manifest.to_csv(manifest_path, index=False, encoding="utf-8-sig")

        payload_path = out_path / "shorts_payload.json"
        payload_path.write_text(self._payload_json(payload), encoding="utf-8")
        script_path = out_path / "narration_script.txt"
        script_path.write_text(script_text or self.build_script(payload, title=title), encoding="utf-8")
        captions_path = out_path / "captions.srt"
        captions_path.write_text(self._build_srt(scene_blocks, slides), encoding="utf-8")
        guide_path = out_path / "editing_notes.md"
        guide_path.write_text(self._editing_notes(), encoding="utf-8")

        video_path: Path | None = None
        if create_video and image_paths:
            video_path = out_path / "daily_shorts.mp4"
            self._build_mp4(image_paths, slides, video_path, fps=max(1, int(fps)))

        zip_path: Path | None = None
        if create_zip:
            zip_path = out_path / "daily_shorts_assets.zip"
            self._zip_assets(out_path, zip_path)

        return {
            "manifest": manifest,
            "manifest_path": manifest_path,
            "payload_path": payload_path,
            "script_path": script_path,
            "captions_path": captions_path,
            "guide_path": guide_path,
            "video_path": video_path,
            "zip_path": zip_path,
            "slides": image_paths,
        }

    @staticmethod
    def build_listing_hold_snapshot(issues: pd.DataFrame, today: pd.Timestamp | None = None, limit: int = 5) -> pd.DataFrame:
        if not isinstance(issues, pd.DataFrame) or issues.empty:
            return pd.DataFrame()
        now = pd.Timestamp(today or today_kst()).normalize()
        required = {"name", "listing_date", "offer_price", "current_price"}
        if not required.issubset(set(issues.columns)):
            return pd.DataFrame()
        work = issues.copy()
        work["listing_date"] = pd.to_datetime(work["listing_date"], errors="coerce")
        work["offer_price"] = pd.to_numeric(work["offer_price"], errors="coerce")
        work["current_price"] = pd.to_numeric(work["current_price"], errors="coerce")
        work = work[
            work["listing_date"].notna()
            & (work["listing_date"] <= now)
            & work["offer_price"].gt(0)
            & work["current_price"].gt(0)
        ].copy()
        if work.empty:
            return pd.DataFrame()
        work["hold_days"] = (now - work["listing_date"]).dt.days.astype(int)
        work["hold_multiple"] = work["current_price"] / work["offer_price"]
        work["hold_return_pct"] = (work["hold_multiple"] - 1.0) * 100.0
        keep = [c for c in ["name", "symbol", "listing_date", "offer_price", "current_price", "hold_multiple", "hold_return_pct", "hold_days", "market", "underwriters"] if c in work.columns]
        work = work[keep].sort_values(["hold_return_pct", "listing_date"], ascending=[False, False]).reset_index(drop=True)
        return work.head(max(1, int(limit))).copy()

    @staticmethod
    def _market_lines(snapshot: pd.DataFrame) -> list[str]:
        if not isinstance(snapshot, pd.DataFrame) or snapshot.empty:
            return ["시장 스냅샷을 불러오지 못했습니다."]
        lines: list[str] = []
        preferred = ["KOSPI", "KOSDAQ", "USD/KRW", "NASDAQ"]
        for name in preferred:
            subset = snapshot[snapshot.get("name", pd.Series(dtype="object")) == name]
            if subset.empty:
                continue
            row = subset.iloc[0]
            last = row.get("last")
            change_pct = safe_float(row.get("change_pct"))
            last_text = "-"
            if pd.notna(last):
                last_num = safe_float(last)
                last_text = f"{last_num:,.2f}" if name != "USD/KRW" else f"{last_num:,.2f}"
            change_text = fmt_pct(change_pct, 2, signed=True)
            lines.append(f"{name} {last_text} · {change_text}")
        return lines or ["시장 핵심 지표를 아직 확보하지 못했습니다."]

    @staticmethod
    def _schedule_lines(subs: pd.DataFrame, listings: pd.DataFrame, unlocks: pd.DataFrame) -> list[str]:
        lines: list[str] = []
        if isinstance(subs, pd.DataFrame) and not subs.empty:
            view = subs.sort_values([c for c in ["subscription_start", "listing_date", "name"] if c in subs.columns]).head(3)
            for _, row in view.iterrows():
                day = pd.to_datetime(row.get("subscription_start"), errors="coerce")
                name = _safe_text(row.get("name"))
                underwriter = _safe_text(row.get("underwriters")) if _safe_text(row.get("underwriters")) != "-" else _safe_text(row.get("lead_manager"))
                lines.append(f"청약 {day.strftime('%m/%d') if pd.notna(day) else '--/--'} · {name} · {underwriter}")
        if isinstance(listings, pd.DataFrame) and not listings.empty:
            view = listings.sort_values([c for c in ["listing_date", "name"] if c in listings.columns]).head(3)
            for _, row in view.iterrows():
                day = pd.to_datetime(row.get("listing_date"), errors="coerce")
                name = _safe_text(row.get("name"))
                offer = fmt_won(row.get("offer_price")) if "offer_price" in row.index else "-"
                lines.append(f"상장 {day.strftime('%m/%d') if pd.notna(day) else '--/--'} · {name} · 공모가 {offer}")
        if isinstance(unlocks, pd.DataFrame) and not unlocks.empty:
            view = unlocks.sort_values([c for c in ["unlock_date", "name", "term"] if c in unlocks.columns]).head(2)
            for _, row in view.iterrows():
                day = pd.to_datetime(row.get("unlock_date"), errors="coerce")
                name = _safe_text(row.get("name"))
                term = _safe_text(row.get("term")) if _safe_text(row.get("term")) != "-" else _safe_text(row.get("unlock_type"))
                lines.append(f"해제 {day.strftime('%m/%d') if pd.notna(day) else '--/--'} · {name} · {term}")
        return lines[:8] or ["향후 일정이 아직 잡히지 않았습니다."]

    @staticmethod
    def _featured_issue_lines(featured: pd.DataFrame) -> list[str]:
        if not isinstance(featured, pd.DataFrame) or featured.empty:
            return ["주목할 예정 종목을 아직 선정하지 못했습니다."]
        lines: list[str] = []
        for _, row in featured.head(5).iterrows():
            name = _safe_text(row.get("name"))
            stage = _safe_text(row.get("stage"))
            market = _safe_text(row.get("market"))
            underwriter = _safe_text(row.get("underwriters"))
            offer = fmt_won(row.get("offer_price")) if "offer_price" in row.index else "-"
            lines.append(f"{name} · {stage} · {market} · {underwriter} · 공모가 {offer}")
        return lines[:5]

    @staticmethod
    def _hold_example_lines(hold_examples: pd.DataFrame) -> list[str]:
        if not isinstance(hold_examples, pd.DataFrame) or hold_examples.empty:
            return ["보유 가정 예시를 만들 수 있는 종목이 부족합니다."]
        lines: list[str] = []
        for _, row in hold_examples.head(4).iterrows():
            name = _safe_text(row.get("name"))
            ret = fmt_pct(row.get("hold_return_pct"), 2, signed=True)
            multiple = safe_float(row.get("hold_multiple"))
            multiple_text = "-" if pd.isna(multiple) else f"{multiple:.2f}x"
            hold_days = int(safe_float(row.get("hold_days"), 0) or 0)
            lines.append(f"{name} · 상장 후 {hold_days}일 · 수익률 {ret} · {multiple_text}")
        return lines[:4]

    @staticmethod
    def _build_featured_issues(issues: pd.DataFrame, now: pd.Timestamp, horizon: pd.Timestamp) -> pd.DataFrame:
        if not isinstance(issues, pd.DataFrame) or issues.empty:
            return pd.DataFrame()
        work = issues.copy()
        for col in ["subscription_start", "listing_date"]:
            if col in work.columns:
                work[col] = pd.to_datetime(work[col], errors="coerce")
        if "stage" not in work.columns:
            return pd.DataFrame()
        stage_order = {"청약중": 0, "청약예정": 1, "상장예정": 2, "상장후": 3, "청약완료": 4}
        work["_stage_rank"] = work["stage"].astype(str).map(stage_order).fillna(99)
        work["_anchor_date"] = work["subscription_start"].combine_first(work.get("listing_date"))
        work = work[(work["_anchor_date"].notna()) & work["_anchor_date"].between(now, horizon, inclusive="both")].copy()
        if work.empty:
            return pd.DataFrame()
        sort_cols = [c for c in ["_stage_rank", "_anchor_date", "subscription_score", "listing_quality_score", "name"] if c in work.columns]
        ascending = [True, True] + [False] * max(0, len(sort_cols) - 3) + ([True] if sort_cols and sort_cols[-1] == "name" else [])
        # rebuild ascending to match exact length
        ascending = []
        for col in sort_cols:
            if col in {"_stage_rank", "_anchor_date", "name"}:
                ascending.append(True)
            else:
                ascending.append(False)
        work = work.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)
        keep = [c for c in ["name", "symbol", "stage", "market", "underwriters", "offer_price", "subscription_start", "listing_date", "subscription_score", "listing_quality_score"] if c in work.columns]
        return work[keep].head(5).copy()

    @staticmethod
    def _payload_json(payload: dict[str, Any]) -> str:
        serializable: dict[str, Any] = {}
        for key, value in payload.items():
            if isinstance(value, pd.DataFrame):
                frame = value.copy()
                frame = frame.where(pd.notna(frame), "")
                serializable[key] = frame.to_dict(orient="records")
            elif isinstance(value, pd.Timestamp):
                serializable[key] = value.isoformat()
            else:
                serializable[key] = value
        return json.dumps(serializable, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _default_narration(slide: ShortsSlide) -> str:
        bits = [slide.subtitle, *slide.bullets[:3]]
        bits = [str(bit).strip() for bit in bits if str(bit or "").strip()]
        return " ".join(bits)

    @staticmethod
    def _parse_script_text(script_text: str, slides: list[ShortsSlide]) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        raw_blocks = [part.strip() for part in re.split(r"(?m)^\[Scene\s*\d+\]\s*$", script_text or "") if part.strip()]
        for idx, slide in enumerate(slides, start=1):
            if idx - 1 >= len(raw_blocks):
                blocks.append({"captions": [slide.title, slide.subtitle, *slide.bullets[:3]], "narration": ShortsStudioService._default_narration(slide)})
                continue
            block = raw_blocks[idx - 1]
            captions: list[str] = []
            narration_lines: list[str] = []
            fallback: list[str] = []
            for raw_line in block.splitlines():
                line = raw_line.strip().lstrip("-• ")
                if not line:
                    continue
                if line.startswith("화면:"):
                    value = line.split(":", 1)[1].strip()
                    if value:
                        fallback.append(value)
                elif line.startswith("자막:"):
                    value = line.split(":", 1)[1].strip()
                    if value:
                        captions.append(value)
                elif line.startswith("나레이션:"):
                    value = line.split(":", 1)[1].strip()
                    if value:
                        narration_lines.append(value)
                else:
                    fallback.append(line)
            scene_captions = captions or fallback or [slide.title, slide.subtitle, *slide.bullets[:3]]
            narration = " ".join(narration_lines).strip() or " ".join(scene_captions[:4])
            blocks.append({"captions": scene_captions[:5], "narration": narration})
        return blocks

    @staticmethod
    def _build_srt(scene_blocks: list[dict[str, Any]], slides: list[ShortsSlide]) -> str:
        lines: list[str] = []
        cursor = 0
        for idx, slide in enumerate(slides, start=1):
            scene = scene_blocks[idx - 1] if idx - 1 < len(scene_blocks) else {"captions": [slide.title, slide.subtitle, *slide.bullets[:2]]}
            start = ShortsStudioService._srt_ts(cursor)
            cursor += max(1, int(slide.duration_sec))
            end = ShortsStudioService._srt_ts(cursor)
            caption_lines = [str(item).strip() for item in scene.get("captions", []) if str(item or "").strip()]
            if not caption_lines:
                caption_lines = [slide.title, slide.subtitle]
            lines.extend([str(idx), f"{start} --> {end}", *caption_lines[:4], ""])
        return "\n".join(lines).strip() + "\n"

    @staticmethod
    def _srt_ts(seconds: int) -> str:
        hh = seconds // 3600
        mm = (seconds % 3600) // 60
        ss = seconds % 60
        return f"{hh:02d}:{mm:02d}:{ss:02d},000"

    @staticmethod
    def _zip_assets(root_dir: Path, zip_path: Path) -> None:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(root_dir.rglob("*")):
                if path == zip_path or path.is_dir():
                    continue
                zf.write(path, path.relative_to(root_dir))

    @staticmethod
    def _editing_notes() -> str:
        return (
            "# 쇼츠 자산 사용 가이드\n\n"
            "- narration_script.txt: 나레이션 초안입니다. 수정 후 TTS나 녹음 대본으로 쓰기 좋습니다.\n"
            "- captions.srt: 영상 자막용입니다. Scene 편집 내용이 반영됩니다.\n"
            "- shorts_manifest.csv: 슬라이드별 제목/길이/미리보기 문구가 들어 있습니다.\n"
            "- shorts_payload.json: 생성 기준 데이터 원본입니다.\n"
            "- slides/*.png: 세로형 카드 이미지입니다. 편집툴에 바로 넣을 수 있습니다.\n"
        )

    @staticmethod
    def _render_slide_to_png(slide: ShortsSlide, out_path: Path, *, index: int, total: int) -> None:
        from PIL import Image, ImageDraw, ImageFont

        width, height = 1080, 1920
        image = Image.new("RGB", (width, height), color=(9, 14, 27))
        draw = ImageDraw.Draw(image)
        font_title = ShortsStudioService._load_font(66, bold=True)
        font_sub = ShortsStudioService._load_font(34)
        font_body = ShortsStudioService._load_font(42)
        font_small = ShortsStudioService._load_font(26)

        draw.rounded_rectangle((54, 54, width - 54, height - 54), radius=38, fill=(16, 24, 44), outline=(42, 65, 119), width=2)
        draw.text((88, 92), slide.title, font=font_title, fill=(242, 246, 255))
        draw.text((88, 184), slide.subtitle, font=font_sub, fill=(155, 171, 204))
        draw.rounded_rectangle((88, 250, width - 88, 290), radius=18, fill=(24, 94, 224))
        draw.text((108, 256), f"Scene {index}/{total}", font=font_small, fill=(255, 255, 255))

        y = 360
        bullet_indent = 46
        max_width = width - 180
        for bullet in slide.bullets:
            wrapped = ShortsStudioService._wrap_text(draw, bullet, font_body, max_width)
            draw.ellipse((94, y + 14, 118, y + 38), fill=(87, 153, 255))
            for line_idx, line in enumerate(wrapped):
                draw.text((88 + bullet_indent, y + line_idx * 54), line, font=font_body, fill=(234, 239, 250))
            y += 54 * len(wrapped) + 26
            if y > height - 220:
                break

        footer = f"자동 생성된 공모주 리포트 · {slide.duration_sec}초"
        draw.text((88, height - 122), footer, font=font_small, fill=(140, 154, 186))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        image.save(out_path)

    @staticmethod
    def _build_mp4(image_paths: list[Path], slides: list[ShortsSlide], out_path: Path, *, fps: int = 2) -> None:
        import numpy as np
        import imageio.v2 as imageio

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with imageio.get_writer(out_path, fps=fps, codec="libx264", quality=8, macro_block_size=None) as writer:
            for path, slide in zip(image_paths, slides):
                frame = imageio.imread(path)
                repeats = max(1, int(slide.duration_sec) * int(fps))
                for _ in range(repeats):
                    writer.append_data(np.asarray(frame))

    @staticmethod
    def _wrap_text(draw: Any, text: str, font: Any, max_width: int) -> list[str]:
        words = str(text or "").split()
        if not words:
            return [""]
        lines: list[str] = []
        current = words[0]
        for word in words[1:]:
            trial = f"{current} {word}"
            if draw.textbbox((0, 0), trial, font=font)[2] <= max_width:
                current = trial
            else:
                lines.append(current)
                current = word
        if current:
            lines.append(current)
        compact: list[str] = []
        for line in lines:
            if draw.textbbox((0, 0), line, font=font)[2] <= max_width:
                compact.append(line)
                continue
            buf = ""
            for ch in line:
                trial = buf + ch
                if draw.textbbox((0, 0), trial, font=font)[2] <= max_width:
                    buf = trial
                else:
                    compact.append(buf)
                    buf = ch
            if buf:
                compact.append(buf)
        return compact

    @staticmethod
    def _load_font(size: int, *, bold: bool = False) -> Any:
        from PIL import ImageFont

        candidates = []
        if bold:
            candidates.extend(
                [
                    "C:/Windows/Fonts/malgunbd.ttf",
                    "C:/Windows/Fonts/NanumGothicBold.ttf",
                    "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
                    "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
                    "/System/Library/Fonts/AppleSDGothicNeo.ttc",
                ]
            )
        candidates.extend(
            [
                "C:/Windows/Fonts/malgun.ttf",
                "C:/Windows/Fonts/NanumGothic.ttf",
                "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
                "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                "/usr/share/fonts/opentype/noto/NotoSansCJK.ttc",
                "/System/Library/Fonts/AppleSDGothicNeo.ttc",
            ]
        )
        for path in candidates:
            if Path(path).exists():
                try:
                    return ImageFont.truetype(path, size=size)
                except Exception:
                    continue
        try:
            return ImageFont.truetype("DejaVuSans.ttf", size=size)
        except Exception:
            return ImageFont.load_default()

    @staticmethod
    def zip_bytes(zip_path: Path) -> bytes:
        return zip_path.read_bytes()

    @staticmethod
    def png_bytes(path: Path) -> bytes:
        return path.read_bytes()
