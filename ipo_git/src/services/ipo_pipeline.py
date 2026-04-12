from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.services.dart_client import DartClient
from src.services.dart_ipo_parser import DartIPOParser
from src.services.ipo_repository import IPORepository
from src.services.ipo_scrapers import (
    KIND_LISTING_URL,
    KIND_PUBLIC_OFFER_URL,
    KIND_PUB_PRICE_URL,
    THIRTYEIGHT_SCHEDULE_URL,
    THIRTYEIGHT_DEMAND_RESULT_URL,
    THIRTYEIGHT_NEW_LISTING_URL,
    THIRTYEIGHT_IR_DATA_URL,
    SEIBRO_RELEASE_URL,
    fetch_38_schedule,
    fetch_38_demand_results,
    fetch_38_new_listing_table,
    fetch_38_ir_links,
    fetch_seibro_release_schedule,
    fetch_kind_corp_download_table,
    fetch_kind_listing_table,
    fetch_kind_public_offering_table,
    fetch_kind_pubprice_table,
    load_kind_export_from_path,
    merge_live_sources,
    standardize_38_schedule_table,
    standardize_38_new_listing_table,
    standardize_kind_listing_table,
    standardize_kind_public_offering_table,
    standardize_kind_pubprice_table,
)
from src.services.kis_client import KISClient
from src.services.live_cache import LiveCacheStore
from src.utils import STANDARD_ISSUE_COLUMNS, clean_issue_frame, coalesce, normalize_name_key, parse_date_columns, standardize_issue_frame, today_kst


@dataclass
class IPODataBundle:
    issues: pd.DataFrame
    sample_unlocks: pd.DataFrame
    external_unlocks: pd.DataFrame
    all_unlocks: pd.DataFrame
    source_status: pd.DataFrame
    raw_tables: dict[str, pd.DataFrame]
    cache_inventory: pd.DataFrame


class IPODataHub:
    def __init__(
        self,
        base_dir: Path | str | None = None,
        kis_client: KISClient | None = None,
        dart_client: DartClient | None = None,
    ) -> None:
        self.base_dir = Path(base_dir) if base_dir is not None else None
        repo_base = self.base_dir
        self.repo = IPORepository(base_dir=repo_base)
        self.cache = LiveCacheStore((repo_base / "cache") if repo_base is not None else None)
        self.kis_client = kis_client
        self.dart_client = dart_client

    def refresh_live_cache(self, fetch_kind: bool = True, fetch_38: bool = True) -> dict[str, Any]:
        report: dict[str, Any] = {
            "kind_listing": None,
            "kind_public": None,
            "kind_pubprice": None,
            "kind_corp_download": None,
            "38": None,
            "38_demand": None,
            "38_new_listing": None,
            "38_ir": None,
            "seibro_release": None,
            "errors": [],
        }
        now = today_kst()
        if fetch_kind:
            try:
                raw_kind = fetch_kind_listing_table()
                std_kind = standardize_kind_listing_table(raw_kind, today=now)
                self.cache.write_frame(
                    "kind_listing_live",
                    std_kind,
                    meta={"source": "KIND", "notes": KIND_LISTING_URL, "row_count": int(len(std_kind)), "saved_at": now.isoformat()},
                )
                report["kind_listing"] = {"rows": int(len(std_kind)), "url": KIND_LISTING_URL}
            except Exception as exc:
                report["errors"].append(f"KIND listing refresh failed: {exc}")
            try:
                raw_public = fetch_kind_public_offering_table()
                std_public = standardize_kind_public_offering_table(raw_public, today=now)
                self.cache.write_frame(
                    "kind_public_offering_live",
                    std_public,
                    meta={"source": "KIND", "notes": KIND_PUBLIC_OFFER_URL, "row_count": int(len(std_public)), "saved_at": now.isoformat()},
                )
                report["kind_public"] = {"rows": int(len(std_public)), "url": KIND_PUBLIC_OFFER_URL}
            except Exception as exc:
                report["errors"].append(f"KIND public-offering refresh failed: {exc}")
            try:
                raw_pubprice = fetch_kind_pubprice_table()
                std_pubprice = standardize_kind_pubprice_table(raw_pubprice, today=now)
                self.cache.write_frame(
                    "kind_pubprice_live",
                    std_pubprice,
                    meta={"source": "KIND", "notes": KIND_PUB_PRICE_URL, "row_count": int(len(std_pubprice)), "saved_at": now.isoformat()},
                )
                report["kind_pubprice"] = {"rows": int(len(std_pubprice)), "url": KIND_PUB_PRICE_URL}
            except Exception as exc:
                report["errors"].append(f"KIND pubprice refresh failed: {exc}")
            try:
                std_corp = fetch_kind_corp_download_table()
                self.cache.write_frame(
                    "kind_corp_download_live",
                    std_corp,
                    meta={"source": "KIND", "notes": "corpList download", "row_count": int(len(std_corp)), "saved_at": now.isoformat()},
                )
                report["kind_corp_download"] = {"rows": int(len(std_corp)), "url": "corpList download"}
            except Exception as exc:
                report["errors"].append(f"KIND corp download refresh failed: {exc}")
        if fetch_38:
            try:
                raw_38 = fetch_38_schedule(include_detail_links=True)
                std_38 = standardize_38_schedule_table(raw_38, today=now, fetch_details=True)
                self.cache.write_frame(
                    "schedule_38_live",
                    std_38,
                    meta={"source": "38", "notes": THIRTYEIGHT_SCHEDULE_URL, "row_count": int(len(std_38)), "saved_at": now.isoformat()},
                )
                report["38"] = {"rows": int(len(std_38)), "url": THIRTYEIGHT_SCHEDULE_URL}
            except Exception as exc:
                report["errors"].append(f"38 refresh failed: {exc}")
            try:
                demand_38 = fetch_38_demand_results()
                self.cache.write_frame(
                    "schedule_38_demand_live",
                    demand_38,
                    meta={"source": "38", "notes": THIRTYEIGHT_DEMAND_RESULT_URL, "row_count": int(len(demand_38)), "saved_at": now.isoformat()},
                )
                report["38_demand"] = {"rows": int(len(demand_38)), "url": THIRTYEIGHT_DEMAND_RESULT_URL}
            except Exception as exc:
                report["errors"].append(f"38 demand-result refresh failed: {exc}")
            try:
                new_listing_38 = standardize_38_new_listing_table(fetch_38_new_listing_table())
                self.cache.write_frame(
                    "schedule_38_new_listing_live",
                    new_listing_38,
                    meta={"source": "38", "notes": THIRTYEIGHT_NEW_LISTING_URL, "row_count": int(len(new_listing_38)), "saved_at": now.isoformat()},
                )
                report["38_new_listing"] = {"rows": int(len(new_listing_38)), "url": THIRTYEIGHT_NEW_LISTING_URL}
            except Exception as exc:
                report["errors"].append(f"38 new-listing refresh failed: {exc}")
            try:
                ir_38 = fetch_38_ir_links()
                self.cache.write_frame(
                    "ir_38_live",
                    ir_38,
                    meta={"source": "38", "notes": THIRTYEIGHT_IR_DATA_URL, "row_count": int(len(ir_38)), "saved_at": now.isoformat()},
                )
                report["38_ir"] = {"rows": int(len(ir_38)), "url": THIRTYEIGHT_IR_DATA_URL}
            except Exception as exc:
                report["errors"].append(f"38 IR refresh failed: {exc}")
            try:
                seibro_release = fetch_seibro_release_schedule()
                self.cache.write_frame(
                    "seibro_release_live",
                    seibro_release,
                    meta={"source": "Seibro", "notes": SEIBRO_RELEASE_URL, "row_count": int(len(seibro_release)), "saved_at": now.isoformat()},
                )
                report["seibro_release"] = {"rows": int(len(seibro_release)), "url": SEIBRO_RELEASE_URL}
            except Exception as exc:
                report["errors"].append(f"Seibro release refresh failed: {exc}")
        return report

    def load_bundle(
        self,
        *,
        prefer_live: bool = False,
        use_cache: bool = True,
        external_unlock_path: str | Path | None = None,
        local_kind_export_path: str | Path | None = None,
        allow_sample_fallback: bool = False,
        allow_packaged_sample_paths: bool = False,
    ) -> IPODataBundle:
        now = today_kst()
        sample = self.repo.load_sample_issues()
        sample_unlocks = self.repo.unlock_calendar_from_issues(sample)
        external_unlocks = self.repo.load_external_unlock_events(
            external_unlock_path,
            allow_packaged_sample=allow_packaged_sample_paths,
        )
        if not external_unlocks.empty:
            external_unlocks = external_unlocks.copy()
            external_unlocks["market"] = external_unlocks.get("market", pd.Series(dtype="object"))
            external_unlocks["offer_price"] = external_unlocks.get("ipo_price")
            external_unlocks["current_price"] = pd.NA
            external_unlocks["stage"] = "전략데이터"
            external_unlocks["source"] = "strategy"

        raw_tables: dict[str, pd.DataFrame] = {"sample": sample, "sample_unlocks": sample_unlocks, "external_unlocks": external_unlocks}
        live_kind = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_kind_public = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_kind_pubprice = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_38 = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_38_demand = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_38_new_listing = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_38_ir = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        live_seibro = pd.DataFrame()
        live_kind_corp = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        local_kind = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        seed_38 = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        dart_enriched = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        source_rows: list[dict[str, Any]] = []

        if local_kind_export_path:
            try:
                local_kind = clean_issue_frame(load_kind_export_from_path(local_kind_export_path))
                if not local_kind.empty:
                    raw_tables["local_kind"] = local_kind
                    source_rows.append({"source": "local-kind", "ok": True, "rows": int(len(local_kind)), "detail": str(local_kind_export_path)})
            except Exception as exc:
                source_rows.append({"source": "local-kind", "ok": False, "rows": 0, "detail": str(exc)})

        try:
            seed_38 = clean_issue_frame(self.repo.load_38_seed_export())
            if not seed_38.empty:
                raw_tables["seed_38"] = seed_38
                source_rows.append({"source": "38-seed", "ok": True, "rows": int(len(seed_38)), "detail": str(self.repo.auto_detect_38_seed_export() or "")})
        except Exception as exc:
            source_rows.append({"source": "38-seed", "ok": False, "rows": 0, "detail": str(exc)})

        try:
            dart_enriched = clean_issue_frame(self.repo.load_dart_enriched_export())
            if not dart_enriched.empty:
                raw_tables["dart_enriched"] = dart_enriched
                source_rows.append({"source": "dart-enriched", "ok": True, "rows": int(len(dart_enriched)), "detail": str(self.repo.auto_detect_dart_enriched_export() or "")})
        except Exception as exc:
            source_rows.append({"source": "dart-enriched", "ok": False, "rows": 0, "detail": str(exc)})

        if use_cache:
            cache_specs = [
                ("kind_listing_live", "KIND-cache-listing"),
                ("kind_public_offering_live", "KIND-cache-public"),
                ("kind_pubprice_live", "KIND-cache-pubprice"),
                ("schedule_38_live", "38-cache"),
("schedule_38_demand_live", "38-demand-cache"),
                ("schedule_38_new_listing_live", "38-new-listing-cache"),
                ("ir_38_live", "38-ir-cache"),
                ("seibro_release_live", "Seibro-cache"),
                ("kind_corp_download_live", "KIND-cache-corpdownload"),
            ]
            for cache_name, source_name in cache_specs:
                cached = self.cache.read_frame(cache_name)
                if cached.empty:
                    continue
                meta = self.cache.read_meta(cache_name)
                raw_tables[cache_name] = cached
                source_rows.append({"source": source_name, "ok": True, "rows": int(len(cached)), "detail": meta.get("saved_at", "")})
                if cache_name == "kind_listing_live":
                    live_kind = clean_issue_frame(cached)
                elif cache_name == "kind_public_offering_live":
                    live_kind_public = clean_issue_frame(cached)
                elif cache_name == "kind_pubprice_live":
                    live_kind_pubprice = clean_issue_frame(cached)
                elif cache_name == "schedule_38_live":
                    live_38 = clean_issue_frame(cached)
                elif cache_name == "schedule_38_demand_live":
                    live_38_demand = clean_issue_frame(cached)
                elif cache_name == "schedule_38_new_listing_live":
                    live_38_new_listing = clean_issue_frame(cached)
                elif cache_name == "ir_38_live":
                    live_38_ir = clean_issue_frame(cached)
                elif cache_name == "seibro_release_live":
                    live_seibro = parse_date_columns(cached.copy())
                elif cache_name == "kind_corp_download_live":
                    live_kind_corp = clean_issue_frame(cached)

        if prefer_live:
            try:
                live_kind = clean_issue_frame(standardize_kind_listing_table(fetch_kind_listing_table()))
                raw_tables["live_kind"] = live_kind
                self.cache.write_frame(
                    "kind_listing_live",
                    live_kind,
                    meta={"source": "KIND", "notes": KIND_LISTING_URL, "row_count": int(len(live_kind))},
                )
                source_rows.append({"source": "KIND-live-listing", "ok": True, "rows": int(len(live_kind)), "detail": KIND_LISTING_URL})
            except Exception as exc:
                source_rows.append({"source": "KIND-live-listing", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_kind_public = clean_issue_frame(standardize_kind_public_offering_table(fetch_kind_public_offering_table()))
                raw_tables["live_kind_public"] = live_kind_public
                self.cache.write_frame(
                    "kind_public_offering_live",
                    live_kind_public,
                    meta={"source": "KIND", "notes": KIND_PUBLIC_OFFER_URL, "row_count": int(len(live_kind_public))},
                )
                source_rows.append({"source": "KIND-live-public", "ok": True, "rows": int(len(live_kind_public)), "detail": KIND_PUBLIC_OFFER_URL})
            except Exception as exc:
                source_rows.append({"source": "KIND-live-public", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_kind_pubprice = clean_issue_frame(standardize_kind_pubprice_table(fetch_kind_pubprice_table()))
                raw_tables["live_kind_pubprice"] = live_kind_pubprice
                self.cache.write_frame(
                    "kind_pubprice_live",
                    live_kind_pubprice,
                    meta={"source": "KIND", "notes": KIND_PUB_PRICE_URL, "row_count": int(len(live_kind_pubprice))},
                )
                source_rows.append({"source": "KIND-live-pubprice", "ok": True, "rows": int(len(live_kind_pubprice)), "detail": KIND_PUB_PRICE_URL})
            except Exception as exc:
                source_rows.append({"source": "KIND-live-pubprice", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_38 = clean_issue_frame(standardize_38_schedule_table(fetch_38_schedule(include_detail_links=True), fetch_details=True))
                raw_tables["live_38"] = live_38
                self.cache.write_frame(
                    "schedule_38_live",
                    live_38,
                    meta={"source": "38", "notes": THIRTYEIGHT_SCHEDULE_URL, "row_count": int(len(live_38))},
                )
                source_rows.append({"source": "38-live", "ok": True, "rows": int(len(live_38)), "detail": THIRTYEIGHT_SCHEDULE_URL})
            except Exception as exc:
                source_rows.append({"source": "38-live", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_38_demand = clean_issue_frame(fetch_38_demand_results())
                raw_tables["live_38_demand"] = live_38_demand
                self.cache.write_frame(
                    "schedule_38_demand_live",
                    live_38_demand,
                    meta={"source": "38", "notes": THIRTYEIGHT_DEMAND_RESULT_URL, "row_count": int(len(live_38_demand))},
                )
                source_rows.append({"source": "38-live-demand", "ok": True, "rows": int(len(live_38_demand)), "detail": THIRTYEIGHT_DEMAND_RESULT_URL})
            except Exception as exc:
                source_rows.append({"source": "38-live-demand", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_38_new_listing = clean_issue_frame(standardize_38_new_listing_table(fetch_38_new_listing_table()))
                raw_tables["live_38_new_listing"] = live_38_new_listing
                self.cache.write_frame(
                    "schedule_38_new_listing_live",
                    live_38_new_listing,
                    meta={"source": "38", "notes": THIRTYEIGHT_NEW_LISTING_URL, "row_count": int(len(live_38_new_listing))},
                )
                source_rows.append({"source": "38-live-new-listing", "ok": True, "rows": int(len(live_38_new_listing)), "detail": THIRTYEIGHT_NEW_LISTING_URL})
            except Exception as exc:
                source_rows.append({"source": "38-live-new-listing", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_38_ir = clean_issue_frame(fetch_38_ir_links())
                raw_tables["live_38_ir"] = live_38_ir
                self.cache.write_frame(
                    "ir_38_live",
                    live_38_ir,
                    meta={"source": "38", "notes": THIRTYEIGHT_IR_DATA_URL, "row_count": int(len(live_38_ir))},
                )
                source_rows.append({"source": "38-live-ir", "ok": True, "rows": int(len(live_38_ir)), "detail": THIRTYEIGHT_IR_DATA_URL})
            except Exception as exc:
                source_rows.append({"source": "38-live-ir", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_seibro = fetch_seibro_release_schedule()
                raw_tables["seibro_release"] = live_seibro
                self.cache.write_frame(
                    "seibro_release_live",
                    live_seibro,
                    meta={"source": "Seibro", "notes": SEIBRO_RELEASE_URL, "row_count": int(len(live_seibro))},
                )
                source_rows.append({"source": "Seibro-live-release", "ok": True, "rows": int(len(live_seibro)), "detail": SEIBRO_RELEASE_URL})
            except Exception as exc:
                source_rows.append({"source": "Seibro-live-release", "ok": False, "rows": 0, "detail": str(exc)})
            try:
                live_kind_corp = clean_issue_frame(fetch_kind_corp_download_table())
                raw_tables["live_kind_corp"] = live_kind_corp
                self.cache.write_frame(
                    "kind_corp_download_live",
                    live_kind_corp,
                    meta={"source": "KIND", "notes": "corpList download", "row_count": int(len(live_kind_corp))},
                )
                source_rows.append({"source": "KIND-live-corpdownload", "ok": True, "rows": int(len(live_kind_corp)), "detail": "corpList download"})
            except Exception as exc:
                source_rows.append({"source": "KIND-live-corpdownload", "ok": False, "rows": 0, "detail": str(exc)})

        live_merged = merge_live_sources(
            live_kind,
            live_38,
            kind_public_df=live_kind_public,
            kind_pubprice_df=live_kind_pubprice,
            kind_corp_df=live_kind_corp,
        )
        if not seed_38.empty:
            live_merged = self._overlay_issues(live_merged, seed_38) if not live_merged.empty else seed_38.copy()
        if not live_38_new_listing.empty:
            live_merged = self._overlay_issues(live_merged, live_38_new_listing) if not live_merged.empty else live_38_new_listing.copy()
        if not live_38_demand.empty:
            live_merged = self._overlay_issues(live_merged, live_38_demand) if not live_merged.empty else live_38_demand.copy()
        if not live_38_ir.empty and not live_merged.empty:
            live_merged = self._overlay_issues(live_merged, live_38_ir)
        if not live_seibro.empty:
            raw_tables.setdefault("seibro_release", live_seibro)
        if not local_kind.empty:
            live_merged = self._overlay_issues(live_merged, local_kind) if not live_merged.empty else local_kind

        if not live_merged.empty:
            issues = live_merged.copy()
        elif not dart_enriched.empty:
            issues = dart_enriched.copy()
        elif allow_sample_fallback:
            issues = sample.copy()
        else:
            issues = pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

        if not seed_38.empty and not issues.empty:
            issues = self._overlay_issues(issues, seed_38)
        if not live_38_new_listing.empty and not issues.empty:
            issues = self._overlay_issues(issues, live_38_new_listing)
        if not live_38_demand.empty and not issues.empty:
            issues = self._overlay_issues(issues, live_38_demand)
        if not live_38_ir.empty and not issues.empty:
            issues = self._overlay_issues(issues, live_38_ir)

        strategy_overlay = self._issue_overlay_from_external_unlocks(external_unlocks)
        if not strategy_overlay.empty:
            raw_tables["strategy_overlay"] = strategy_overlay
            issues = self._overlay_issues(issues, strategy_overlay) if not issues.empty else strategy_overlay
            source_rows.append({"source": "strategy-overlay", "ok": True, "rows": int(len(strategy_overlay)), "detail": str(external_unlock_path or "")})

        if not dart_enriched.empty and not issues.empty:
            issues = self._overlay_issues(issues, dart_enriched)
        issues = clean_issue_frame(issues)
        auto_dart = self._auto_dart_overlay(issues, today=now, max_items=20)
        if not auto_dart.empty:
            raw_tables["auto_dart_overlay"] = auto_dart
            issues = self._overlay_issues(issues, auto_dart)
            source_rows.append({"source": "DART-auto-overlay", "ok": True, "rows": int(len(auto_dart)), "detail": "recent IPO candidates"})
        elif self.dart_client is not None:
            source_rows.append({"source": "DART-auto-overlay", "ok": False, "rows": 0, "detail": "no recent missing candidates or overlay unavailable"})
        if prefer_live and self.kis_client is not None and not issues.empty:
            live_prices = self._fetch_live_price_snapshot(issues)
            if not live_prices.empty:
                raw_tables["live_prices"] = live_prices
                issues = self.repo.merge_price_snapshot(issues, live_prices)
                source_rows.append({"source": "KIS-live-price", "ok": True, "rows": int(len(live_prices)), "detail": "listed symbols"})
            elif any(str(row.get("source", "")) == "KIS-live-price" for row in source_rows) is False:
                source_rows.append({"source": "KIS-live-price", "ok": False, "rows": 0, "detail": "no listed symbols or price fetch failed"})
        issues = self._curate_ui_issues(issues, today=now)
        if not issues.empty and "market" in issues.columns:
            issues["market"] = issues["market"].where(issues["market"].notna(), "미상")
        issue_unlocks = self.repo.unlock_calendar_from_issues(issues)
        all_unlocks = self.prepare_unlock_union(issue_unlocks, external_unlocks)

        source_status = pd.DataFrame(source_rows, columns=["source", "ok", "rows", "detail"])
        cache_inventory = self.cache.list_inventory()
        raw_tables["issues"] = issues
        raw_tables["issue_unlocks"] = issue_unlocks
        raw_tables["all_unlocks"] = all_unlocks
        return IPODataBundle(
            issues=issues,
            sample_unlocks=sample_unlocks,
            external_unlocks=external_unlocks,
            all_unlocks=all_unlocks,
            source_status=source_status,
            raw_tables=raw_tables,
            cache_inventory=cache_inventory,
        )

    @staticmethod
    def _curate_ui_issues(issues: pd.DataFrame, *, today: pd.Timestamp | None = None) -> pd.DataFrame:
        if issues is None or issues.empty:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        today = today or today_kst()
        work = standardize_issue_frame(issues.copy())
        listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
        sub_start = pd.to_datetime(work.get("subscription_start"), errors="coerce")
        sub_end = pd.to_datetime(work.get("subscription_end"), errors="coerce")
        source = work.get("source", pd.Series(dtype="object")).fillna("").astype(str)
        recent_cutoff = today - pd.Timedelta(days=365 * 3)
        recent_listing = listing.notna() & (listing >= recent_cutoff)
        upcoming_stage = work.get("stage", pd.Series(dtype="object")).fillna("").isin(["청약예정", "청약중", "청약완료", "상장예정", "전략데이터"])
        has_schedule = sub_start.notna() | sub_end.notna()
        has_offer = pd.to_numeric(work.get("offer_price", pd.Series(dtype="object")), errors="coerce").notna()
        has_unlock = False
        unlock_cols = [c for c in ["unlock_date_15d", "unlock_date_1m", "unlock_date_3m", "unlock_date_6m", "unlock_date_1y"] if c in work.columns]
        if unlock_cols:
            has_unlock = work[unlock_cols].notna().any(axis=1)
        preferred_source = source.isin(["38", "KIND-공모기업", "KIND-공모가비교", "strategy-overlay"])
        listed_recent_corp = source.eq("KIND-corpList") & recent_listing & listing.notna()
        keep = upcoming_stage | has_schedule | has_offer | has_unlock | preferred_source | listed_recent_corp | recent_listing
        out = work.loc[keep].copy()
        if out.empty:
            return out
        sort_date = pd.to_datetime(out.get("listing_date"), errors="coerce")
        sort_date = sort_date.combine_first(pd.to_datetime(out.get("subscription_start"), errors="coerce"))
        sort_date = sort_date.combine_first(pd.to_datetime(out.get("subscription_end"), errors="coerce"))
        source_rank = out.get("source", pd.Series(dtype="object")).map({
            "strategy-overlay": 6,
            "KIND-공모기업": 5,
            "38": 5,
            "local-kind": 4,
            "KIND-공모가비교": 4,
            "KIND-corpList": 2,
        }).fillna(3)
        out["_sort_date"] = sort_date
        out["_source_rank"] = source_rank
        out = out.sort_values(["_sort_date", "_source_rank", "name_key"], ascending=[False, False, True], na_position="last")
        return out.drop(columns=["_sort_date", "_source_rank"], errors="ignore").reset_index(drop=True)

    def _auto_dart_overlay(self, issues: pd.DataFrame, *, today: pd.Timestamp | None = None, max_items: int = 40) -> pd.DataFrame:
        if self.dart_client is None or issues is None or issues.empty:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        today = today or today_kst()
        work = standardize_issue_frame(issues.copy())
        listing = pd.to_datetime(work.get("listing_date"), errors="coerce")
        sub_start = pd.to_datetime(work.get("subscription_start"), errors="coerce")
        src = work.get("source", pd.Series(dtype="object")).fillna("").astype(str)
        recent_mask = (
            (sub_start.notna() & (sub_start >= today - pd.Timedelta(days=240)) & (sub_start <= today + pd.Timedelta(days=240)))
            | (listing.notna() & (listing >= today - pd.Timedelta(days=540)) & (listing <= today + pd.Timedelta(days=240)))
            | src.isin(["38", "KIND-공모기업", "KIND-공모가비교", "local-kind", "strategy-overlay"])
        )
        missing_cols = [c for c in ["market", "sector", "symbol", "total_offer_shares", "post_listing_total_shares", "secondary_sale_ratio", "existing_shareholder_ratio", "employee_forfeit_ratio", "lockup_commitment_ratio", "offer_price"] if c in work.columns]
        missing_score = work[missing_cols].isna().sum(axis=1) if missing_cols else pd.Series(0, index=work.index)
        priority = src.map({"38": 4, "KIND-공모기업": 3, "KIND-공모가비교": 3, "local-kind": 2, "strategy-overlay": 2}).fillna(1)
        target = work.loc[recent_mask].copy()
        if target.empty:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        target["_missing_score"] = missing_score.loc[target.index]
        target["_priority"] = priority.loc[target.index]
        target = target.sort_values(["_priority", "_missing_score", "listing_date", "subscription_start"], ascending=[False, False, False, False], na_position="last").head(max_items)
        if target.empty:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        parser = DartIPOParser(self.dart_client, base_dir=self.cache.base_dir)
        rows: list[dict[str, Any]] = []
        for _, issue in target.iterrows():
            try:
                snapshot = parser.analyze_company(
                    stock_code=str(issue.get("symbol") or "").strip() or None,
                    corp_name=str(issue.get("name") or "").strip() or None,
                    force=False,
                    days=540,
                )
            except Exception:
                continue
            overlay = parser.snapshot_to_issue_overlay(snapshot)
            if not overlay:
                continue
            row = {col: pd.NA for col in STANDARD_ISSUE_COLUMNS}
            row.update({
                "name": issue.get("name"),
                "name_key": issue.get("name_key"),
                "symbol": issue.get("symbol"),
                "source": issue.get("source"),
                "source_detail": coalesce(issue.get("source_detail"), "dart-auto-overlay"),
            })
            for key, value in overlay.items():
                if key in row and value is not None:
                    row[key] = value
            rows.append(row)
        return standardize_issue_frame(pd.DataFrame(rows)) if rows else pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)

    @staticmethod
    def _issue_overlay_from_external_unlocks(external_unlocks: pd.DataFrame) -> pd.DataFrame:
        if external_unlocks is None or external_unlocks.empty:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        work = external_unlocks.copy()
        work["name_key"] = work.get("name_key", pd.Series(dtype="object")).fillna(work.get("name", pd.Series(dtype="object")).map(normalize_name_key))
        work["listing_date"] = pd.to_datetime(work.get("listing_date"), errors="coerce")
        rows: list[dict[str, Any]] = []
        for _, row in work.sort_values([c for c in ["listing_date", "unlock_date", "name"] if c in work.columns], na_position="last").iterrows():
            item = {col: pd.NA for col in STANDARD_ISSUE_COLUMNS}
            item.update({
                "ipo_id": row.get("ipo_id") or f"strategy_{row.get('name_key')}",
                "name": row.get("name"),
                "name_key": row.get("name_key"),
                "market": row.get("market"),
                "symbol": row.get("symbol"),
                "stage": row.get("stage") or pd.NA,
                "underwriters": row.get("lead_manager") if "lead_manager" in row.index else row.get("underwriters"),
                "listing_date": row.get("listing_date"),
                "offer_price": row.get("ipo_price") if "ipo_price" in row.index else row.get("offer_price"),
                "post_listing_total_shares": row.get("listed_shares") if "listed_shares" in row.index else pd.NA,
                "unlock_date_15d": row.get("unlock_date") if str(row.get("term")) == "15D" else pd.NA,
                "unlock_date_1m": row.get("unlock_date") if str(row.get("term")) == "1M" else pd.NA,
                "unlock_date_3m": row.get("unlock_date") if str(row.get("term")) == "3M" else pd.NA,
                "unlock_date_6m": row.get("unlock_date") if str(row.get("term")) == "6M" else pd.NA,
                "unlock_date_1y": row.get("unlock_date") if str(row.get("term")) == "1Y" else pd.NA,
                "source": "strategy-overlay",
                "source_detail": row.get("source") or "integrated-lab-dataset",
                "last_refresh_ts": today_kst(),
            })
            rows.append(item)
        if not rows:
            return pd.DataFrame(columns=STANDARD_ISSUE_COLUMNS)
        overlay = standardize_issue_frame(pd.DataFrame(rows))
        overlay = overlay.sort_values(["listing_date", "name_key"], na_position="last").drop_duplicates(subset=["name_key"], keep="last")
        return overlay.reset_index(drop=True)

    def _fetch_live_price_snapshot(self, issues: pd.DataFrame, max_symbols: int = 80) -> pd.DataFrame:
        if self.kis_client is None or issues is None or issues.empty:
            return pd.DataFrame(columns=["symbol", "price", "change_pct"])
        work = issues.copy()
        work["listing_date"] = pd.to_datetime(work.get("listing_date"), errors="coerce")
        today = today_kst()
        mask = work.get("symbol", pd.Series(dtype="object")).astype(str).str.fullmatch(r"\d{6}").fillna(False)
        mask = mask & work["listing_date"].notna() & (work["listing_date"].dt.normalize() <= today)
        subset = work.loc[mask, [c for c in ["symbol", "current_price"] if c in work.columns]].copy()
        if subset.empty:
            return pd.DataFrame(columns=["symbol", "price", "change_pct"])
        symbols = subset["symbol"].astype(str).str.zfill(6).drop_duplicates().tolist()[:max_symbols]
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            try:
                quote = self.kis_client.get_stock_price(symbol)
                if quote.get("price") is None:
                    continue
                rows.append({"symbol": symbol, "price": quote.get("price"), "change_pct": quote.get("change_pct")})
            except Exception:
                continue
        return pd.DataFrame(rows, columns=["symbol", "price", "change_pct"])

    @staticmethod
    def prepare_unlock_union(sample_unlocks: pd.DataFrame, external_unlocks: pd.DataFrame) -> pd.DataFrame:
        frames = []
        if not sample_unlocks.empty:
            a = sample_unlocks.copy()
            a["source"] = a.get("source", "demo")
            frames.append(a)
        if not external_unlocks.empty:
            b = external_unlocks.copy()
            b["source"] = b.get("source", "strategy")
            frames.append(b)
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return pd.DataFrame(columns=["name", "symbol", "listing_date", "unlock_date", "term", "source"])
        column_order: list[str] = []
        for frame in frames:
            for col in frame.columns:
                if col not in column_order:
                    column_order.append(col)
        records: list[dict[str, Any]] = []
        for frame in frames:
            normalized = frame.copy()
            for col in column_order:
                if col not in normalized.columns:
                    normalized[col] = pd.NA
            records.extend(normalized[column_order].to_dict("records"))
        out = pd.DataFrame(records, columns=column_order)
        out["name_key"] = out.get("name_key", pd.Series(dtype="object")).fillna(out["name"].map(normalize_name_key))
        out = parse_date_columns(out, ["listing_date", "unlock_date"])
        out["source_priority"] = out.get("source", pd.Series(dtype="object")).map({"strategy": 0, "demo": 1, "sample": 1}).fillna(2)
        dedupe_cols = [c for c in ["symbol", "name_key", "unlock_date", "term"] if c in out.columns]
        if dedupe_cols:
            out = out.sort_values(["source_priority", "unlock_date", "name"]).drop_duplicates(subset=dedupe_cols, keep="first")
        return out.drop(columns=["source_priority"], errors="ignore").sort_values(["unlock_date", "name"]).reset_index(drop=True)

    def write_uploaded_kind_file(self, payload: bytes, filename: str) -> Path:
        uploads_dir = self.repo.base_dir / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        ext = Path(filename).suffix or ".xlsx"
        path = uploads_dir / f"kind_latest{ext}"
        path.write_bytes(payload)
        return path

    @staticmethod
    def _overlay_issues(base: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
        if base.empty:
            return updates.copy()
        if updates.empty:
            return base.copy()
        base = standardize_issue_frame(base)
        updates = standardize_issue_frame(updates)
        base["name_key"] = base["name_key"].map(normalize_name_key)
        updates["name_key"] = updates["name_key"].map(normalize_name_key)

        update_map = updates.set_index("name_key")
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for _, row in base.iterrows():
            key = row["name_key"]
            current = row.to_dict()
            if key in update_map.index:
                upd = update_map.loc[key]
                if isinstance(upd, pd.DataFrame):
                    upd = upd.iloc[0]
                for col in STANDARD_ISSUE_COLUMNS:
                    current[col] = coalesce(upd.get(col), current.get(col))
                current["source"] = coalesce(upd.get("source"), current.get("source"))
                current["source_detail"] = coalesce(upd.get("source_detail"), current.get("source_detail"))
            rows.append(current)
            seen.add(key)
        new_rows = updates[~updates["name_key"].isin(seen)]
        if not new_rows.empty:
            rows.extend(new_rows.to_dict(orient="records"))
        out = pd.DataFrame(rows)
        return standardize_issue_frame(out)

    def analyze_issue_with_dart(
        self,
        issue: pd.Series | dict[str, Any],
        *,
        force: bool = False,
        days: int = 540,
    ) -> dict[str, Any] | None:
        if self.dart_client is None:
            return None
        parser = DartIPOParser(self.dart_client, base_dir=self.cache.base_dir)
        stock_code = str((issue.get("symbol") if isinstance(issue, dict) else issue.get("symbol")) or "").strip() or None
        corp_name = str((issue.get("name") if isinstance(issue, dict) else issue.get("name")) or "").strip() or None
        try:
            return parser.analyze_company(stock_code=stock_code, corp_name=corp_name, force=force, days=days)
        except Exception:
            return None

    def batch_enrich_issues_from_dart(
        self,
        issues: pd.DataFrame,
        *,
        max_items: int = 10,
        only_missing: bool = True,
        force: bool = False,
        days: int = 540,
    ) -> pd.DataFrame:
        if self.dart_client is None or issues is None or issues.empty:
            return pd.DataFrame()
        work = standardize_issue_frame(issues.copy())
        if only_missing:
            work = work[
                work[["lockup_commitment_ratio", "circulating_shares_ratio_on_listing", "existing_shareholder_ratio", "employee_forfeit_ratio"]].isna().any(axis=1)
            ]
        if work.empty:
            return pd.DataFrame()
        parser = DartIPOParser(self.dart_client, base_dir=self.cache.base_dir)
        rows: list[dict[str, Any]] = []
        for _, issue in work.head(max_items).iterrows():
            try:
                snapshot = parser.analyze_company(
                    stock_code=str(issue.get("symbol") or "").strip() or None,
                    corp_name=str(issue.get("name") or "").strip() or None,
                    force=force,
                    days=days,
                )
            except Exception as exc:
                rows.append({"name": issue.get("name"), "symbol": issue.get("symbol"), "status": "error", "detail": str(exc)})
                continue
            overlay = parser.snapshot_to_issue_overlay(snapshot)
            row = issue.to_dict()
            for key, value in overlay.items():
                row[f"current_{key}"] = row.get(key)
                row[key] = value if value is not None else row.get(key)
            row["status"] = "ok"
            row["detail"] = snapshot.get("filing", {}).get("report_nm")
            row["dart_summary"] = parser.build_snapshot_note(snapshot)
            rows.append(row)
        return pd.DataFrame(rows)

    def load_company_filings(self, *, stock_code: str | None = None, corp_name: str | None = None, days: int = 365) -> pd.DataFrame:
        if self.dart_client is None:
            return pd.DataFrame()
        end = today_kst()
        start = end - pd.Timedelta(days=days)
        try:
            return self.dart_client.latest_company_filings(
                stock_code=stock_code,
                corp_name=corp_name,
                bgn_de=start.strftime("%Y%m%d"),
                end_de=end.strftime("%Y%m%d"),
                page_count=20,
                base_dir=self.cache.base_dir,
            )
        except Exception:
            return pd.DataFrame()
