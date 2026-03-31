from __future__ import annotations

import json
import re
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from lxml import html as lxml_html

from src.services.dart_client import DartClient
from src.utils import cache_dir, ensure_dir, fmt_date, normalize_name_key, safe_float


HTML_EXTENSIONS = {".html", ".htm", ".xhtml", ".xml", ".txt"}
METRIC_FIELDS = [
    "offer_price",
    "total_offer_shares",
    "new_shares",
    "selling_shares",
    "secondary_sale_ratio",
    "post_listing_total_shares",
    "existing_shareholder_ratio",
    "lockup_commitment_ratio",
    "employee_subscription_ratio",
    "employee_forfeit_ratio",
    "circulating_shares_on_listing",
    "circulating_shares_ratio_on_listing",
]


class DartIPOParser:
    def __init__(self, dart_client: DartClient | None, base_dir: Path | str | None = None) -> None:
        self.dart_client = dart_client
        self.base_dir = Path(base_dir) if base_dir is not None else cache_dir()
        self.snapshot_dir = ensure_dir(self.base_dir / "dart_ipo")

    def snapshot_cache_path(self, corp_code: str) -> Path:
        return self.snapshot_dir / f"{corp_code}_snapshot.json"

    def load_cached_snapshot(self, corp_code: str) -> dict[str, Any] | None:
        path = self.snapshot_cache_path(corp_code)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def save_snapshot(self, corp_code: str, payload: dict[str, Any]) -> Path:
        path = self.snapshot_cache_path(corp_code)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def analyze_company(
        self,
        *,
        stock_code: str | None = None,
        corp_name: str | None = None,
        days: int = 540,
        force: bool = False,
    ) -> dict[str, Any]:
        if self.dart_client is None:
            raise RuntimeError("DART_API_KEY가 없어 자동추출을 수행할 수 없습니다.")

        company = self.dart_client.lookup_company(stock_code=stock_code, corp_name=corp_name, base_dir=self.base_dir)
        if company is None:
            raise RuntimeError("DART corp code에서 회사를 찾지 못했습니다.")

        corp_code = str(company.get("corp_code") or "").strip()
        cached = None if force else self.load_cached_snapshot(corp_code)
        if cached:
            return cached

        end = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None).normalize()
        start = end - pd.Timedelta(days=days)
        filings = self.dart_client.latest_company_filings(
            stock_code=stock_code,
            corp_name=corp_name,
            bgn_de=start.strftime("%Y%m%d"),
            end_de=end.strftime("%Y%m%d"),
            page_count=30,
            base_dir=self.base_dir,
        )
        preferred = self.select_preferred_filing(filings)
        if preferred is None:
            raise RuntimeError("투자설명서/증권신고서 계열 공시를 찾지 못했습니다.")

        structured = self.load_structured_tables(
            corp_code=corp_code,
            bgn_de=start.strftime("%Y%m%d"),
            end_de=end.strftime("%Y%m%d"),
            rcept_no=str(preferred.get("rcept_no") or ""),
        )
        files = self.load_document_files(str(preferred.get("rcept_no") or ""))
        snapshot = self.parse_package(files=files, structured_tables=structured, filing=preferred, company=company)
        snapshot["cache_path"] = str(self.save_snapshot(corp_code, snapshot))
        return snapshot

    def select_preferred_filing(self, filings: pd.DataFrame) -> dict[str, Any] | None:
        if filings is None or filings.empty:
            return None
        work = filings.copy()
        work["rcept_dt"] = pd.to_datetime(work.get("rcept_dt"), errors="coerce")

        def score_report(name: Any) -> int:
            text = str(name or "")
            score = 0
            if "투자설명서" in text:
                score += 100
            if "증권신고서" in text:
                score += 80
            if "증권발행실적보고서" in text:
                score += 60
            if "정정" in text:
                score += 10
            if "철회" in text:
                score -= 100
            return score

        work["_report_score"] = work.get("report_nm", "").map(score_report)
        work = work.sort_values(["_report_score", "rcept_dt", "report_nm"], ascending=[False, False, True]).reset_index(drop=True)
        if work.empty or int(work.iloc[0]["_report_score"]) <= 0:
            return None
        row = work.iloc[0].drop(labels=["_report_score"]).to_dict()
        if pd.notna(row.get("rcept_dt")):
            row["rcept_dt"] = pd.Timestamp(row["rcept_dt"]).strftime("%Y-%m-%d")
        return row

    def load_structured_tables(
        self,
        *,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        rcept_no: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        if self.dart_client is None:
            return {}
        try:
            groups = self.dart_client.equity_registration_statement(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        except Exception:
            return {}
        if not groups:
            return {}
        if not rcept_no:
            return groups
        filtered: dict[str, pd.DataFrame] = {}
        for title, df in groups.items():
            if df.empty or "rcept_no" not in df.columns:
                filtered[title] = df
                continue
            matched = df[df["rcept_no"].astype(str) == str(rcept_no)].copy()
            filtered[title] = matched if not matched.empty else df.head(0)
        return filtered

    def load_document_files(self, rcept_no: str) -> list[dict[str, Any]]:
        if self.dart_client is None:
            return []
        return self.dart_client.extract_document_files(rcept_no=rcept_no, base_dir=self.base_dir)

    def parse_package(
        self,
        *,
        files: list[dict[str, Any]],
        structured_tables: dict[str, pd.DataFrame] | None = None,
        filing: dict[str, Any] | None = None,
        company: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        structured_tables = structured_tables or {}
        files = files or []

        cleaned_files = self._prepare_files(files)
        all_lines = self._collect_lines(cleaned_files, structured_tables)
        metrics, evidence_rows = self._extract_metrics(all_lines, structured_tables)
        evidence_df = pd.DataFrame(evidence_rows)
        overlay = self.snapshot_to_issue_overlay({"metrics": metrics, "filing": filing or {}, "company": company or {}})

        structured_preview: dict[str, list[dict[str, Any]]] = {}
        for title, df in structured_tables.items():
            if df is None or df.empty:
                structured_preview[title] = []
            else:
                structured_preview[title] = df.head(20).where(pd.notna(df), None).to_dict(orient="records")

        snapshot = {
            "company": {
                "corp_code": str((company or {}).get("corp_code") or ""),
                "corp_name": str((company or {}).get("corp_name") or ""),
                "stock_code": str((company or {}).get("stock_code") or "").zfill(6) if (company or {}).get("stock_code") else "",
            },
            "filing": {
                "rcept_no": str((filing or {}).get("rcept_no") or ""),
                "report_nm": str((filing or {}).get("report_nm") or ""),
                "rcept_dt": str((filing or {}).get("rcept_dt") or ""),
                "viewer_url": str((filing or {}).get("viewer_url") or ""),
            },
            "metrics": {k: self._json_value(v) for k, v in metrics.items()},
            "overlay": {k: self._json_value(v) for k, v in overlay.items()},
            "evidence": evidence_df.where(pd.notna(evidence_df), None).to_dict(orient="records"),
            "structured_tables": structured_preview,
            "document_file_count": len(cleaned_files),
            "parsed_at": pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S %Z"),
        }
        return snapshot

    @staticmethod
    def snapshot_to_issue_overlay(snapshot: dict[str, Any]) -> dict[str, Any]:
        metrics = snapshot.get("metrics", {})
        filing = snapshot.get("filing", {})
        company = snapshot.get("company", {})
        overlay = {
            "symbol": str(company.get("stock_code") or "").zfill(6) if str(company.get("stock_code") or "").isdigit() else None,
            "offer_price": metrics.get("offer_price"),
            "lockup_commitment_ratio": metrics.get("lockup_commitment_ratio"),
            "employee_subscription_ratio": metrics.get("employee_subscription_ratio"),
            "employee_forfeit_ratio": metrics.get("employee_forfeit_ratio"),
            "circulating_shares_on_listing": metrics.get("circulating_shares_on_listing"),
            "circulating_shares_ratio_on_listing": metrics.get("circulating_shares_ratio_on_listing"),
            "existing_shareholder_ratio": metrics.get("existing_shareholder_ratio"),
            "total_offer_shares": metrics.get("total_offer_shares"),
            "new_shares": metrics.get("new_shares"),
            "selling_shares": metrics.get("selling_shares"),
            "secondary_sale_ratio": metrics.get("secondary_sale_ratio"),
            "post_listing_total_shares": metrics.get("post_listing_total_shares"),
            "dart_receipt_no": filing.get("rcept_no"),
            "dart_viewer_url": filing.get("viewer_url"),
            "dart_report_nm": filing.get("report_nm"),
            "dart_filing_date": filing.get("rcept_dt"),
            "notes": DartIPOParser.build_snapshot_note(snapshot),
        }
        return overlay

    @staticmethod
    def build_snapshot_note(snapshot: dict[str, Any]) -> str:
        metrics = snapshot.get("metrics", {})
        parts: list[str] = []
        if metrics.get("secondary_sale_ratio") is not None:
            parts.append(f"구주매출 {float(metrics['secondary_sale_ratio']):.2f}%")
        if metrics.get("circulating_shares_ratio_on_listing") is not None:
            parts.append(f"상장직후 유통 {float(metrics['circulating_shares_ratio_on_listing']):.2f}%")
        if metrics.get("lockup_commitment_ratio") is not None:
            parts.append(f"확약 {float(metrics['lockup_commitment_ratio']):.2f}%")
        filing = snapshot.get("filing", {})
        if filing.get("rcept_dt"):
            parts.append(f"DART {filing['rcept_dt']}")
        return " · ".join(parts)

    def _prepare_files(self, files: list[dict[str, Any]]) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for item in files:
            name = str(item.get("name") or "")
            if Path(name).suffix.lower() not in HTML_EXTENSIONS:
                continue
            raw_text = str(item.get("text") or "")
            if not raw_text.strip():
                continue
            body_text = self._html_to_text(raw_text)
            tables = self._extract_tables(raw_text)
            prepared.append({"name": name, "text": body_text, "tables": tables})
        prepared.sort(key=lambda x: (0 if any(token in x["name"].lower() for token in ["index", "main", "본문", "report"]) else 1, len(x["text"])), reverse=False)
        return prepared[:30]

    def _collect_lines(self, files: list[dict[str, Any]], structured_tables: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
        lines: list[dict[str, Any]] = []
        for file in files:
            name = file["name"]
            for sentence in self._split_sentences(file["text"]):
                if len(sentence) < 5:
                    continue
                lines.append({"source": f"doc:{name}", "text": sentence})
            for idx, table in enumerate(file.get("tables", []), start=1):
                for line in self._table_lines(table):
                    lines.append({"source": f"table:{name}#{idx}", "text": line})
        for title, table in structured_tables.items():
            if table is None or table.empty:
                continue
            for line in self._table_lines(table):
                lines.append({"source": f"estkRs:{title}", "text": line})
        return lines

    def _extract_metrics(self, lines: list[dict[str, Any]], structured_tables: dict[str, pd.DataFrame]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        metrics = self._extract_structured_metrics(structured_tables)
        evidence_rows: list[dict[str, Any]] = []

        def apply_evidence(metric: str, result: dict[str, Any] | None) -> None:
            if not result:
                return
            if result.get("value") is not None and metrics.get(metric) is None:
                metrics[metric] = result["value"]
            evidence_rows.append(
                {
                    "metric": metric,
                    "value": result.get("value"),
                    "source": result.get("source"),
                    "basis": result.get("basis"),
                    "excerpt": result.get("excerpt"),
                }
            )

        apply_evidence("lockup_commitment_ratio", self._best_percentage_line(lines, [["의무보유", "확약"], ["보유확약"]], prefer_total=False))
        apply_evidence(
            "employee_forfeit_ratio",
            self._extract_employee_forfeit(lines, structured_tables),
        )
        apply_evidence(
            "employee_subscription_ratio",
            self._extract_employee_subscription(lines, structured_tables, metrics),
        )
        circ = self._extract_circulating(lines)
        if circ:
            if metrics.get("circulating_shares_on_listing") is None:
                metrics["circulating_shares_on_listing"] = circ.get("shares")
            apply_evidence("circulating_shares_ratio_on_listing", circ)
        apply_evidence(
            "existing_shareholder_ratio",
            self._extract_existing_shareholder_ratio(lines, metrics),
        )
        apply_evidence("post_listing_total_shares", self._extract_post_listing_total_shares(lines))

        if metrics.get("post_listing_total_shares") is None:
            post_listing_evidence = self._extract_post_listing_total_shares(lines)
            if post_listing_evidence:
                metrics["post_listing_total_shares"] = post_listing_evidence.get("value")
                evidence_rows.append(
                    {
                        "metric": "post_listing_total_shares",
                        "value": post_listing_evidence.get("value"),
                        "source": post_listing_evidence.get("source"),
                        "basis": post_listing_evidence.get("basis"),
                        "excerpt": post_listing_evidence.get("excerpt"),
                    }
                )

        if metrics.get("existing_shareholder_ratio") is None:
            post = safe_float(metrics.get("post_listing_total_shares"))
            new_shares = safe_float(metrics.get("new_shares"))
            if post and new_shares is not None and post > 0:
                metrics["existing_shareholder_ratio"] = round((post - new_shares) / post * 100, 4)
                evidence_rows.append(
                    {
                        "metric": "existing_shareholder_ratio",
                        "value": metrics["existing_shareholder_ratio"],
                        "source": "derived",
                        "basis": "(상장후 총주식수 - 신주모집수) / 상장후 총주식수",
                        "excerpt": f"post_listing_total_shares={int(post):,}, new_shares={int(new_shares):,}",
                    }
                )

        if metrics.get("secondary_sale_ratio") is None:
            offer = safe_float(metrics.get("total_offer_shares"))
            selling = safe_float(metrics.get("selling_shares"))
            if offer and selling is not None and offer > 0:
                metrics["secondary_sale_ratio"] = round(selling / offer * 100, 4)

        return metrics, evidence_rows

    def _extract_structured_metrics(self, structured_tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
        metrics = {field: None for field in METRIC_FIELDS}

        securities = structured_tables.get("증권의종류", pd.DataFrame())
        sellers = structured_tables.get("매출인에관한사항", pd.DataFrame())
        general = structured_tables.get("일반사항", pd.DataFrame())
        underwriters = structured_tables.get("인수인정보", pd.DataFrame())

        total_offer_shares = self._sum_numeric_column(securities, "stkcnt")
        offer_price = self._first_numeric_value(securities, ["slprc"])
        selling_shares = self._sum_numeric_column(sellers, "slstk")
        if total_offer_shares is not None:
            metrics["total_offer_shares"] = total_offer_shares
        if offer_price is not None:
            metrics["offer_price"] = offer_price
        if selling_shares is not None:
            metrics["selling_shares"] = selling_shares
        if total_offer_shares is not None and selling_shares is not None:
            metrics["new_shares"] = total_offer_shares - selling_shares
            if total_offer_shares > 0:
                metrics["secondary_sale_ratio"] = round(selling_shares / total_offer_shares * 100, 4)
        elif total_offer_shares is not None:
            metrics["new_shares"] = total_offer_shares

        if not underwriters.empty and "actnmn" in underwriters.columns:
            names = sorted({str(x).strip() for x in underwriters["actnmn"].tolist() if str(x).strip() and str(x).strip() != "nan"})
            if names:
                metrics["underwriters"] = ", ".join(names)

        if not general.empty:
            if metrics.get("offer_price") is None:
                metrics["offer_price"] = self._first_numeric_value(general, ["slprc", "exprc"])

        return metrics

    def _extract_employee_subscription(self, lines: list[dict[str, Any]], structured_tables: dict[str, pd.DataFrame], metrics: dict[str, Any]) -> dict[str, Any] | None:
        offer = safe_float(metrics.get("total_offer_shares"))
        if offer:
            for title, table in structured_tables.items():
                if table is None or table.empty:
                    continue
                normalized = self._normalize_table(table)
                if normalized.empty:
                    continue
                joined = normalized.astype(str).agg(" | ".join, axis=1)
                target_idx = joined[joined.str.contains("우리사주", regex=False)].index.tolist()
                if not target_idx:
                    continue
                for idx in target_idx:
                    row = normalized.loc[idx]
                    qty_col = next((c for c in normalized.columns if "배정" in str(c) or "수량" in str(c)), None)
                    if qty_col is None:
                        continue
                    qty = safe_float(row.get(qty_col))
                    if qty is None:
                        continue
                    return {
                        "value": round(qty / offer * 100, 4),
                        "source": f"estkRs:{title}",
                        "basis": "우리사주 배정수량 / 전체 공모주식수",
                        "excerpt": f"배정수량 {qty:,.0f} / 총공모주식수 {offer:,.0f}",
                    }
            for item in lines:
                text = str(item["text"])
                compact = text.replace(" ", "")
                if "우리사주" not in compact or "배정" not in compact:
                    continue
                numbers = [safe_float(x) for x in re.findall(r"([0-9][0-9,]{2,})", text)]
                numbers = [x for x in numbers if x is not None and 0 < x <= offer]
                if not numbers:
                    continue
                qty = float(numbers[0])
                return {
                    "value": round(qty / offer * 100, 4),
                    "source": item["source"],
                    "basis": "문서 내 우리사주 배정수량 / 전체 공모주식수",
                    "excerpt": text,
                }
        result = self._best_percentage_line(lines, [["우리사주", "배정비율"], ["우리사주", "배정", "비율"]], prefer_total=False)
        if result and result.get("value") is not None:
            result["basis"] = "문서 내 우리사주 배정 관련 비율"
        return result

    def _extract_employee_forfeit(self, lines: list[dict[str, Any]], structured_tables: dict[str, pd.DataFrame]) -> dict[str, Any] | None:
        best = self._best_percentage_line(lines, [["우리사주", "실권"], ["우리사주", "미청약"], ["실권율"]], prefer_total=False)
        if best:
            return best

        for title, table in structured_tables.items():
            if table is None or table.empty:
                continue
            normalized = self._normalize_table(table)
            if normalized.empty:
                continue
            row_text = normalized.astype(str).agg(" | ".join, axis=1)
            targets = row_text[row_text.str.contains("우리사주", regex=False)].index.tolist()
            if not targets:
                continue
            alloc_col = next((c for c in normalized.columns if "배정" in str(c)), None)
            forfeit_col = next((c for c in normalized.columns if "실권" in str(c)), None)
            ratio_col = next((c for c in normalized.columns if "실권율" in str(c) or ("실권" in str(c) and "%" in str(c))), None)
            for idx in targets:
                row = normalized.loc[idx]
                if ratio_col:
                    ratio = safe_float(row.get(ratio_col))
                    if ratio is not None:
                        return {
                            "value": ratio,
                            "source": f"estkRs:{title}",
                            "basis": "우리사주 실권율 컬럼",
                            "excerpt": row_text.loc[idx],
                        }
                alloc = safe_float(row.get(alloc_col)) if alloc_col else None
                forfeit = safe_float(row.get(forfeit_col)) if forfeit_col else None
                if alloc and forfeit is not None and alloc > 0:
                    return {
                        "value": round(forfeit / alloc * 100, 4),
                        "source": f"estkRs:{title}",
                        "basis": "실권수량 / 우리사주 배정수량",
                        "excerpt": row_text.loc[idx],
                    }
        return None

    def _extract_circulating(self, lines: list[dict[str, Any]]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        keywords = ["유통가능", "상장직후"]
        fallback_keywords = ["유통 가능", "상장 직후"]
        for item in lines:
            text = str(item["text"])
            if not self._contains_all(text, keywords) and not self._contains_all(text, fallback_keywords):
                continue
            pct = self._best_percent_from_text(text)
            shares = self._best_shares_from_text(text)
            if pct is None and shares is None:
                continue
            score = 0
            if "상장" in text:
                score += 3
            if "직후" in text:
                score += 3
            if "유통가능" in text.replace(" ", ""):
                score += 3
            if "합계" in text:
                score += 2
            candidates.append(
                {
                    "value": pct,
                    "shares": shares,
                    "source": item["source"],
                    "basis": "상장 직후 유통가능 물량 문구",
                    "excerpt": text,
                    "score": score,
                }
            )
        if not candidates:
            return None
        candidates.sort(key=lambda x: ((x.get("value") is None), -int(x["score"]), -(x.get("value") or 0.0), -(x.get("shares") or 0.0)))
        best = candidates[0].copy()
        best.pop("score", None)
        return best

    def _extract_existing_shareholder_ratio(self, lines: list[dict[str, Any]], metrics: dict[str, Any]) -> dict[str, Any] | None:
        explicit = self._best_percentage_line(lines, [["기존주주", "비율"], ["기존주주", "지분"], ["주주", "보유", "비율"]], prefer_total=False)
        if explicit:
            return explicit
        post = safe_float(metrics.get("post_listing_total_shares"))
        new_shares = safe_float(metrics.get("new_shares"))
        if post and new_shares is not None and post > 0:
            value = round((post - new_shares) / post * 100, 4)
            return {
                "value": value,
                "source": "derived",
                "basis": "(상장후 총주식수 - 신주모집수) / 상장후 총주식수",
                "excerpt": f"post_listing_total_shares={int(post):,}, new_shares={int(new_shares):,}",
            }
        return None

    def _extract_post_listing_total_shares(self, lines: list[dict[str, Any]]) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        keyword_sets = [
            ["상장예정주식수"],
            ["공모후", "발행주식총수"],
            ["상장후", "발행주식총수"],
            ["공모 후", "총 발행주식수"],
        ]
        for item in lines:
            text = str(item["text"])
            compact = text.replace(" ", "")
            if not any(all(keyword in compact for keyword in [k.replace(" ", "") for k in keys]) for keys in keyword_sets):
                continue
            shares = self._best_shares_from_text(text)
            if shares is None:
                continue
            score = 0
            if "상장예정주식수" in compact:
                score += 4
            if "발행주식총수" in compact:
                score += 3
            if shares > 10000:
                score += 1
            candidates.append(
                {
                    "value": shares,
                    "source": item["source"],
                    "basis": "상장후/공모후 총주식수 문구",
                    "excerpt": text,
                    "score": score,
                }
            )
        if not candidates:
            return None
        candidates.sort(key=lambda x: (-int(x["score"]), -(x.get("value") or 0.0)))
        best = candidates[0].copy()
        best.pop("score", None)
        return best

    def _best_percentage_line(
        self,
        lines: list[dict[str, Any]],
        keyword_options: list[list[str]],
        *,
        prefer_total: bool = False,
    ) -> dict[str, Any] | None:
        candidates: list[dict[str, Any]] = []
        for item in lines:
            text = str(item["text"])
            compact = text.replace(" ", "")
            matched_keywords: list[str] | None = None
            for keywords in keyword_options:
                if all(keyword.replace(" ", "") in compact for keyword in keywords):
                    matched_keywords = keywords
                    break
            if matched_keywords is None:
                continue
            pct = self._best_percent_from_text(text)
            if pct is None:
                continue
            score = len(matched_keywords) * 5
            if "합계" in text or "총계" in text:
                score += 4
            if prefer_total and ("합계" in text or "총계" in text):
                score += 5
            if "비율" in text or "%" in text:
                score += 2
            if "미확약" in text:
                score -= 3
            candidates.append(
                {
                    "value": pct,
                    "source": item["source"],
                    "basis": ", ".join(matched_keywords),
                    "excerpt": text,
                    "score": score,
                }
            )
        if not candidates:
            return None
        candidates.sort(key=lambda x: (-int(x["score"]), abs((x.get("value") or 0.0) - 100.0)))
        best = candidates[0].copy()
        best.pop("score", None)
        return best

    @staticmethod
    def _html_to_text(raw_text: str) -> str:
        try:
            tree = lxml_html.fromstring(raw_text)
            text = tree.text_content()
        except Exception:
            text = re.sub(r"<[^>]+>", " ", raw_text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[\t\r\f\v]+", " ", text)
        text = re.sub(r"\n+", "\n", text)
        text = re.sub(r" +", " ", text)
        return text

    @staticmethod
    def _extract_tables(raw_text: str) -> list[pd.DataFrame]:
        try:
            tables = pd.read_html(StringIO(raw_text), displayed_only=False)
        except Exception:
            return []
        cleaned: list[pd.DataFrame] = []
        for table in tables[:20]:
            if table is None or table.empty:
                continue
            cleaned.append(DartIPOParser._normalize_table(table))
        return cleaned

    @staticmethod
    def _normalize_table(df: pd.DataFrame) -> pd.DataFrame:
        work = df.copy()
        columns: list[str] = []
        for i, col in enumerate(work.columns):
            if isinstance(col, tuple):
                parts = [str(x).strip() for x in col if str(x).strip() and not str(x).lower().startswith("unnamed")]
                label = " ".join(parts) or f"col_{i}"
            else:
                label = str(col).strip() or f"col_{i}"
            columns.append(re.sub(r"\s+", " ", label))
        work.columns = columns
        work = work.dropna(axis=1, how="all")
        return work

    @staticmethod
    def _table_lines(df: pd.DataFrame) -> list[str]:
        work = DartIPOParser._normalize_table(df)
        lines: list[str] = []
        header = " | ".join(str(c) for c in work.columns)
        if header.strip():
            lines.append(header)
        for _, row in work.head(120).iterrows():
            values = [str(v).replace("\n", " ").strip() for v in row.tolist() if str(v).strip() and str(v).strip() != "nan"]
            if values:
                row_line = " | ".join(values)
                lines.append(row_line)
                if header.strip():
                    lines.append(f"{header} || {row_line}")
        return lines

    @staticmethod
    def _split_sentences(text: str) -> list[str]:
        if not text:
            return []
        raw_lines = [line.strip() for line in text.split("\n") if line.strip()]
        sentences: list[str] = []
        for raw in raw_lines:
            parts = re.split(r"(?<=[\.!?다요%])\s+", raw)
            for part in parts:
                part = re.sub(r"\s+", " ", part).strip()
                if part:
                    sentences.append(part)
        return sentences

    @staticmethod
    def _contains_all(text: str, keywords: list[str]) -> bool:
        compact = text.replace(" ", "")
        return all(keyword.replace(" ", "") in compact for keyword in keywords)

    @staticmethod
    def _best_percent_from_text(text: str) -> float | None:
        matches = [safe_float(x) for x in re.findall(r"([0-9]{1,3}(?:\.[0-9]+)?)\s*%", text)]
        matches = [x for x in matches if x is not None]
        if not matches:
            return None
        # Prefer values below 100 unless the line is explicitly total/합계.
        non_total = [x for x in matches if x < 100]
        if non_total:
            return float(non_total[0])
        return float(matches[0])

    @staticmethod
    def _best_shares_from_text(text: str) -> float | None:
        matches = [safe_float(x) for x in re.findall(r"([0-9][0-9,]{2,})\s*주", text)]
        matches = [x for x in matches if x is not None]
        if not matches:
            return None
        return float(max(matches))

    @staticmethod
    def _sum_numeric_column(df: pd.DataFrame, column: str) -> float | None:
        if df is None or df.empty or column not in df.columns:
            return None
        values = pd.to_numeric(df[column].astype(str).str.replace(",", "", regex=False), errors="coerce").dropna()
        if values.empty:
            return None
        return float(values.sum())

    @staticmethod
    def _first_numeric_value(df: pd.DataFrame, columns: list[str]) -> float | None:
        if df is None or df.empty:
            return None
        for column in columns:
            if column not in df.columns:
                continue
            values = pd.to_numeric(df[column].astype(str).str.replace(",", "", regex=False), errors="coerce").dropna()
            if not values.empty:
                return float(values.iloc[0])
        return None

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, (pd.Int64Dtype, pd.Float64Dtype)):
            return float(value)
        if isinstance(value, float):
            if pd.isna(value):
                return None
            return float(value)
        if isinstance(value, int):
            return int(value)
        if pd.isna(value):
            return None
        return value


def snapshot_evidence_frame(snapshot: dict[str, Any]) -> pd.DataFrame:
    rows = snapshot.get("evidence", []) or []
    return pd.DataFrame(rows)


def snapshot_overlay_frame(issue: pd.Series, snapshot: dict[str, Any]) -> pd.DataFrame:
    overlay = snapshot.get("overlay", {})
    rows: list[dict[str, Any]] = []
    field_labels = {
        "offer_price": "공모가",
        "lockup_commitment_ratio": "확약비율",
        "employee_subscription_ratio": "우리사주 배정비율",
        "employee_forfeit_ratio": "우리사주 실권",
        "circulating_shares_ratio_on_listing": "상장일 유통가능물량",
        "existing_shareholder_ratio": "기존주주비율",
        "dart_receipt_no": "DART 접수번호",
        "dart_viewer_url": "DART 뷰어",
        "notes": "노트",
    }
    for field, label in field_labels.items():
        rows.append(
            {
                "field": field,
                "label": label,
                "before": issue.get(field),
                "after": overlay.get(field),
            }
        )
    return pd.DataFrame(rows)


def snapshot_summary_text(snapshot: dict[str, Any]) -> str:
    company = snapshot.get("company", {})
    filing = snapshot.get("filing", {})
    metrics = snapshot.get("metrics", {})
    parts = [company.get("corp_name") or "미상회사"]
    if filing.get("report_nm"):
        parts.append(str(filing["report_nm"]))
    if filing.get("rcept_dt"):
        parts.append(str(filing["rcept_dt"]))
    metric_bits: list[str] = []
    if metrics.get("lockup_commitment_ratio") is not None:
        metric_bits.append(f"확약 {float(metrics['lockup_commitment_ratio']):.2f}%")
    if metrics.get("circulating_shares_ratio_on_listing") is not None:
        metric_bits.append(f"유통 {float(metrics['circulating_shares_ratio_on_listing']):.2f}%")
    if metrics.get("secondary_sale_ratio") is not None:
        metric_bits.append(f"구주매출 {float(metrics['secondary_sale_ratio']):.2f}%")
    if metric_bits:
        parts.append(" / ".join(metric_bits))
    return " · ".join([p for p in parts if p])
