from __future__ import annotations

import pandas as pd

from src.utils import clip_score, safe_float, score_percentile


class IPOScorer:
    def add_scores(self, issues: pd.DataFrame) -> pd.DataFrame:
        score_columns = ["subscription_score", "listing_quality_score", "unlock_pressure_score", "overall_score"]
        if issues is None:
            return pd.DataFrame(columns=score_columns)
        if issues.empty:
            out = issues.copy()
            for col in score_columns:
                if col not in out.columns:
                    out[col] = pd.Series(index=out.index, dtype="float64")
            return out
        out = issues.copy()
        out["subscription_score"] = out.apply(self.subscription_score, axis=1)
        out["listing_quality_score"] = out.apply(self.listing_quality_score, axis=1)
        out["unlock_pressure_score"] = out.apply(self.unlock_pressure_score, axis=1)
        out["overall_score"] = (
            out["subscription_score"].fillna(0) * 0.35
            + out["listing_quality_score"].fillna(0) * 0.40
            - out["unlock_pressure_score"].fillna(0) * 0.15
            + out["subscription_score"].fillna(0) * 0.40
        ).round(1)
        for col in score_columns:
            if col not in out.columns:
                out[col] = pd.Series(index=out.index, dtype="float64")
        return out

    def subscription_score(self, row: pd.Series) -> float:
        institution = safe_float(row.get("institutional_competition_ratio"), 0.0) or 0.0
        retail_live = safe_float(row.get("retail_competition_ratio_live"), 0.0) or 0.0
        offer_price = safe_float(row.get("offer_price"), 0.0) or 0.0
        score = 0.0
        score += score_percentile(institution, [(0, 0), (300, 25), (800, 55), (1500, 85), (2500, 100)], default=0)
        score += score_percentile(retail_live, [(0, 0), (100, 10), (300, 25), (700, 45), (1500, 55)], default=0)
        if offer_price > 0:
            score += score_percentile(offer_price, [(2000, 5), (10000, 8), (20000, 12), (50000, 8), (100000, 5)], default=5)
        return round(clip_score(score, 0, 100), 1)

    def listing_quality_score(self, row: pd.Series) -> float:
        lockup = safe_float(row.get("lockup_commitment_ratio"), 0.0) or 0.0
        float_ratio = safe_float(row.get("circulating_shares_ratio_on_listing"), 0.0) or 0.0
        existing = safe_float(row.get("existing_shareholder_ratio"), 0.0) or 0.0
        employee_forfeit = safe_float(row.get("employee_forfeit_ratio"), 0.0) or 0.0
        current_price = safe_float(row.get("current_price"))
        offer_price = safe_float(row.get("offer_price"))
        score = 0.0
        score += score_percentile(lockup, [(0, 0), (5, 10), (10, 20), (20, 35), (40, 45)], default=0)
        score += score_percentile(float_ratio, [(15, 30), (25, 22), (35, 12), (50, 5), (80, 0)], default=0)
        score += score_percentile(existing, [(20, 20), (40, 16), (60, 10), (80, 4)], default=0)
        score += score_percentile(employee_forfeit, [(0, 10), (1, 8), (3, 5), (5, 1)], default=0)
        if current_price and offer_price:
            premium = (current_price / offer_price - 1.0) * 100
            score += score_percentile(premium, [(-30, 0), (-10, 6), (0, 10), (20, 15), (60, 8), (120, 4)], default=0)
        return round(clip_score(score, 0, 100), 1)

    def unlock_pressure_score(self, row: pd.Series) -> float:
        float_ratio = safe_float(row.get("circulating_shares_ratio_on_listing"), 0.0) or 0.0
        existing = safe_float(row.get("existing_shareholder_ratio"), 0.0) or 0.0
        lockup = safe_float(row.get("lockup_commitment_ratio"), 0.0) or 0.0
        day_change = abs(safe_float(row.get("day_change_pct"), 0.0) or 0.0)
        score = 0.0
        score += score_percentile(float_ratio, [(10, 5), (20, 15), (35, 35), (50, 60), (80, 90)], default=0)
        score += score_percentile(existing, [(20, 5), (40, 15), (60, 30), (80, 45)], default=0)
        score += score_percentile(day_change, [(0, 0), (3, 5), (7, 12), (12, 18)], default=0)
        score -= score_percentile(lockup, [(0, 0), (10, 4), (20, 10), (40, 18)], default=0)
        return round(clip_score(score, 0, 100), 1)
