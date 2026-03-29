from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


from pathlib import Path
from tempfile import TemporaryDirectory

import os
import pandas as pd

from src.services.alert_engine import AlertEngine, AlertSettings
from src.services.backtest_repository import BacktestRepository
from src.services.dart_ipo_parser import DartIPOParser
from src.services.execution_runtime import ExecutionRuntimeService
from src.services.ipo_pipeline import IPODataHub
from src.services.ipo_repository import IPORepository
from src.services.market_service import MarketService
from src.services.lockup_strategy_service import LockupStrategyService
from src.services.strategy_bridge import StrategyBridge
from src.services.unified_lab_bridge import UnifiedLabBridgeService
from src.utils import load_project_env


def parser_fixture_test() -> None:
    parser = DartIPOParser(None)
    fixture_html = """
    <html><body>
      <h2>의무보유 확약</h2>
      <p>기관투자자 의무보유 확약 비율은 14.80%입니다.</p>
      <h2>상장 직후 유통가능 물량</h2>
      <p>상장 직후 유통가능 물량은 상장예정주식수의 27.50%에 해당하는 2,750,000주입니다.</p>
      <h2>우리사주조합 배정결과</h2>
      <table>
        <tr><th>구분</th><th>배정수량</th><th>청약수량</th><th>실권수량</th><th>실권율</th></tr>
        <tr><td>우리사주조합</td><td>100,000</td><td>99,200</td><td>800</td><td>0.80%</td></tr>
      </table>
      <p>상장예정주식수는 10,000,000주이며 공모 후 발행주식총수는 10,000,000주입니다.</p>
      <p>기존주주 지분율은 공모 후 58.00%입니다.</p>
    </body></html>
    """
    structured_tables = {
        "증권의종류": pd.DataFrame(
            [
                {"rcept_no": "20260101000001", "stksen": "보통주", "stkcnt": "2,000,000", "slprc": "15,000", "slmthn": "일반공모"},
            ]
        ),
        "매출인에관한사항": pd.DataFrame(
            [
                {"rcept_no": "20260101000001", "hdr": "VC A", "slstk": "300,000"},
            ]
        ),
        "인수인정보": pd.DataFrame(
            [
                {"rcept_no": "20260101000001", "actnmn": "한국투자증권"},
            ]
        ),
    }
    snapshot = parser.parse_package(
        files=[{"name": "fixture.html", "text": fixture_html}],
        structured_tables=structured_tables,
        filing={"rcept_no": "20260101000001", "report_nm": "투자설명서", "rcept_dt": "2026-01-01"},
        company={"corp_code": "00000000", "corp_name": "테스트IPO", "stock_code": "000000"},
    )
    metrics = snapshot["metrics"]
    assert round(float(metrics["lockup_commitment_ratio"]), 2) == 14.80
    assert round(float(metrics["circulating_shares_ratio_on_listing"]), 2) == 27.50
    assert round(float(metrics["employee_forfeit_ratio"]), 2) == 0.80
    assert round(float(metrics["existing_shareholder_ratio"]), 2) == 58.00
    assert round(float(metrics["secondary_sale_ratio"]), 2) == 15.00
    assert int(float(metrics["new_shares"])) == 1700000


def env_loader_fixture_test() -> None:
    with TemporaryDirectory() as tmpdir:
        env_path = Path(tmpdir) / ".env"
        env_path.write_text("KIS_APP_KEY=test_key\nDART_API_KEY=test_dart\n")
        old_key = os.environ.get("KIS_APP_KEY")
        old_dart = os.environ.get("DART_API_KEY")
        os.environ.pop("KIS_APP_KEY", None)
        os.environ.pop("DART_API_KEY", None)
        loaded = load_project_env(env_path, override=True)
        assert loaded.get("KIS_APP_KEY") == "test_key"
        assert os.environ.get("DART_API_KEY") == "test_dart"
        if old_key is not None:
            os.environ["KIS_APP_KEY"] = old_key
        else:
            os.environ.pop("KIS_APP_KEY", None)
        if old_dart is not None:
            os.environ["DART_API_KEY"] = old_dart
        else:
            os.environ.pop("DART_API_KEY", None)


def empty_score_frame_fixture_test() -> None:
    from src.services.scoring import IPOScorer

    scorer = IPOScorer()
    empty = pd.DataFrame(columns=["name", "subscription_start", "listing_date"])
    scored = scorer.add_scores(empty)
    for col in ["subscription_score", "listing_quality_score", "unlock_pressure_score", "overall_score"]:
        assert col in scored.columns, f"missing score column: {col}"





def thirtyeight_duplicate_columns_fixture_test() -> None:
    from src.services.ipo_scrapers import standardize_38_schedule_table

    raw = pd.DataFrame(
        [
            ["테스트기업", "03.20~03.21", "10,000~12,000", "11,000", "850.5:1", "1,234.0:1", "한국투자증권", "2026-03-28"],
        ],
        columns=["종목명", "공모일정", "공모가", "공모가", "기관경쟁률", "청약경쟁률", "주간사", "상장일"],
    )
    out = standardize_38_schedule_table(raw, today=pd.Timestamp("2026-03-01"))
    assert not out.empty
    assert out.loc[0, "offer_price"] == 11000
    assert out.loc[0, "price_band_low"] == 10000
    assert out.loc[0, "price_band_high"] == 12000



def nullable_overlay_fixture_test() -> None:
    from src.services.ipo_scrapers import merge_live_sources

    kind = pd.DataFrame(
        [
            {
                "name": "카나프테라퓨틱스",
                "market": "코스닥",
                "listing_date": "2026-03-16",
                "offer_price": 20000,
                "source": "KIND",
            }
        ]
    )
    schedule = pd.DataFrame(
        [
            {
                "name": "카나프테라퓨틱스",
                "subscription_start": "2026-03-05",
                "subscription_end": "2026-03-06",
                "underwriters": "한국투자증권",
                "price_band_low": 16000,
                "price_band_high": 20000,
                "market": pd.NA,
                "source": "38",
            }
        ]
    )
    out = merge_live_sources(kind, schedule)
    assert out.loc[0, "market"] == "코스닥"
    assert pd.Timestamp(out.loc[0, "subscription_start"]) == pd.Timestamp("2026-03-05")
    assert out.loc[0, "stage"] == "상장후"


def date_parser_fixture_test() -> None:
    from src.utils import parse_date_range_text, parse_date_text

    start, end = parse_date_range_text("03.11(수) ~ 12(목)", default_year=2026)
    assert start == pd.Timestamp("2026-03-11")
    assert end == pd.Timestamp("2026-03-12")
    single = parse_date_text("2026.03.20(금)", default_year=2026)
    assert single == pd.Timestamp("2026-03-20")


def safe_bool_fixture_test() -> None:
    from src.utils import safe_bool, coalesce

    assert safe_bool(pd.NA, False) is False
    assert coalesce(pd.NA, None, "값") == "값"

def safe_float_fixture_test() -> None:
    from src.utils import safe_float

    assert safe_float("1,234.56:1") == 1234.56
    assert safe_float("47,200 원 (0.21%)") == 47200.0



def kind_public_offering_fixture_test() -> None:
    from src.services.ipo_scrapers import standardize_kind_public_offering_table

    raw = pd.DataFrame(
        [
            {
                "회사명": "테스트공모",
                "청약일정": "2026.04.01~04.02",
                "상장예정일": "2026.04.10",
                "확정공모가": "15,000",
                "상장주선인": "한국투자증권",
            }
        ]
    )
    out = standardize_kind_public_offering_table(raw, today=pd.Timestamp("2026-03-20"))
    assert not out.empty
    assert out.loc[0, "subscription_start"] == pd.Timestamp("2026-04-01")
    assert out.loc[0, "subscription_end"] == pd.Timestamp("2026-04-02")
    assert out.loc[0, "listing_date"] == pd.Timestamp("2026-04-10")
    assert out.loc[0, "offer_price"] == 15000



def kind_pubprice_fixture_test() -> None:
    from src.services.ipo_scrapers import standardize_kind_pubprice_table

    raw = pd.DataFrame(
        [
            {
                "회사명": "테스트상장",
                "상장일": "2026-03-10",
                "공모가": "12,000",
                "최근거래일 종가": "14,500",
                "최근거래일 등락률": "5.50%",
                "주관사": "삼성증권",
            }
        ]
    )
    out = standardize_kind_pubprice_table(raw, today=pd.Timestamp("2026-03-28"))
    assert not out.empty
    assert out.loc[0, "current_price"] == 14500
    assert out.loc[0, "day_change_pct"] == 5.5



def thirtyeight_detail_fixture_test() -> None:
    from src.services.ipo_scrapers import parse_38_detail_html

    html = """
    <html><body>
    <table>
      <tr><th>종목명</th><td>아이엠바이오로직스</td><th>시장구분</th><td>코스닥</td></tr>
      <tr><th>종목코드</th><td>123456</td><th>업종</th><td>의약품</td></tr>
      <tr><th>공모청약일</th><td>2026.03.18 ~ 2026.03.19</td><th>신규상장일</th><td>2026.03.20</td></tr>
      <tr><th>기관경쟁률</th><td>1,234.56:1</td><th>의무보유확약</th><td>12.34%</td></tr>
      <tr><th>확정공모가</th><td>15,000원</td><th>희망공모가액</th><td>13,000 ~ 15,000원</td></tr>
      <tr><th>현재가</th><td>18,200 원 (1.11%)</td><th>주간사</th><td>한국투자증권</td></tr>
      <tr><th>총공모주식수</th><td>2,635,000 주</td><th>상장공모</th><td>신주모집 : 2,135,000 주 (81.0%) / 구주매출 : 500,000 주 (19.0%)</td></tr>
      <tr><th>상장후총주식수</th><td>10,000,000 주</td><th></th><td></td></tr>
    </table>
    </body></html>
    """
    out = parse_38_detail_html(html)
    assert out["market"] == "코스닥"
    assert out["symbol"] == "123456"
    assert out["listing_date"] == pd.Timestamp("2026-03-20")
    assert out["institutional_competition_ratio"] == 1234.56
    assert out["lockup_commitment_ratio"] == 12.34
    assert out["current_price"] == 18200
    assert out["total_offer_shares"] == 2635000
    assert out["new_shares"] == 2135000
    assert out["selling_shares"] == 500000
    assert round(float(out["secondary_sale_ratio"]), 2) == 18.98
    assert out["post_listing_total_shares"] == 10000000


def thirtyeight_menu_blob_fixture_test() -> None:
    from src.services.ipo_scrapers import parse_38_detail_html

    html = """
    <html><body>
    <table>
      <tr><th>종목명</th><td>테스트바이오</td><th>시장구분</th><td>코스닥</td></tr>
      <tr><th>종목코드</th><td>0088D0</td><th>업종</th><td>비상장매매 시세정보 비상장(장외) IPO/공모 IPO예정분석 기업정보 주주동호회 공모주 청약일정 신규상장 증시캘린더</td></tr>
      <tr><th>주간사</th><td>메리츠증권</td><th>신규상장일</th><td>2026.04.01</td></tr>
    </table>
    </body></html>
    """
    out = parse_38_detail_html(html)
    assert out["name"] == "테스트바이오"
    assert out["market"] == "코스닥"
    assert out.get("symbol") is None or out.get("symbol") == ""
    assert out.get("sector") is None


def clean_issue_frame_fixture_test() -> None:
    from src.utils import clean_issue_frame

    raw = pd.DataFrame(
        [
            {
                "name": "테스트바이오",
                "market": "코스닥",
                "subscription_start": "2026-04-01",
                "subscription_end": "2026-04-02",
                "underwriters": "한국투자증권",
                "offer_price": 15000,
                "source": "38",
            },
            {
                "name": "[공모뉴스] 교보스팩20호 공모청약 1일차 청약경쟁률 function search_corp() document.getElementById('tap7')",
                "market": "미상",
                "stage": "청약예정",
                "source": "38",
            },
        ]
    )
    out = clean_issue_frame(raw)
    assert len(out) == 1
    assert out.iloc[0]["name"] == "테스트바이오"


def best_table_scoring_fixture_test() -> None:
    from src.services.ipo_scrapers import _read_best_table

    html = """
    <html><body>
      <table>
        <tr><th>메뉴</th><th>링크</th><th>IPO현황</th><th>정보실</th></tr>
        <tr><td>오늘의공시</td><td>회사별검색</td><td>공모기업</td><td>신규상장기업</td></tr>
      </table>
      <table>
        <tr><th>회사명</th><th>청약일정</th><th>상장예정일</th><th>상장주선인</th></tr>
        <tr><td>테스트공모</td><td>2026-04-01 ~ 2026-04-02</td><td>2026-04-10</td><td>한국투자증권</td></tr>
      </table>
    </body></html>
    """
    out = _read_best_table(html, ["회사명", "청약일정", "상장예정일"])
    assert not out.empty
    assert "회사명" in [str(c) for c in out.columns]


def workspace_autodetect_fixture_test() -> None:
    from src.services.unified_lab_bridge import UnifiedLabBridgeService

    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        project = root / "ipo_lockup_unified_lab" / "workspace"
        (project / "unlock_out").mkdir(parents=True)
        (project / "signal_out").mkdir(parents=True)
        (project / "turnover_backtest_out").mkdir(parents=True)
        (project / "unlock_out" / "unlock_events_backtest_input.csv").write_text("name,unlock_date,term\n테스트,2026-04-10,1M\n", encoding="utf-8")
        (project / "signal_out" / "turnover_signals.csv").write_text("name,entry_ts\n테스트,2026-04-10 10:00:00\n", encoding="utf-8")
        service = UnifiedLabBridgeService(root / "data")
        detected = service.auto_detect_workspace(root)
        assert detected is not None
        assert detected.name == "workspace"



def workspace_dataset_only_autodetect_fixture_test() -> None:
    from src.services.unified_lab_bridge import UnifiedLabBridgeService

    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        project = root / "integrated_lab" / "ipo_lockup_unified_lab" / "workspace" / "dataset_out"
        project.mkdir(parents=True)
        (project / "synthetic_ipo_events.csv").write_text("symbol,name,listing_date,unlock_date,term,ipo_price,market\n123456,테스트,2026-03-10,2026-04-10,1M,10000,코스닥\n", encoding="utf-8")
        service = UnifiedLabBridgeService(root / "data")
        detected = service.auto_detect_workspace(root)
        assert detected is not None
        assert detected.name == "workspace"


def strategy_overlay_fixture_test() -> None:
    hub = IPODataHub(Path("data"))
    external = pd.DataFrame([
        {
            "symbol": "123456",
            "name": "테스트오버레이",
            "name_key": "테스트오버레이",
            "listing_date": "2026-03-10",
            "unlock_date": "2026-04-10",
            "term": "1M",
            "ipo_price": 12000,
            "market": "코스닥",
            "lead_manager": "한국투자증권",
            "listed_shares": 10000000,
        }
    ])
    overlay = hub._issue_overlay_from_external_unlocks(external)
    assert not overlay.empty
    assert overlay.loc[0, "symbol"] == "123456"
    assert overlay.loc[0, "listing_date"] == pd.Timestamp("2026-03-10")
    assert overlay.loc[0, "offer_price"] == 12000

def issue_recency_sort_fixture_test() -> None:
    from src.utils import issue_recency_sort

    raw = pd.DataFrame(
        [
            {"name": "오래된상장사", "listing_date": "2023-04-01", "source": "KIND-corpList"},
            {"name": "다가올청약", "subscription_start": "2026-05-11", "source": "38", "underwriters": "미래에셋증권"},
            {"name": "최근상장", "listing_date": "2026-03-20", "source": "local-kind"},
        ]
    )
    out = issue_recency_sort(raw, today=pd.Timestamp("2026-03-29"))
    assert out.iloc[0]["name"] == "다가올청약"
    assert out.iloc[1]["name"] == "최근상장"

def issue_recency_sort_preserves_scores_fixture_test() -> None:
    from src.services.scoring import IPOScorer
    from src.utils import issue_recency_sort

    raw = pd.DataFrame(
        [
            {"name": "청약대상", "subscription_start": "2026-05-11", "source": "38", "underwriters": "미래에셋증권"},
            {"name": "상장대상", "listing_date": "2026-03-20", "source": "local-kind"},
        ]
    )
    scored = IPOScorer().add_scores(raw)
    out = issue_recency_sort(scored, today=pd.Timestamp("2026-03-29"))
    for col in ["subscription_score", "listing_quality_score", "unlock_pressure_score", "overall_score"]:
        assert col in out.columns, f"missing after sort: {col}"



def thirtyeight_schedule_detail_enrichment_fixture_test() -> None:
    from types import SimpleNamespace
    import src.services.ipo_scrapers as scrapers

    raw = pd.DataFrame(
        [
            {
                "종목명": "테스트바이오",
                "공모일정": "2026.03.18 ~ 2026.03.19",
                "희망공모가": "13,000 ~ 15,000원",
                "확정공모가": "15,000원",
                "기관경쟁률": "1,234.56:1",
                "청약경쟁률": "321.00:1",
                "주간사": "한국투자증권",
                "상장일": "2026.03.20",
                "detail_url": "https://example.com/detail/1",
            }
        ]
    )
    html = """
    <html><body>
    <table>
      <tr><th>종목명</th><td>테스트바이오</td><th>시장구분</th><td>코스닥</td></tr>
      <tr><th>종목코드</th><td>123456</td><th>업종</th><td>의약품</td></tr>
      <tr><th>총공모주식수</th><td>2,635,000 주</td><th>상장공모</th><td>신주모집 : 2,135,000 주 (81.0%) / 구주매출 : 500,000 주 (19.0%)</td></tr>
      <tr><th>상장후총주식수</th><td>10,000,000 주</td><th>의무보유확약</th><td>12.34%</td></tr>
    </table>
    </body></html>
    """
    original_http_get = scrapers._http_get
    scrapers._http_get = lambda url, timeout=10: SimpleNamespace(text=html)
    try:
        out = scrapers.standardize_38_schedule_table(raw, today=pd.Timestamp("2026-03-01"), fetch_details=True)
    finally:
        scrapers._http_get = original_http_get
    assert not out.empty
    assert out.loc[0, "market"] == "코스닥"
    assert out.loc[0, "symbol"] == "123456"
    assert out.loc[0, "sector"] == "의약품"
    assert out.loc[0, "total_offer_shares"] == 2635000
    assert out.loc[0, "new_shares"] == 2135000
    assert out.loc[0, "selling_shares"] == 500000
    assert round(float(out.loc[0, "secondary_sale_ratio"]), 2) == 18.98
    assert out.loc[0, "post_listing_total_shares"] == 10000000
    assert out.loc[0, "lockup_commitment_ratio"] == 12.34



def packaged_kind_seed_fixture_test() -> None:
    from src.services.ipo_repository import IPORepository
    from src.services.ipo_scrapers import load_kind_export_from_path

    repo = IPORepository(Path("data"))
    detected = repo.auto_detect_local_kind_export(include_home_dirs=False)
    assert detected is not None
    assert detected.name == "kind_ipo_master.csv"
    out = load_kind_export_from_path(detected)
    target = out[out["name_key"] == "지아이이노베이션"]
    assert not target.empty
    row = target.iloc[0]
    assert row["symbol"] == "358570"
    assert row["market"] == "코스닥"
    assert "증권" in str(row["underwriters"])


def thirtyeight_companyinfo_text_fallback_fixture_test() -> None:
    from src.services.ipo_scrapers import parse_38_detail_html

    html = """
    <html><head><title>마키나락스 기업개요 - 비상장주식, 장외주식시장 NO.1</title></head>
    <body>
      <div>마키나락스 코스닥 상장예비심사 승인종목</div>
      <div>업종 응용 소프트웨어 개발 및 공급업</div>
      <div>희망공모가 12,500~15,000</div>
      <div>공모주식수 2,635,000주</div>
      <div>주간사 미래에셋증권,현대차증권</div>
    </body></html>
    """
    out = parse_38_detail_html(html, url="https://www.38.co.kr/html/forum/board/?code=477850&o=cinfo")
    assert out.get("market") == "코스닥"
    assert out.get("sector") == "응용 소프트웨어 개발 및 공급업"
    assert out.get("underwriters") == "미래에셋증권,현대차증권"
    assert out.get("total_offer_shares") == 2635000


def runtime_plan_fixture_test() -> None:
    runtime = ExecutionRuntimeService(Path("data"))
    board = pd.DataFrame(
        [
            {
                "strategy_version": "2.0",
                "decision": "우선검토",
                "decision_rank": 1,
                "priority_tier": "A",
                "symbol": "123456",
                "name": "테스트IPO",
                "market": "코스닥",
                "term": "3M",
                "unlock_date": "2026-04-10",
                "planned_check_date": "2026-04-09",
                "planned_entry_date": "2026-04-10",
                "planned_exit_date": "2026-05-15",
                "entry_rule": "해제일 종가",
                "suggested_weight_pct_of_base": 100,
                "current_price": 25000,
                "combined_score": 55.0,
                "conviction_score": 80.0,
                "bridge_status": "신호발생",
                "minute_job_status": "done",
                "turnover_signal_hits": 2,
                "turnover_first_signal_ts": "2026-04-10 10:15:00",
                "turnover_first_entry_price": 24800,
                "turnover_best_multiple": 2.0,
                "turnover_best_price_filter": "reclaim_open_or_vwap",
                "turnover_best_ratio": 2.03,
                "rationale": "fixture",
            }
        ]
    )
    bundle = runtime.build_runtime_plan(board, total_budget_krw=5_000_000, today=pd.Timestamp("2026-04-08"))
    assert not bundle.plan.empty
    assert bundle.summary["selected"] == 1
    assert bundle.plan.loc[0, "planned_qty"] > 0
    dry_run = runtime.dry_run(bundle.plan, today=pd.Timestamp("2026-04-08"))
    assert dry_run.loc[0, "dry_run_status"] == "WATCH"



def main() -> None:
    root = Path(__file__).resolve().parents[1]
    data_dir = root / "data"

    repo = IPORepository(data_dir)
    sample = repo.load_sample_issues()
    assert not sample.empty, "sample issues empty"

    hub = IPODataHub(data_dir)
    bundle = hub.load_bundle(prefer_live=False, use_cache=False, allow_sample_fallback=True, allow_packaged_sample_paths=True)
    assert not bundle.issues.empty, "bundle issues empty"
    assert not bundle.all_unlocks.empty, "bundle unlocks empty"

    alerts = AlertEngine().generate(bundle.issues, bundle.all_unlocks, sample["listing_date"].dropna().min().normalize(), AlertSettings())
    assert alerts is not None

    bridge = StrategyBridge(data_dir)
    term_edge = bridge.term_edge_table("2.0")
    assert not term_edge.empty, "term edge empty"

    lockup_service = LockupStrategyService(data_dir)
    lockup_board = lockup_service.build_strategy_board(bundle.all_unlocks, sample, pd.Timestamp("2026-03-26"), "2.0", horizon_days=90)
    assert not lockup_board.empty, "lockup strategy board empty"
    assert "decision" in lockup_board.columns, "lockup decision missing"

    bt_repo = BacktestRepository(data_dir)
    assert not bt_repo.load_summary("2.0").empty, "backtest summary empty"
    assert not bt_repo.load_skip_summary("2.0").empty, "skip summary empty"

    unified_service = UnifiedLabBridgeService(data_dir)
    unified = unified_service.load_bundle(data_dir / "sample_unified_lab_workspace")
    assert unified.paths.workspace is not None, "unified workspace missing"
    assert not unified.unlocks.empty, "unified unlocks empty"
    assert not unified.signals.empty, "unified signals empty"
    bridge_board = unified_service.enrich_strategy_board(lockup_board, unified, today=pd.Timestamp("2026-03-26"))
    assert "bridge_status" in bridge_board.columns, "bridge status missing"
    assert bridge_board["bridge_status"].astype(str).isin(["수집대기", "큐미설정", "신호발생", "데이터적재", "수집중", "신호없음", "미연결"]).any(), "unexpected bridge status"
    execution_bridge = unified_service.build_execution_bridge_export(bridge_board, unified, today=pd.Timestamp("2026-03-26"), min_decision_rank=4)
    assert execution_bridge is not None

    market = MarketService(data_dir)
    snap, source = market.get_market_snapshot(prefer_live=False)
    assert not snap.empty

    parser_fixture_test()
    env_loader_fixture_test()
    empty_score_frame_fixture_test()
    date_parser_fixture_test()
    safe_bool_fixture_test()
    safe_float_fixture_test()
    kind_public_offering_fixture_test()
    kind_pubprice_fixture_test()
    thirtyeight_detail_fixture_test()
    thirtyeight_menu_blob_fixture_test()
    clean_issue_frame_fixture_test()
    best_table_scoring_fixture_test()
    workspace_autodetect_fixture_test()
    workspace_dataset_only_autodetect_fixture_test()
    nullable_overlay_fixture_test()
    strategy_overlay_fixture_test()
    runtime_plan_fixture_test()
    thirtyeight_duplicate_columns_fixture_test()
    issue_recency_sort_fixture_test()
    issue_recency_sort_preserves_scores_fixture_test()
    thirtyeight_schedule_detail_enrichment_fixture_test()
    packaged_kind_seed_fixture_test()
    print("SMOKE TEST OK")


if __name__ == "__main__":
    main()