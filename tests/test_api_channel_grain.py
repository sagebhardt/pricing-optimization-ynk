"""Tests for the grain=channel API path (Phase 2)."""

import os
import pytest

os.environ["GOOGLE_CLIENT_ID"] = ""

from fastapi.testclient import TestClient
from api.main import app
from api import storage


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def channel_actions_fixture(monkeypatch):
    """Stub load_pricing_actions_channel with a tiny in-memory fixture.

    Two parent SKUs; each has a BM row and one of them also has an ecomm row.
    Keys for the /decisions/plan + export flows are '{parent}-{channel}'.
    """
    fake = {
        "week": "2026-04-20",
        "grain": "channel",
        "total": 3,
        "items": [
            {
                "parent_sku": "NI1111111111",
                "channel": "bm",
                "product": "Shoe A",
                "category": "Footwear",
                "subcategory": "Running",
                "vendor_brand": "Nike",
                "n_stores": 3,
                "stores_in_channel": "2002,2003,2004",
                "has_stock_data": True,
                "channel_stock": 60,
                "current_list_price": 100000,
                "current_price": 99990,
                "current_discount": "Full price",
                "current_velocity": 9.0,
                "recommended_price": 84990,
                "recommended_discount": "15%",
                "expected_velocity": 13.5,
                "current_weekly_rev": 899910,
                "expected_weekly_rev": 1147365,
                "rev_delta": 247455,
                "unit_cost": 30000,
                "margin_pct": 42.0,
                "margin_delta": 60000,
                "urgency": "MEDIUM",
                "reasons": "Velocity declining",
                "confidence_tier": "HIGH",
                "action_type": "decrease",
                "per_store_variance_pct": 0.33,
                "mandatory_review": True,
                "aggregation_method": "profit_simulation",
            },
            {
                "parent_sku": "NI1111111111",
                "channel": "ecomm",
                "product": "Shoe A",
                "category": "Footwear",
                "subcategory": "Running",
                "vendor_brand": "Nike",
                "n_stores": 1,
                "stores_in_channel": "AB02",
                "has_stock_data": True,
                "channel_stock": 12,
                "current_list_price": 100000,
                "current_price": 99990,
                "current_discount": "Full price",
                "current_velocity": 2.0,
                "recommended_price": 84990,
                "recommended_discount": "15%",
                "expected_velocity": 3.0,
                "current_weekly_rev": 199980,
                "expected_weekly_rev": 254970,
                "rev_delta": 54990,
                "unit_cost": 30000,
                "margin_pct": 42.0,
                "margin_delta": 15000,
                "urgency": "MEDIUM",
                "reasons": "Velocity declining",
                "confidence_tier": "HIGH",
                "action_type": "decrease",
                "per_store_variance_pct": 0.0,
                "mandatory_review": False,
                "aggregation_method": "profit_simulation",
            },
            {
                "parent_sku": "NI2222222222",
                "channel": "bm",
                "product": "Shoe B",
                "category": "Footwear",
                "subcategory": "Running",
                "vendor_brand": "Nike",
                "n_stores": 3,
                "stores_in_channel": "2002,2003,2004",
                "has_stock_data": True,
                "channel_stock": 40,
                "current_list_price": 149990,
                "current_price": 119990,
                "current_discount": "20%",
                "current_velocity": 5.0,
                "recommended_price": 139990,
                "recommended_discount": "7%",
                "expected_velocity": 3.8,
                "current_weekly_rev": 599950,
                "expected_weekly_rev": 531962,
                "rev_delta": -67988,
                "unit_cost": 55000,
                "margin_pct": 53.2,
                "margin_delta": 12000,
                "urgency": "INCREASE",
                "reasons": "Margin recovery",
                "confidence_tier": "MEDIUM",
                "action_type": "increase",
                "per_store_variance_pct": 0.0,
                "mandatory_review": False,
                "aggregation_method": "profit_simulation",
            },
        ],
    }

    monkeypatch.setattr(storage, "load_pricing_actions_channel", lambda brand: fake)
    # Don't let any test leak decisions across tests
    storage.cache_clear("")
    return fake


class TestPricingActionsChannelGrain:
    def test_grain_channel_returns_channel_rows(self, client, channel_actions_fixture):
        r = client.get("/pricing-actions?brand=bold&grain=channel")
        assert r.status_code == 200
        data = r.json()
        assert data["grain"] == "channel"
        assert data["total"] == 3
        assert all("channel" in item for item in data["items"])

    def test_grain_store_is_default(self, client):
        # No grain passed — should hit store loader, returning the empty-store result
        r = client.get("/pricing-actions?brand=nonexistent")
        assert r.status_code == 200
        assert r.json().get("grain") == "store"

    def test_invalid_grain_rejected(self, client):
        r = client.get("/pricing-actions?brand=bold&grain=garbage")
        assert r.status_code == 400

    def test_brand_access_enforced(self, client, monkeypatch):
        # Simulate a user whose brands list excludes 'bold'
        from api import main
        original_get_user = main._get_user
        def fake_user(request):
            return {
                "email": "mgr@ynk.cl", "name": "Mgr",
                "role": "brand_manager",
                "permissions": ["approve", "read"],
                "brands": ["hoka"],  # NOT bold
            }
        monkeypatch.setattr(main, "_get_user", fake_user)

        r = client.get("/pricing-actions?brand=bold&grain=channel")
        assert r.status_code == 403, f"Expected 403 brand-access denial, got {r.status_code}"

        monkeypatch.setattr(main, "_get_user", original_get_user)


class TestChannelStatsEndpoint:
    def test_returns_stats_shape(self, client, monkeypatch):
        fake = {
            "summary": {
                "brand": "BOLD",
                "week": "2026-04-20",
                "n_channel_actions_written": 3,
                "n_mandatory_review": 1,
                "gap_pct_mean": 2.1,
                "gap_pct_p95": 8.4,
            },
            "per_parent": [],
        }
        monkeypatch.setattr(storage, "load_channel_aggregation_stats", lambda b: fake)
        r = client.get("/channel-stats/bold")
        assert r.status_code == 200
        data = r.json()
        assert data["summary"]["n_channel_actions_written"] == 3


class TestChannelDecisionsGetPost:
    def test_post_and_get_channel_decision(self, client, monkeypatch, tmp_path):
        # Route decisions_channel to a tmp dir via the local fallback path
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        storage.cache_clear("")

        payload = {
            "brand": "bold",
            "week": "2026-04-20",
            "key": "NI1111111111-bm",
            "status": "bm_approved",
            "grain": "channel",
        }
        r = client.post("/decisions", json=payload)
        assert r.status_code == 200, r.text
        assert r.json()["grain"] == "channel"

        # The per-store decisions file must NOT have this key
        r_store = client.get("/decisions?brand=bold&week=2026-04-20&grain=store")
        assert "NI1111111111-bm" not in r_store.json().get("decisions", {})

        # The channel decisions file must have it
        r_ch = client.get("/decisions?brand=bold&week=2026-04-20&grain=channel")
        assert r_ch.status_code == 200
        decisions = r_ch.json().get("decisions", {})
        assert "NI1111111111-bm" in decisions
        assert decisions["NI1111111111-bm"]["status"] == "bm_approved"

    def test_channel_rejects_chain_scope(self, client, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        payload = {
            "brand": "bold", "week": "2026-04-20",
            "key": "NI1111111111-chain-all",
            "status": "bm_approved",
            "chain_scope": "all",
            "grain": "channel",
        }
        r = client.post("/decisions", json=payload)
        assert r.status_code == 400


class TestPlannerQueueChannelGrain:
    def test_queue_keys_by_channel(self, client, channel_actions_fixture, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        storage.cache_clear("")

        # BM decides on one channel row
        r = client.post("/decisions", json={
            "brand": "bold", "week": "2026-04-20",
            "key": "NI1111111111-bm", "status": "bm_approved",
            "grain": "channel",
        })
        assert r.status_code == 200, r.text

        # Planner queue should surface it
        r = client.get("/decisions/planner-queue?brand=bold&grain=channel")
        assert r.status_code == 200
        data = r.json()
        assert data["grain"] == "channel"
        keys = [q["decision_key"] for q in data["items"]]
        assert "NI1111111111-bm" in keys


class TestPlannerDecideChannelGrain:
    def test_planner_flips_channel_decision(self, client, channel_actions_fixture, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        storage.cache_clear("")

        client.post("/decisions", json={
            "brand": "bold", "week": "2026-04-20",
            "key": "NI1111111111-bm", "status": "bm_approved",
            "grain": "channel",
        })

        r = client.post("/decisions/plan", json={
            "brand": "bold", "week": "2026-04-20",
            "keys": ["NI1111111111-bm"],
            "status": "planner_approved",
            "grain": "channel",
        })
        assert r.status_code == 200, r.text
        assert r.json()["changed"] == 1

        # Verify status flipped
        r_ch = client.get("/decisions?brand=bold&week=2026-04-20&grain=channel")
        dec = r_ch.json()["decisions"]["NI1111111111-bm"]
        assert dec["status"] == "planner_approved"
        assert dec["bm_status"] == "bm_approved"


class TestExportChannelGrain:
    def test_export_channel_only_exports_approved(self, client, channel_actions_fixture, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        # Stub save_export so we don't touch real GCS/filesystem
        saved = {}
        def fake_save(brand, filename, content):
            saved["brand"] = brand
            saved["filename"] = filename
            saved["size"] = len(content)
        monkeypatch.setattr(storage, "save_export", fake_save)
        storage.cache_clear("")

        # Approve two channel rows
        for key in ("NI1111111111-bm", "NI2222222222-bm"):
            client.post("/decisions", json={
                "brand": "bold", "week": "2026-04-20",
                "key": key, "status": "approved",
                "grain": "channel",
            })

        r = client.get("/export/price-changes?brand=bold&grain=channel&format=excel")
        assert r.status_code == 200
        assert saved.get("filename", "").endswith(".xlsx")
        assert "_channel_" in saved["filename"]

    def test_export_channel_no_approved_returns_400(self, client, channel_actions_fixture, monkeypatch, tmp_path):
        monkeypatch.setattr(storage, "_BASE_DIR", tmp_path)
        storage.cache_clear("")
        r = client.get("/export/price-changes?brand=bold&grain=channel&format=text")
        assert r.status_code == 400


class TestEstimateImpactChannelGrain:
    def test_channel_requires_channel_field(self, client, channel_actions_fixture):
        r = client.post("/estimate-impact", json={
            "brand": "bold", "parent_sku": "NI1111111111",
            "store": "2002",  # ignored when grain=channel
            "manual_price": 79990,
            "grain": "channel",
        })
        assert r.status_code == 400

    def test_channel_finds_row_by_channel(self, client, channel_actions_fixture):
        r = client.post("/estimate-impact", json={
            "brand": "bold", "parent_sku": "NI1111111111",
            "channel": "bm",
            "manual_price": 79990,
            "grain": "channel",
        })
        assert r.status_code == 200
        data = r.json()
        # snapped_price should be one of the anchor values
        assert data.get("snapped_price") in (79990, 74990, 84990)

    def test_channel_404_unknown_channel(self, client, channel_actions_fixture):
        r = client.post("/estimate-impact", json={
            "brand": "bold", "parent_sku": "NI2222222222",
            "channel": "ecomm",  # not present for this parent in fixture
            "manual_price": 119990,
            "grain": "channel",
        })
        assert r.status_code == 404
