"""Tests for API endpoints — auth, analytics, decisions, export."""

import pytest
import os
import json

# Disable auth for testing
os.environ["GOOGLE_CLIENT_ID"] = ""

from fastapi.testclient import TestClient
from api.main import app


@pytest.fixture
def client():
    """FastAPI test client with auth disabled (dev mode)."""
    return TestClient(app)


class TestHealthCheck:
    def test_health_endpoint(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


class TestAuthConfig:
    def test_auth_config_returns_client_id(self, client):
        r = client.get("/auth/config")
        assert r.status_code == 200
        data = r.json()
        assert "client_id" in data or "require_auth" in data

    def test_auth_me_returns_dev_user(self, client):
        r = client.get("/auth/me")
        assert r.status_code == 200
        data = r.json()
        assert data["email"] == "dev@local"
        assert "admin" == data["role"]
        assert "plan" in data["permissions"]
        assert "approve" in data["permissions"]


class TestPricingActions:
    def test_no_brand_returns_empty(self, client):
        r = client.get("/pricing-actions")
        assert r.status_code == 200
        assert r.json()["items"] == []

    def test_unknown_brand_returns_empty(self, client):
        r = client.get("/pricing-actions?brand=nonexistent")
        assert r.status_code == 200
        data = r.json()
        assert data["total"] == 0


class TestAnalyticsEndpoint:
    def test_analytics_returns_four_sections(self, client):
        r = client.get("/analytics/hoka")
        assert r.status_code == 200
        data = r.json()
        assert "modelo" in data
        assert "elasticidad" in data
        assert "ciclo_de_vida" in data
        assert "impacto" in data

    def test_modelo_section_has_metrics(self, client):
        r = client.get("/analytics/hoka")
        data = r.json()
        modelo = data["modelo"]
        # These may be None if no local data, but keys must exist
        assert "classifier_auc" in modelo
        assert "regressor_r2" in modelo
        assert "regressor_mae_pp" in modelo
        assert "n_features" in modelo
        assert "classifier_shap" in modelo
        assert "regressor_shap" in modelo

    def test_ciclo_section_has_distributions(self, client):
        r = client.get("/analytics/hoka")
        data = r.json()
        ciclo = data["ciclo_de_vida"]
        assert "total_actions" in ciclo
        assert "urgency_dist" in ciclo
        assert "action_type_dist" in ciclo

    def test_impacto_section_structure(self, client):
        r = client.get("/analytics/hoka")
        data = r.json()
        impacto = data["impacto"]
        assert "by_store" in impacto
        assert "by_subcategory" in impacto
        assert "by_vendor_brand" in impacto
        assert "thin_margin_count" in impacto
        assert isinstance(impacto["thin_margin_count"], int)


class TestModelInfo:
    def test_model_info_returns_data(self, client):
        r = client.get("/model/info?brand=hoka")
        assert r.status_code == 200
        data = r.json()
        assert "classifier" in data
        assert "regressor" in data


class TestDecisions:
    def test_get_decisions_empty(self, client):
        # Use a unique week that no other test writes to
        r = client.get("/decisions?brand=hoka&week=2099-12-31")
        assert r.status_code == 200
        data = r.json()
        assert data["decisions"] == {}

    def test_post_decision(self, client):
        r = client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-01-01",
            "key": "TEST-SKU-001",
            "status": "approved",
        })
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_post_and_get_decision(self, client):
        client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-01-02",
            "key": "TEST-SKU-002",
            "status": "rejected",
        })
        r = client.get("/decisions?brand=hoka&week=2099-01-02")
        data = r.json()
        assert "TEST-SKU-002" in data["decisions"]
        assert data["decisions"]["TEST-SKU-002"]["status"] == "rejected"


class TestOverviewEndpoint:
    def test_overview_returns_brands(self, client):
        r = client.get("/analytics/overview")
        assert r.status_code == 200
        data = r.json()
        assert "brands" in data
        assert isinstance(data["brands"], list)

    def test_overview_brand_has_required_fields(self, client):
        r = client.get("/analytics/overview")
        data = r.json()
        if data["brands"]:
            b = data["brands"][0]
            for field in ["brand", "total_actions", "pending", "decided", "approved",
                          "rev_delta", "margin_delta", "classifier_auc", "regressor_r2"]:
                assert field in b, f"Missing field: {field}"


class TestEstimateImpact:
    def test_estimate_impact_404_unknown_sku(self, client):
        r = client.post("/estimate-impact", json={
            "brand": "hoka",
            "parent_sku": "NONEXISTENT",
            "store": "9999",
            "manual_price": 29990,
        })
        assert r.status_code == 404

    def test_estimate_impact_returns_snapped_price(self, client):
        """If a real action exists, we'd get a valid response. Test the error case."""
        r = client.post("/estimate-impact", json={
            "brand": "hoka",
            "parent_sku": "FAKE-SKU",
            "store": "0000",
            "manual_price": 50000,
        })
        # Should be 404 since the SKU doesn't exist in actions
        assert r.status_code == 404


class TestManualDecision:
    def test_post_manual_decision(self, client):
        r = client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-03-01",
            "key": "TEST-MANUAL-001",
            "status": "manual",
            "manual_price": 49990,
            "estimated_impact": {"velocity": 2.5, "weekly_revenue": 124975, "margin_pct": 35.2},
        })
        assert r.status_code == 200
        assert r.json()["ok"] is True

    def test_manual_decision_persists_extra_fields(self, client):
        client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-03-02",
            "key": "TEST-MANUAL-002",
            "status": "manual",
            "manual_price": 39990,
        })
        r = client.get("/decisions?brand=hoka&week=2099-03-02")
        data = r.json()
        dec = data["decisions"].get("TEST-MANUAL-002", {})
        assert dec.get("status") == "manual"
        assert dec.get("manual_price") == 39990


class TestChainDecision:
    def test_chain_decision_requires_chain_format(self, client):
        r = client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-04-01",
            "key": "BAD-KEY-NO-CHAIN",
            "status": "approved",
            "chain_scope": "all",
        })
        assert r.status_code == 400

    def test_chain_decision_with_valid_key(self, client):
        r = client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-04-02",
            "key": "HK9999-chain-all",
            "status": "approved",
            "chain_scope": "all",
        })
        assert r.status_code == 200
        assert r.json()["ok"] is True


class TestPlannerEndpoints:
    def test_planner_queue_returns_data(self, client):
        r = client.get("/decisions/planner-queue?brand=hoka")
        assert r.status_code == 200
        data = r.json()
        assert "items" in data
        assert "week" in data

    def test_planner_decide_validates_status(self, client):
        r = client.post("/decisions/plan", json={
            "brand": "hoka",
            "week": "2099-05-01",
            "keys": ["TEST-KEY"],
            "status": "invalid_status",
        })
        assert r.status_code == 400

    def test_planner_decide_with_valid_status(self, client):
        # First create a BM decision
        client.post("/decisions", json={
            "brand": "hoka",
            "week": "2099-05-02",
            "key": "TEST-PLANNER-001",
            "status": "approved",
        })
        # Then planner approves it
        r = client.post("/decisions/plan", json={
            "brand": "hoka",
            "week": "2099-05-02",
            "keys": ["TEST-PLANNER-001"],
            "status": "planner_approved",
        })
        assert r.status_code == 200
        data = r.json()
        assert data["ok"] is True
        assert data["changed"] == 1


class TestRolePermissions:
    def test_admin_has_plan_permission(self):
        from config.auth import ROLE_PERMISSIONS
        assert "plan" in ROLE_PERMISSIONS["admin"]

    def test_planner_has_plan_permission(self):
        from config.auth import ROLE_PERMISSIONS
        assert "plan" in ROLE_PERMISSIONS["planner"]

    def test_brand_manager_cannot_plan(self):
        from config.auth import ROLE_PERMISSIONS
        assert "plan" not in ROLE_PERMISSIONS["brand_manager"]

    def test_viewer_is_read_only(self):
        from config.auth import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["viewer"] == {"read"}

    def test_planner_can_export(self):
        from config.auth import ROLE_PERMISSIONS
        assert "export" in ROLE_PERMISSIONS["planner"]

    def test_all_roles_have_read(self):
        from config.auth import ROLE_PERMISSIONS
        for role, perms in ROLE_PERMISSIONS.items():
            assert "read" in perms, f"Role {role} missing read permission"
