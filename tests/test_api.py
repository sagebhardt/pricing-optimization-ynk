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
