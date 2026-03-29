"""Shared fixtures for the YNK pricing optimization test suite."""

import sys
import os
import pytest

# Ensure project root is on path so imports work
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def sample_action():
    """A realistic pricing action row (from CSV)."""
    return {
        "parent_sku": "NI1234567890",
        "store": "2002",
        "store_name": "Bold Plaza Norte",
        "product": "AIR FORCE 1 07 WHITE",
        "category": "Footwear",
        "subcategory": "Running",
        "current_list_price": 99990,
        "current_price": 79990,
        "current_discount": "20%",
        "current_velocity": 2.5,
        "recommended_price": 69990,
        "recommended_discount": "30%",
        "expected_velocity": 3.8,
        "current_weekly_rev": 199975,
        "expected_weekly_rev": 265962,
        "rev_delta": 65987,
        "unit_cost": 35000,
        "margin_pct": 33.2,
        "margin_delta": 12400,
        "urgency": "MEDIUM",
        "reasons": "Velocity declining; Size curve breaking",
        "model_confidence": 0.92,
        "confidence_tier": "HIGH",
        "action_type": "decrease",
        "vendor_brand": "Nike",
    }


@pytest.fixture
def sample_action_increase():
    """A pricing action for a price increase."""
    return {
        "parent_sku": "AD9876543210",
        "store": "2003",
        "store_name": "Bold Iquique",
        "product": "ULTRABOOST 22 BLACK",
        "category": "Footwear",
        "subcategory": "Running",
        "current_list_price": 149990,
        "current_price": 119990,
        "current_discount": "20%",
        "current_velocity": 3.0,
        "recommended_price": 139990,
        "recommended_discount": "7%",
        "expected_velocity": 2.2,
        "current_weekly_rev": 359970,
        "expected_weekly_rev": 307978,
        "rev_delta": -51992,
        "unit_cost": 55000,
        "margin_pct": 53.2,
        "margin_delta": 8500,
        "urgency": "INCREASE",
        "reasons": "Selling well at current discount",
        "model_confidence": 0.88,
        "confidence_tier": "MEDIUM",
        "action_type": "increase",
        "vendor_brand": "Adidas",
    }


@pytest.fixture
def sample_action_no_cost():
    """Action without cost data."""
    return {
        "parent_sku": "PM5555555555",
        "store": "2005",
        "store_name": "Bold Ahumada",
        "product": "SUEDE CLASSIC BLACK",
        "category": "Footwear",
        "subcategory": "Street",
        "current_list_price": 69990,
        "current_price": 59990,
        "current_discount": "15%",
        "current_velocity": 1.5,
        "recommended_price": 49990,
        "recommended_discount": "30%",
        "expected_velocity": 2.8,
        "current_weekly_rev": 89985,
        "expected_weekly_rev": 139972,
        "rev_delta": 49987,
        "unit_cost": None,
        "margin_pct": None,
        "margin_delta": None,
        "urgency": "LOW",
        "reasons": "Markdown recommended",
        "model_confidence": 0.75,
        "confidence_tier": "LOW",
        "action_type": "decrease",
        "vendor_brand": "Puma",
    }
