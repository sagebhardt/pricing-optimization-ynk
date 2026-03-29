"""
Pure-math pricing functions shared between the pipeline and API.

Extracted from src/models/weekly_pricing_brand.py so the slim API image
(no ML deps) can use them for manual price impact estimation.

Dependencies: only stdlib + numpy (available via pandas in API image).
"""

# Discount ladder (same across brands)
DISCOUNT_STEPS = [0.0, 0.15, 0.20, 0.30, 0.40]

# Cognitive price anchors — each sits just below a round psychological threshold.
PRICE_ANCHORS = [
    990, 1990, 2990, 3990, 4990, 5990, 6990, 7990, 8990, 9990,
    12990, 14990, 16990, 19990,
    24990, 29990, 34990, 39990, 44990, 49990,
    54990, 59990, 69990, 79990, 89990, 99990,
    109990, 119990, 129990, 139990, 149990, 169990, 199990,
    249990, 299990, 399990, 499990, 599990, 799990, 999990,
]


def snap_to_price_anchor(price, direction="down"):
    """
    Snap price to the nearest cognitive price anchor.

    direction:
      "down"    - largest anchor <= price (use for markdowns)
      "up"      - smallest anchor >= price (use for price increases)
      "nearest" - closest anchor (use for display / current price)
    """
    if price <= 0:
        return 0

    if price > PRICE_ANCHORS[-1]:
        step = 10000
        if direction == "down":
            return int(price // step) * step - 10
        elif direction == "up":
            return (int(price // step) + 1) * step - 10
        else:
            return int(round(price / step) * step - 10)

    if direction == "down":
        valid = [a for a in PRICE_ANCHORS if a <= price]
        return valid[-1] if valid else PRICE_ANCHORS[0]
    elif direction == "up":
        valid = [a for a in PRICE_ANCHORS if a >= price]
        return valid[0] if valid else PRICE_ANCHORS[-1]
    else:
        return min(PRICE_ANCHORS, key=lambda a: abs(a - price))


def snap_to_discount_step(discount_pct):
    """Snap a continuous discount % to the actual discount ladder."""
    if discount_pct < 0.07:
        return 0.0
    best = min(DISCOUNT_STEPS, key=lambda s: abs(s - discount_pct))
    if best == 0.0 and discount_pct >= 0.07:
        return 0.15
    return best


def compute_expected_velocity(current_vel, current_disc, new_disc, elasticity):
    """
    Estimate weekly units at new price point.
    Uses elasticity where available, falls back to historical lift patterns.
    """
    if current_vel <= 0:
        return 0.5

    if new_disc <= current_disc:
        return current_vel

    price_change_pct = (new_disc - current_disc) / max(1 - current_disc, 0.01)

    if elasticity is not None and elasticity < -0.3:
        volume_change = -price_change_pct * elasticity
        return max(current_vel * (1 + volume_change), 0.5)
    else:
        lift_by_step = {
            0.15: 1.8,
            0.20: 2.2,
            0.30: 3.0,
            0.40: 4.0,
        }
        lift = lift_by_step.get(new_disc, 1 + price_change_pct * 5)
        return max(current_vel * lift, 0.5)


def estimate_manual_price_impact(action: dict, manual_price: int, elasticity=None) -> dict:
    """
    Recalculate expected impact for a manually-set price.

    Args:
        action: the pricing action dict (from CSV row)
        manual_price: the BM's chosen price (will be snapped to anchor)
        elasticity: elasticity coefficient for this SKU (optional)

    Returns dict with: snapped_price, velocity, weekly_revenue, margin_pct, margin_delta, warning
    """
    snapped = snap_to_price_anchor(manual_price, direction="nearest")
    list_price = int(action.get("current_list_price", 0))
    current_price = int(action.get("current_price", 0))
    current_vel = float(action.get("current_velocity", 0))
    unit_cost = action.get("unit_cost")

    if list_price <= 0:
        return {"snapped_price": snapped, "warning": "no_list_price"}

    # Compute discount rates
    current_disc = 1 - (current_price / list_price) if current_price > 0 else 0
    new_disc = 1 - (snapped / list_price) if snapped > 0 else 0
    new_disc = max(0, min(new_disc, 0.5))  # cap at 50%

    # Velocity estimate
    velocity = compute_expected_velocity(current_vel, current_disc, new_disc, elasticity)
    weekly_revenue = velocity * snapped

    # Margin calculation
    warning = None
    margin_pct = None
    margin_delta = None

    if unit_cost and float(unit_cost) > 0:
        cost = float(unit_cost)
        rec_neto = snapped / 1.19
        current_neto = current_price / 1.19
        margin_unit = rec_neto - cost
        margin_pct = round(margin_unit / rec_neto * 100, 1) if rec_neto > 0 else 0

        current_margin_weekly = (current_neto - cost) * current_vel
        expected_margin_weekly = margin_unit * velocity
        margin_delta = int(expected_margin_weekly - current_margin_weekly)

        if margin_pct < 0:
            warning = "below_cost"
        elif margin_pct < 15:
            warning = "below_margin_floor"
        elif margin_pct < 20:
            warning = "thin_margin"
    else:
        warning = "no_cost_data"

    return {
        "snapped_price": snapped,
        "velocity": round(velocity, 1),
        "weekly_revenue": int(weekly_revenue),
        "margin_pct": margin_pct,
        "margin_delta": margin_delta,
        "warning": warning,
    }
