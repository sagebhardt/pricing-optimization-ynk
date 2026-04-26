"""
Pure-math pricing primitives and profit simulation.

Shared between the per-store path (weekly_pricing_brand.py) and the
per-channel path (channel_pricing_brand.py) so both grains use identical math.
Depends on stdlib + numpy only; safe to import from anywhere.
"""

from typing import Iterable, Optional

# Discount ladder
DISCOUNT_STEPS = [0.0, 0.15, 0.20, 0.30, 0.40]

# Minimum acceptable margin (%) after stripping IVA — step back if breached
MIN_MARGIN_PCT = 15

# IVA rate (Chile)
IVA = 0.19

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
    """Snap price to the nearest cognitive price anchor."""
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
    Estimate weekly units at a DEEPER discount (markdowns only).

    Intentionally markdown-only: returns current_vel unchanged when
    new_disc <= current_disc. The per-store PASS 1 (price increase) path
    in weekly_pricing_brand.py handles raises with its own elasticity
    block; do NOT generalise this function to handle raises or the
    per-store path will silently change behavior. Use
    expected_velocity_bidirectional for sims that must handle raises.
    """
    if current_vel <= 0:
        return 0.5
    if new_disc <= current_disc:
        return current_vel
    price_change_pct = (new_disc - current_disc) / max(1 - current_disc, 0.01)
    if elasticity is not None and elasticity < -0.3:
        volume_change = -price_change_pct * elasticity
        return max(current_vel * (1 + volume_change), 0.5)
    lift_by_step = {0.15: 1.8, 0.20: 2.2, 0.30: 3.0, 0.40: 4.0}
    lift = lift_by_step.get(new_disc, 1 + price_change_pct * 5)
    return max(current_vel * lift, 0.5)


def expected_velocity_bidirectional(current_vel, current_disc, new_disc, elasticity):
    """
    Like compute_expected_velocity but handles price INCREASES too (new_disc < current_disc).
    Used by the channel-level profit sim, which evaluates raises and markdowns symmetrically.
    """
    if current_vel <= 0:
        return 0.5
    if new_disc == current_disc:
        return current_vel
    # When new_disc > current_disc the discount is deepening → price dropped.
    # When new_disc < current_disc the discount is shallowing → price rose.
    # Relative price change (positive = price rose): (current_disc - new_disc) / (1 - current_disc)
    price_change_pct = (current_disc - new_disc) / max(1 - current_disc, 0.01)
    if elasticity is not None and elasticity < -0.3:
        # vol_change_pct = elasticity * price_change_pct (elasticity negative)
        vol_change = elasticity * price_change_pct
        return max(current_vel * (1 + vol_change), 0.3)
    if new_disc > current_disc:
        # Fallback: empirical lift table for deepening discounts
        lift_by_step = {0.15: 1.8, 0.20: 2.2, 0.30: 3.0, 0.40: 4.0}
        lift = lift_by_step.get(new_disc, 1 + abs(price_change_pct) * 5)
        return max(current_vel * lift, 0.5)
    # Shallowing: no elasticity → assume 25% vol drop per 15pp de-discount, capped
    pp_up = current_disc - new_disc
    vol_loss_frac = min(0.25 * (pp_up / 0.15), 0.6)
    return max(current_vel * (1 - vol_loss_frac), 0.3)


def _margin_pct(price_gross, cost):
    """Margin % at the given price, stripping IVA. None if no cost."""
    if cost is None or cost <= 0:
        return None
    net = price_gross / (1 + IVA)
    if net <= 0:
        return 0.0
    return (net - cost) / net * 100.0


def find_profit_maximizing_step(
    list_price: float,
    current_price: float,
    current_discount: float,
    velocity: float,
    unit_cost: Optional[float],
    elasticity: Optional[float],
    discount_steps: Optional[Iterable[float]] = None,
    min_margin_pct: float = MIN_MARGIN_PCT,
    min_price_delta: float = 2000.0,
    allow_increase: bool = True,
) -> Optional[dict]:
    """
    Pick the discount step that maximizes expected weekly gross profit
    (or weekly revenue if unit_cost is None), applied uniformly at one price.

    Iterates the candidate ladder, filters steps that would breach the cost
    floor (net price < cost) or margin floor (margin_pct < min_margin_pct),
    and selects the feasible step with max weekly_profit.

    Return values:
    - None: no feasible step passed the cost / margin filters. The caller
      may want a velocity-weighted fallback (missing cost data path).
    - dict with action_type == "hold": at least one step was feasible and
      the best one is no material change from the current price. Callers
      MUST skip these — do not contradict the sim's "hold" verdict.
    - dict with action_type in ("decrease","increase"): the emitted action.

    Args:
        list_price: IVA-inclusive full price
        current_price: IVA-inclusive current price
        current_discount: current discount fraction (0.0-1.0)
        velocity: current weekly units at current_price (sum over the grain)
        unit_cost: IVA-exclusive unit cost in CLP; None if unavailable
        elasticity: price elasticity coefficient (negative); None triggers fallback
        discount_steps: candidate discount ladder (default DISCOUNT_STEPS)
        min_margin_pct: margin floor; steps below are filtered
        min_price_delta: below this |price delta| the action is "no material change"
        allow_increase: if False, only steps >= current_discount are considered
    """
    if list_price is None or list_price <= 0 or velocity is None:
        return None
    steps = list(discount_steps) if discount_steps is not None else list(DISCOUNT_STEPS)

    if not allow_increase:
        steps = [s for s in steps if s >= current_discount]

    current_price_rounded = snap_to_price_anchor(current_price, direction="nearest") if current_price else 0

    candidates = []
    constraint_hit = None
    for step in steps:
        raw = list_price * (1 - step)
        price = snap_to_price_anchor(raw, direction="nearest")
        if price <= 0:
            continue
        net = price / (1 + IVA)
        # Cost floor: net price below cost is never allowed
        if unit_cost is not None and unit_cost > 0 and net < unit_cost:
            constraint_hit = constraint_hit or "cost"
            continue
        # Margin floor (only when cost known)
        if unit_cost is not None and unit_cost > 0:
            margin_pct_at_step = (net - unit_cost) / net * 100.0 if net > 0 else 0.0
            if margin_pct_at_step < min_margin_pct:
                constraint_hit = constraint_hit or "margin"
                continue

        exp_vel = expected_velocity_bidirectional(velocity, current_discount, step, elasticity)
        weekly_revenue = exp_vel * price
        if unit_cost is not None and unit_cost > 0:
            weekly_profit = (net - unit_cost) * exp_vel
            margin_unit = net - unit_cost
            margin_pct_at_step = (net - unit_cost) / net * 100.0 if net > 0 else 0.0
        else:
            # No cost → optimize revenue as the proxy for profit
            weekly_profit = weekly_revenue
            margin_unit = None
            margin_pct_at_step = None

        candidates.append({
            "step": step,
            "price": int(price),
            "expected_velocity": exp_vel,
            "weekly_profit": weekly_profit,
            "weekly_revenue": weekly_revenue,
            "margin_pct": round(margin_pct_at_step, 1) if margin_pct_at_step is not None else None,
            "margin_unit": margin_unit,
        })

    if not candidates:
        return None

    # Max profit; tie-break by shallower discount (smaller step) then higher velocity
    candidates.sort(key=lambda c: (-c["weekly_profit"], c["step"], -c["expected_velocity"]))
    best = candidates[0]

    # Classify action type from PRICE DIRECTION (chosen vs current), not from
    # step direction. At channel grain the velocity-weighted current_price and
    # the step-derived chosen_price use different bases — a "deeper step"
    # (higher discount %) does NOT always produce a lower price than the
    # weighted-average current. Deriving from price direction guarantees the
    # action_type matches what the BM sees: price up = increase, down = decrease.
    current_step_snapped = snap_to_discount_step(current_discount)
    price_diff = best["price"] - current_price_rounded
    if abs(price_diff) < min_price_delta and best["step"] == current_step_snapped:
        action_type = "hold"
    elif price_diff > 0:
        action_type = "increase"
    elif price_diff < 0:
        action_type = "decrease"
    else:
        # Same price but different step (anchor-snap tie) — call it hold to
        # avoid emitting a no-op recommendation.
        action_type = "hold"

    return {
        "chosen_step": best["step"],
        "chosen_price": best["price"],
        "expected_velocity": best["expected_velocity"],
        "weekly_profit": best["weekly_profit"],
        "weekly_revenue": best["weekly_revenue"],
        "margin_pct": best["margin_pct"],
        "margin_unit": best["margin_unit"],
        "constraint_hit": constraint_hit if len(candidates) < len(steps) else None,
        "feasible_steps": [c["step"] for c in candidates],
        "action_type": action_type,
    }
