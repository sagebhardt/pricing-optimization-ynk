"""Category interaction features.

Creates interaction terms between product category and other dimensions
so the model can learn category-specific patterns (e.g., "Footwear in
decline behaves differently than Apparel in decline") without needing
separate models per category.
"""

import numpy as np
import pandas as pd


def add_category_interactions(features: pd.DataFrame) -> pd.DataFrame:
    """Add category × dimension interaction features.

    New columns (all integer-encoded categoricals):
    - cat_x_lifecycle: category × lifecycle stage
    - cat_x_season: category × fall/winter flag
    - cat_x_velocity_bucket: category × velocity tercile (low/med/high)
    - cat_x_age_bucket: category × product age bucket (new/mid/old)
    """
    if "primera_jerarquia" not in features.columns:
        return features

    cat = features["primera_jerarquia"].fillna("Unknown").astype(str)

    # Category × lifecycle stage
    if "lifecycle_stage_code" in features.columns:
        lc = features["lifecycle_stage_code"].fillna(-1).astype(int).astype(str)
        features["cat_x_lifecycle"] = (cat + "_" + lc).astype("category").cat.codes

    # Category × season
    if "is_fall_winter" in features.columns:
        fw = features["is_fall_winter"].fillna(0).astype(int).astype(str)
        features["cat_x_season"] = (cat + "_" + fw).astype("category").cat.codes

    # Category × velocity bucket (terciles: low/med/high)
    if "velocity_4w" in features.columns:
        vel = features["velocity_4w"].fillna(0)
        q33 = vel.quantile(0.33)
        q66 = vel.quantile(0.66)
        vel_bucket = np.where(vel <= q33, "low", np.where(vel <= q66, "med", "high"))
        features["cat_x_velocity"] = (cat + "_" + vel_bucket).astype("category").cat.codes

    # Category × product age bucket (new <8w, mid 8-26w, old >26w)
    if "product_age_weeks" in features.columns:
        age = features["product_age_weeks"].fillna(0)
        age_bucket = np.where(age < 8, "new", np.where(age <= 26, "mid", "old"))
        features["cat_x_age"] = (cat + "_" + age_bucket).astype("category").cat.codes

    return features
