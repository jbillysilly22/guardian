# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import sqlite3
import tensorflow as tf

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# defaults — adjust paths to match your repo layout
THIS_FILE = Path(__file__).resolve()
DB_PATH = THIS_FILE.parents[2] / "illini_fetch" / "chicago_filtered_data" / "violent_crimes.sqlite"
NORMALIZER_DIR = THIS_FILE.parents[2] / "illini_fetch" / "normalizer_saved"


def _load_enriched_from_sqlite(db_path: Path = DB_PATH) -> pd.DataFrame:
    """
    Load the enriched rows saved into the backfill DB.
    The backfill/upsert stores `raw_json` (enriched JSON) and also many flat columns.
    This function prefers the flattened JSON (raw_json) to preserve enrichment fields.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    with sqlite3.connect(str(db_path)) as conn:
        # prefer raw_json; fallback to selecting all columns where raw_json not present
        try:
            rows = pd.read_sql("SELECT raw_json FROM violent_crimes", conn)
            if "raw_json" in rows.columns:
                # raw_json column contains JSON strings; convert to records
                recs = rows["raw_json"].dropna().map(lambda s: json.loads(s)).tolist()
                df = pd.json_normalize(recs)
                return df
        except Exception:
            log.exception("Failed to load raw_json; trying to read full table")

        # fallback: read full table and return as DataFrame
        df = pd.read_sql("SELECT * FROM violent_crimes", conn)
        return df


def _choose_numeric_features(df: pd.DataFrame, candidates: Optional[Iterable[str]] = None) -> List[str]:
    """
    Pick numeric columns to normalize. If candidates provided, return intersection.
    Typical enriched numeric features: lat, lon, latitude, longitude, severity, hour, day_of_week, month, year.
    """
    default_candidates = ["severity", "lat", "lon", "latitude", "longitude", "hour", "day_of_week", "month", "year"]
    cand = list(candidates) if candidates else default_candidates
    return [c for c in cand if c in df.columns]


def build_zscore_normalization_layer(
    df: pd.DataFrame,
    feature_columns: List[str],
    *,
    fillna_with: Optional[float] = None,
) -> tf.keras.layers.Normalization:
    """
    Build and adapt a tf.keras.layers.Normalization layer (z-score) for the given feature columns.
    - df: DataFrame containing features
    - feature_columns: list of numeric column names
    Returns an adapted Normalization layer.
    """
    if not feature_columns:
        raise ValueError("feature_columns must not be empty")

    # Prepare numeric array: coerce to numeric and handle NaNs
    X = df[feature_columns].apply(pd.to_numeric, errors="coerce")
    if fillna_with is None:
        # fill NaN with column medians (robust) to avoid dropping many rows
        X = X.fillna(X.median())
    else:
        X = X.fillna(fillna_with)

    arr = X.to_numpy(dtype="float32")
    layer = tf.keras.layers.Normalization(axis=-1)
    layer.adapt(arr)
    log.info("Normalization layer adapted: mean=%s var=%s", layer.mean.numpy(), layer.variance.numpy())
    return layer


def save_normalizer(layer: tf.keras.layers.Layer, path: Path = NORMALIZER_DIR) -> Path:
    """
    Save the normalization layer as a tiny Keras model so it can be reloaded with tf.keras.models.load_model.
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    model = tf.keras.Sequential([tf.keras.Input(shape=(int(layer.mean.shape[0]),), dtype=tf.float32), layer])
    # overwrite if exists
    tf.keras.models.save_model(model, str(path), overwrite=True, include_optimizer=False)
    log.info("Saved normalizer model -> %s", path)
    return path


def load_normalizer(path: Path = NORMALIZER_DIR) -> tf.keras.Model:
    """
    Load the saved normalizer model; it returns a model that maps raw feature vector -> normalized vector.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    model = tf.keras.models.load_model(str(path))
    log.info("Loaded normalizer model from %s", path)
    return model


def normalize_dataframe(df: pd.DataFrame, feature_columns: List[str], normalizer_model: tf.keras.Model) -> pd.DataFrame:
    """
    Apply the saved normalizer model to a DataFrame and return a new DataFrame of normalized features.
    """
    X = df[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(df[feature_columns].median())
    arr = X.to_numpy(dtype="float32")
    normalized = normalizer_model.predict(arr, batch_size=8192)
    out = pd.DataFrame(normalized, columns=[f"{c}_norm" for c in feature_columns], index=df.index)
    return pd.concat([df.reset_index(drop=True), out.reset_index(drop=True)], axis=1)


def example_workflow(
    db_path: Optional[Path] = None,
    feature_columns: Optional[List[str]] = None,
    save_dir: Optional[Path] = None,
) -> Path:
    """
    Example end-to-end:
      1. Load enriched backfill data from the sqlite DB
      2. Pick numeric features
      3. Build/adapt a z-score normalization layer
      4. Save the layer as a small Keras model
      5. Return path to saved normalizer
    """
    db = Path(db_path) if db_path else DB_PATH
    save_dir = Path(save_dir) if save_dir else NORMALIZER_DIR

    df = _load_enriched_from_sqlite(db)
    features = feature_columns or _choose_numeric_features(df)
    if not features:
        raise RuntimeError("No numeric features found in the loaded DataFrame")

    normalizer = build_zscore_normalization_layer(df, features)
    model_path = save_normalizer(normalizer, save_dir)
    return model_path


if __name__ == "__main__":
    # Quick demo:
    #  - run backfill to populate DB first (chicago_data_backfill.backfill_chicago_violent_crimes)
    #  - then run this file to build and save a normalizer
    try:
        p = example_workflow()
        print("Normalizer saved to:", p)
    except Exception as e:
        log.exception("Example workflow failed: %s", e)