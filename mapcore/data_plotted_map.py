# mapcore/data_plotted_map.py
from __future__ import annotations

import sys
from pathlib import Path
import sqlite3
from typing import List

import folium
from folium.plugins import HeatMap


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_sqlite import DB_PATH


USA_SW = [24.0, -130.0]
USA_NE = [50.0, -67.79]


CHICAGO_SW = [41.60, -87.95]
CHICAGO_NE = [42.05, -87.50]


def _counties_geojson_path() -> Path:
    
    p = Path(__file__).resolve().parent / "map_data" / "gz_2010_us_050_00_500k.json"
    if not p.exists():
        raise FileNotFoundError(f"Counties GeoJSON not found: {p}")
    return p


def load_risk_points(limit: int = 50000) -> List[List[float]]:
    """
    Reads risk_grid produced by chicago_data_normalization.py
    Columns: grid_id, score_0_100
    Returns HeatMap points: [lat, lon, weight(0..1)]
    """
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT grid_id, score_0_100
            FROM risk_grid
            WHERE grid_id IS NOT NULL
            ORDER BY score_0_100 DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        
        total = conn.execute("SELECT COUNT(*) FROM risk_grid").fetchone()[0]
        print(f"risk_grid total rows: {total}")
        if rows:
            print(f"top score: {rows[0][1]}")
    finally:
        conn.close()

    pts: List[List[float]] = []
    for grid_id, score in rows:
        try:
            lat_s, lon_s = str(grid_id).split(":")
            lat, lon = float(lat_s), float(lon_s)
        except Exception:
            continue

        try:
            w = max(0.0, min(1.0, float(score) / 100.0))
        except Exception:
            continue

        pts.append([lat, lon, w])

    return pts


def main() -> None:
    # Base map
    m = folium.Map(
        location=[39.5, -98.35],
        zoom_start=4,
        tiles="CartoDB positron",
        control_scale=True,
        max_bounds=True,
    )
    m.fit_bounds([USA_SW, USA_NE])

    
    counties_path = _counties_geojson_path()
    folium.GeoJson(
        str(counties_path),
        name="USA Counties",
        style_function=lambda feature: {
            "fillColor": "lightgray",
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.15,
        },
    ).add_to(m)

    
    pts = load_risk_points(limit=50000)
    if not pts:
        raise RuntimeError("No points found from risk_grid. Run normalization first.")

    HeatMap(
        pts,
        name="Risk Heatmap",
        radius=14,
        blur=18,
        min_opacity=0.25,
        max_zoom=12,
    ).add_to(m)

   
    m.fit_bounds([CHICAGO_SW, CHICAGO_NE])

    folium.LayerControl().add_to(m)

    out = Path(__file__).resolve().parent / "map_risk.html"
    m.save(str(out))
    print("Saved:", out)
    print("DB:", DB_PATH)


if __name__ == "__main__":
    main()
