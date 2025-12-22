from pathlib import Path
import sqlite3

import folium
import geopandas as gpd
from folium.plugins import HeatMap

# your DB path (same one normalization wrote into)
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_sqlite import DB_PATH


USA_SW = [24.0, -130.0]
USA_NE = [50.0, -67.79]

# optional: zoom to Chicago so you can actually SEE the heat
CHICAGO_SW = [41.60, -87.95]
CHICAGO_NE = [42.05, -87.50]


def _find_counties_geojson(base_dir: Path) -> Path:
    fname = "gz_2010_us_050_00_500k.json"
    candidates = [
        base_dir / "map_data" / fname,                 # if you run from inside mapcore/
        base_dir / "mapcore" / "map_data" / fname,     # if map.py is at repo root
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError(f"Could not find {fname} in: {candidates}")


def load_risk_points(limit: int = 50000):
    """
    Expects risk_grid columns like:
      grid_id (e.g. "41.893:-87.636") and score (0-100)
    """
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # adjust column name if yours isn't literally "score"
        rows = conn.execute(
            "SELECT grid_id, score FROM risk_grid ORDER BY score DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    pts = []
    for grid_id, score in rows:
        if not grid_id:
            continue
        try:
            lat_s, lon_s = str(grid_id).split(":")
            lat, lon = float(lat_s), float(lon_s)
        except Exception:
            continue

        # folium HeatMap weight works best as 0..1
        w = max(0.0, min(1.0, float(score) / 100.0))
        pts.append([lat, lon, w])

    return pts


def main():
    base_dir = Path(__file__).resolve().parent

    # Base map
    m = folium.Map(
        location=[39.5, -98.35],
        zoom_start=4,
        tiles="CartoDB positron",
        control_scale=True,
        max_bounds=True,
    )
    m.fit_bounds([USA_SW, USA_NE])

    # Counties layer
    counties_path = _find_counties_geojson(base_dir)
    gdf = gpd.read_file(counties_path, engine="fiona").to_crs(epsg=4326)

    folium.GeoJson(
        gdf.to_json(),
        name="USA Counties",
        style_function=lambda feature: {
            "fillColor": "lightgray",
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.2,
        },
    ).add_to(m)

    # Risk heat layer
    pts = load_risk_points(limit=50000)
    if not pts:
        raise RuntimeError("No points found from risk_grid. Run normalization first.")

    HeatMap(
        pts,
        name="Risk Heatmap",
        radius=14,
        blur=18,
        min_opacity=0.2,
        max_zoom=12,
    ).add_to(m)

    # zoom to Chicago so you can see it right away
    m.fit_bounds([CHICAGO_SW, CHICAGO_NE])

    folium.LayerControl().add_to(m)

    out = base_dir / "map_risk.html"
    m.save(str(out))
    print("Saved:", out)
    print("DB:", DB_PATH)


if __name__ == "__main__":
    main()
