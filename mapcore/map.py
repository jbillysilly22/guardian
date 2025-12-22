from pathlib import Path
import folium
import geopandas as gpd
import fiona


USA_SW = [24.0, -130.0]
USA_NE = [50.0, -67.79]


m = folium.Map(
    location=[39.5, -98.35],
    zoom_start=4,
    tiles="CartoDB positron",
    control_scale=True,
    max_bounds=True,
)

m.fit_bounds([USA_SW, USA_NE])  


BASE_DIR = Path(__file__).resolve().parent
COUNTIES_PATH = BASE_DIR / "map_data" / "gz_2010_us_050_00_500k.json"


gdf = gpd.read_file(COUNTIES_PATH, engine="fiona")  
gdf = gdf.to_crs(epsg=4326)

folium.GeoJson(
    gdf.to_json(),
    name="USA Counties",
    style_function=lambda feature: {
        "fillColor": "lightgray",
        "color": "black",
        "weight": 0.5,
        "fillOpacity": 0.3,
    },
).add_to(m)

folium.LayerControl().add_to(m)

m.save(str(BASE_DIR / "map.html"))
print("Saved:", BASE_DIR / "map.html")



