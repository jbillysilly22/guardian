from pathlib import Path
import folium
import geopandas as gpd

#world map but location set to center of USA also using geopandas to read in a geojson file of USA counties
world_map = folium.Map(
    location=[39.5,-98.35],
    control_scale=True,
    zoom_start = 4,
    tiles="CartoDB positron",
)

usa_counties = gpd.read_file("core/map_data/gz_2010_us_050_500k.json")

usa_counties_gdf = usa_counties.to_crs(epsg=4326)

usa_counties_json = usa_counties_gdf.to_json()

usa_counties_layer = folium.GeoJson(
    usa_counties_json,
    name="USA Counties",
    style_function=lambda feature: {
        "fillColor": "lightgray",
        "color": "black",
        "weight": 0.5,
        "fillOpacity": 0.5,
    },
)

