import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

def create_dummy_gdf():
    """
    Create a dummy GeoPandas DataFrame with 5 rows of square polygons in EPSG:4326.
    
    Returns:
    gpd.GeoDataFrame: A GeoDataFrame with index and geometry columns.
    """
    # Define the coordinates for the square polygons
    polygons = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
        Polygon([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]),
        Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
        Polygon([(3, 3), (4, 3), (4, 4), (3, 4), (3, 3)]),
        Polygon([(4, 4), (5, 4), (5, 5), (4, 5), (4, 4)])
    ]
    
    # Create a DataFrame with an index
    df = pd.DataFrame(index=range(5))
    
    # Add the geometry column
    df['geometry'] = polygons
    
    # Convert to a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    
    return gdf

# Example usage
if __name__ == "__main__":
    gdf = create_dummy_gdf()
    print(gdf)