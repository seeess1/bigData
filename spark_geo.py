"""
In this exercise, I used PySpark to find, for every borough in NYC, the neighborhoods where most taxi trips began. For example, we could find that most taxi trips that end in Manhattan begin in the Upper East Side of Manhattan, Williamsburg in Brooklyn, and Long Island City in Queens.

This script analyzed data from more than 15 million taxi trips taken in May 2011 as well as spatial shapefiles for the borough and neighborhood boundaries of NYC. 

Note: I ran my script on NYU's High Performance Computing cluster using command line syntax and elements that are specific to NYU's HPC.
"""

# Import SparkContext
from pyspark import SparkContext

# Function to create spatial indices for testing points against polygons
def createIndex(shapefile):
    # Import needed libraries
    import rtree
    import fiona.crs
    import geopandas as gpd
    # Create a geodataframe from the input shapefile and convert to 2263 coordinate projection system
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    # Create an R-Tree spatial index
    index = rtree.Rtree()
    # Iterate through shapefile, create indices, and get the polygon geometry data for each feature
    for idx,geometry in enumerate(zones.geometry):
        # Add the indices and create bounding boxes based on polygons
        index.insert(idx, geometry.bounds)
    # Return the R-Tree spatial index and the "zones" geodataframe
    return (index, zones)

# Function for determing which zone a given point falls within
def findZone(p, index, zones):
    # Match the input point to the given R-Tree index using intersection
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        # Check that point matches a point within the points geometry column of shapefile
        if zones.geometry[idx].contains(p):
            # Return the index associated with that point
            return idx
    return None

# Function to put it all together
def processTrips(pid, records):
    # Import needed libraries
    import csv
    import pyproj
    import shapely.geometry as geom

    # Set coordinate projection system to make sure data sets match
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    # Get the R-Tree index and geodataframe for NYC boroughs
    index_bors, zones_bors = createIndex('boroughs.geojson')
    # Get the R-Tree index and geodataframe for NYC neighborhoods
    index_neighbs, zones_neighbs = createIndex('neighborhoods.geojson')

    # Skip the first row of the input dataset
    if pid==0:
        next(records)
    # Read in the taxi data
    reader = csv.reader(records)
    # Create a dictionary to keep track of trip counts
    counts = {}

    # Iterate through the taxi data
    for row in reader:
        # Check to make sure all fields in each row
        if len(row) != 18:
            continue
        else:
            # Get the taxi trip destination lon/lat and origin lon/lat
            dp_lon, dp_lat, op_lon, op_lat = row[9], row[10], row[5], row[6]
            # Skip over rows with 'NULL' entered for destination or origin lon/lat
            if (dp_lon=='NULL' or dp_lat=='NULL' or op_lon=='NULL' or op_lat=='NULL'):
                continue
            # Make sure all longitude and latitude can be converted from string to float
            try:
                dp_lon = float(dp_lon)
            except ValueError:
                continue
            try:
                dp_lat = float(dp_lat)
            except ValueError:
                continue
            try:
                op_lon = float(op_lon)
            except ValueError:
                continue
            try:
                op_lat = float(op_lat)
            except ValueError:
                continue
            # Create a point for the taxi destination with the correct projection
            dest_p = geom.Point(proj(dp_lon, dp_lat))
            # Create a point for the taxi origin with the correct projection
            orig_p = geom.Point(proj(op_lon, op_lat))

            # Match the taxi trip destination to a borough
            bors_zone = findZone(dest_p, index_bors, zones_bors)
            # Match the taxi trip origin to a neighborhood
            neighbs_zone = findZone(orig_p, index_neighbs, zones_neighbs)

            # Keep a count for each pair of destination/borough - origin/neighborhood pairs
            if bors_zone and neighbs_zone:
                counts[(bors_zone, neighbs_zone)] = counts.get((bors_zone, neighbs_zone), 0) + 1

    # Match indices in counts dict to the names of boroughs and neighborhoods in new dictionary
    new_dict = {}
    # Iterate over counts dictionary items
    for item in counts.items():
        # Create a new key that matches old indices and replaces them with borough and neighborhood names
        new_key = tuple([zones_bors['boroname'][item[0][0]], zones_neighbs['neighborhood'][item[0][1]]])
        # Add the new key to the new dict along with the associated count
        new_dict[new_key] = new_dict.get(new_key, item[1])

    # Return the new dict
    return new_dict.items()

"""
Overview of the following RDD transformations:
1. Reduce on the borough-neighborhood pairs
2. Switch the keys and values for sorting
3. Sort by the trip numbers in descending order
4. Reverse the keys and values again and put the neighborhoods together with counts
5. Reduce on boroughs only
6. Keep the boroughs and only the first 6 values for each key (the first 3 pairs of neighborhood-count)
7. Format for output
"""
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/yellow_tripdata_2011-05.csv', minPartitions = 75)
    data_result = rdd.mapPartitionsWithIndex(processTrips)
    print(data_result.reduceByKey(lambda x,y: (x+y)) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(ascending=False) \
        .map(lambda x: (x[1][0], (x[1][1], x[0]))) \
        .reduceByKey(lambda x,y: (x+y)) \
        .map(lambda x: (x[0], x[1][:6])) \
        .map(lambda x: (str(x[0]) + ": " + str(x[1]))) \
        .collect())
