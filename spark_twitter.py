"""
In this exercise, I used PySpark to find tweets containing drug-related content that were sent from users located in the 500 largest cities in the US. The script calculates the percentage of tweets for each city that were related to drug use. For example, if 100 users tweeted while located in Cityville, USA and 10 of those tweets were related to drug use, then this script would find that 10% of tweets from Cityville, USA are drug-related.

This script analyzed 100 million geotagged tweets and compared their content to a set of drug-related terms and their geotagged locations to the census tract boundaries of the 500 largest cities in the US. 

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
    # Create a geodataframe from the input shapefile and convert to 5070
    # coordinate projection system
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(5070))

    # Check to make sure geographies are valid and population isn't 0
    zones1 = zones[zones.is_valid].reset_index(drop=True)
    zones2 = zones1[zones1['plctrpop10']>0].reset_index(drop=True)

    # Create an R-Tree spatial index
    index = rtree.Rtree()
    # Iterate through shapefile, create indices, and get the polygon geometry
    # data for each feature
    for idx,geometry in enumerate(zones2.geometry):
        # Add the indices and create bounding boxes based on polygons
        index.insert(idx, geometry.bounds)
    # Return the R-Tree spatial index and the "zones" geodataframe
    return (index, zones2)

# Function for determing which zone a given point falls within
def findZone(p, index, zones):
    # Match the input point to the given R-Tree index using intersection
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        # Check that point matches a point within the points geometry column of
        # shapefile
        if zones.geometry[idx].contains(p):
            # Return the index associated with that point
            return idx
    return None

# Function for getting drug terms
def drugTerms(files):
    drugTerms = set()
    for file in files:
        with open(file, 'r') as fi:
            for word_phrase in fi:
                word_phrase = word_phrase.strip('\n')
                if len(word_phrase) > 2:
                    drugTerms.add(word_phrase.lower())
                # For dealing with very short drug terms like "x"
                else:
                    drugTerms.add(" " + word_phrase.lower() + " ")
                    drugTerms.add(" " + word_phrase.lower() + ".")
                    drugTerms.add(" " + word_phrase.lower() + "!")
                    drugTerms.add(" " + word_phrase.lower() + "?")
    return drugTerms

# Function to put it all together
def processTweets(pid, records):
    # Import needed libraries
    import csv
    import pyproj
    import shapely.geometry as geom

    # Get drug words
    drugFiles = ['drug_illegal.txt', 'drug_sched2.txt']
    drugs = drugTerms(drugFiles)

    # Set coordinate projection system to make sure data sets match
    proj = pyproj.Proj(init="epsg:5070", preserve_units=True)
    # Get the R-Tree index and geodataframe for US cities
    index_cities, zones_cities = createIndex('500cities_tracts.geojson')

    # Skip the first row of the input dataset
    if pid==0:
        next(records)
    # Read in the tweets
    reader = csv.reader(records, delimiter='|')
    # Create a dictionary to keep track of drug-related tweet counts
    counts = {}

    # Iterate through the tweets
    for row in reader:
        # Check to make sure all fields in each row
        if len(row) != 7:
            continue
        else:
            # Check to see if drug terms in tweet
            tweet = row[5].lower()
            if any(word in tweet for word in drugs):
                # Get the tweet lon/lat
                tweet_lon, tweet_lat = row[2], row[1]
                # Skip over rows with 'NULL' entered for lon/lat
                if (tweet_lon=='NULL' or tweet_lat=='NULL'):
                    continue
                # Make sure all longitude and latitude can be converted from
                # string to float
                try:
                    tweet_lon = float(tweet_lon)
                except ValueError:
                    continue
                try:
                    tweet_lat = float(tweet_lat)
                except ValueError:
                    continue
                # Create a point for the tweet with the correct projection
                tweet_p = geom.Point(proj(tweet_lon, tweet_lat))

                # Match the tweet lon/lat to a city
                city_zone = findZone(tweet_p, index_cities, zones_cities)
                if city_zone is None:
                    continue

                # Keep a count for every census tract with population
                if city_zone:
                    key = tuple([zones_cities.iloc[city_zone]['plctract10'], \
                    zones_cities.iloc[city_zone]['plctrpop10']])
                    counts[key] = counts.get(key, 0) + 1
    # Return dict
    return counts.items()

"""
Overview of the following RDD transformations and action:
1. Initiate the spark context, create an RDD, and apply my processTweets
function to the RDD
2. Reduce on the census tract-population pairs
3. Pull the populations out and pair them with the counts as values
4. Divide the counts by the population of each census tract
5. Round the values to 4 decimal points
6. Collect and print
"""
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/tweets-100m.csv', minPartitions=75)
    counts = rdd.mapPartitionsWithIndex(processTweets)
    result = counts.reduceByKey(lambda x,y: (x+y)) \
        .map(lambda x: (x[0][0], (x[1], x[0][1]))) \
        .mapValues(lambda x: x[0]/x[1]) \
        .mapValues(lambda x: round(x, 4)) \
        .sortByKey() \
        .collect()
    print(result)
