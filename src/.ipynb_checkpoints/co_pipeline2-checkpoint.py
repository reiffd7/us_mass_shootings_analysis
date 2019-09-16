import numpy as np
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import boto3
import src.api2 as ap
import pyspark as ps# for the pyspark suite
import census_tract as a


spark = ps.sql.SparkSession.builder \
            .master("local[4]") \
            .appName("case study") \
            .getOrCreate()

sc = spark.sparkContext




def site_selector(site, df):
    """
    Input:
        site (str): name of site selected 
        df (dataframe): pandas dataframe of all all sites to be seleceted from 
        
    Output:
        pandas dataframe characetrizing the site w/ columns: CustZIP, CustState, CustLat, CustLong, ResSize
    """
    result = df[df['Park'] == site]
    result_group = result.groupby(['CustZIP', 'CustState', 'CustLat', 'CustLong'])
    result_zips = result_group.count().reset_index(drop = False)
    result_zips = result_zips.drop(['Location', 'Park', 'SiteType', 'UseType', 'FacState', 'FacLong', 'FacLat', 'CustCountry', 'CustSize', 'Dist'], axis=1)
    result_zips = result_zips.rename(columns={'Res_ID':'Res_Size'})
    return result_zips




def all_sites_mapper(df, size_metric, factor, color):
    map_osm = folium.Map(location=[39, -105.547222], zoom_start=7)

    df.apply(lambda row:folium.CircleMarker(location=[row["FacLat"], row["FacLong"]], 
                                                  radius=(row[size_metric]/df[size_metric].max())*factor, color = color, fill_color = color, popup=row['Park'])
                                                 .add_to(map_osm), axis=1)

    return map_osm




def site_customer_mapper(df, popup):
    """
    Input: 
        df (dataframe): pandas dataframe characterizing the site with CustLat, CustLong, Res_Size columns
        popup (str): name of row to be used in popup message for each marker
    Output:
        folium map centered on cont. USA showing each customer location for a given site
    """
    
    map_osm = folium.Map(location=[39.8283, -98.5795], zoom_start=4)
    marker_cluster = MarkerCluster().add_to(map_osm)
    df.apply(lambda row:folium.Marker(location=[row["CustLat"], row["CustLong"]], 
                                              radius=(row["Res_Size"]/df['Res_Size'].max())*10, fill_color='blue', popup = row[popup])
                                             .add_to(marker_cluster), axis=1)
    return map_osm


def add_census_data(df):
    """
    Input: 
        df (dataframe): pandas dataframe characterizing the site w/ columns: CustZIP, CustState, CustLat, CustLong, ResSize
    Output:
        numpy array of input dataframe with added census tract, state number, and county number
    """
    nparr = df.to_numpy()
    rdd = sc.parallelize(nparr)\
        .map(lambda row: row.tolist())\
        .map(lambda row: ap.add_census(row))
    census_data = rdd.collect()
    census_data = list(filter(None.__ne__, census_data))
    census_df = pd.DataFrame.from_records(census_data)
    census_arr = census_df.to_numpy()
    return census_arr



def rdd_to_data(census_data, cluster):
    """
    Input: 
        numpy array of a site's customer information including census tract, state number, and county number
    Output:
        array of a site's customer information with added census data according to the variable cluster for each customer
    """
    rdd = sc.parallelize(census_data)\
        .map(lambda row: row.tolist())\
        .map(lambda row: ap.add_census_vars(row, cluster))
    return rdd.collect()


def arr_to_pandas(arr, cluster):
    """
    Input: 
        array of a site's customer information including census data for a given cluster
    Output:
        pandas dataframe  of a site's customer information with additional columns for each cluster variable
    """
    result = pd.DataFrame(arr)
    result = result.rename(columns={0: "ZIP", 1: "State", 2: "Lat", 3: "Lng", 4: "Size", 5: "Tract", 6: "State_code", 7: "County"})
    result = result.rename(columns={i: cluster[i-8][1] for i in range(8, 8+len(cluster))})
    return result


def export(df, fname, bucket):
    """
    Input: 
        pandas dataframe  of a site's customer information with additional columns for each cluster variable
    Output:
        No output. The data is exported to s3 and csv
    """
    to_export = fname
    df.to_csv(to_export, header=True, index=True)
    s3_client.upload_file(to_export, bucket, to_export)
