import pyspark as ps    # for the pyspark suite
import requests
from bs4 import BeautifulSoup
import numpy as np
import censusgeocode as cg 
import csv
import json
import ast

def read_variable_names(file_name):
    with open(file_name, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        # get header from first row
        headers = next(reader)
        # get all the rows as a list
        data = list(reader)
        return np.array(data)



def add_census(row):
    if row[0] == 'CustZIP':
        return None
    else:
        try:
            census_geocode_dict = cg.coordinates(row[3], row[2])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['TRACT'])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['STATE'])
            row.append(census_geocode_dict['2010 Census Blocks'][0]['COUNTY'])
            return row
        except:
            return None 
    

def cluster_variables(all_variables, subset):
    return np.array([all_variables[i].tolist() for i in range(subset[0], subset[1]+1)])


def add_census_vars(row, var_names):
    if row == None:
        return row
    else:
        for i in range(len(var_names)):
            search_term = var_names[i][0]
            row.append(call_api(search_term, row))
        return row



def call_api(search_term, row, key="2f321eb597c3d3e59dfa9aa2f694622639dee6fc"):
    tract, state, county = row[5], row[6], row[7]
    query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=tract:{}&in=state:{}%20county:{}&key={}".format(search_term, tract, state, county, key)
    try:
        call = requests.get(query).text
        clean_call = ast.literal_eval(call)
        isolated_value =  float(clean_call[1][1])
        return isolated_value
    except:
        return None 