import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


def parse_table_to_data(table):
    """
    Input:
        table (str): a beautiful soup searched table in an html webpage
    Output:
        (np array) : a multi-dimensional array representing the data contained in the table
    """
    data = []
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)
    return np.array(data)


def filter_subset_data(census_vars, subset):
    """
    Input:
        census_var_names (np array): a multi-dimensional array of all census variables
        subset (str): a phrase that broadly subsets the census variables
    Output:
        subset_vars : a multi-dimensional array representing census variables of a subset
    """
    subset_vars = census_vars[census_vars[:,2] == subset]
    return subset_vars

def filter_percentages(census_vars):
    """
    Input:
        census_vars (np array): a multi-dimensional array of census variables
        
    Output:
        (np array) : only the census variables that measure a percent estimate
    """
    result = []
    for i in range(len(census_vars)):
        if 'Percent Estimate' in census_vars[i][1]:
            result.append([census_vars[i][0], census_vars[i][1]])
    return np.array(result)

def var_names_to_file(var_names, file_name):
     """
    Input:
        var_names (np array): a multi-dimensional array of census variables
        file_name (str): name of the file to be exported
        
    Output: (None)
    """
    pd.DataFrame(var_names).to_csv(file_name, header=False, index=False)

if __name__ == '__main__':
    
    
    url = requests.get("https://api.census.gov/data/2017/acs/acs5/profile/variables.html").text
    soup = BeautifulSoup(url, "html.parser")
    table = soup.find('table')
    census_var_names = parse_table_to_data(table)[:, 0:3]

    subset = 'ACS DEMOGRAPHIC AND HOUSING ESTIMATES'
    demographic_vars = filter_subset_data(census_var_names, subset)
    demographic_percent_vars = filter_percentages(demographic_vars)

    
