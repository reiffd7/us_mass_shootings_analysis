import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt 
from scipy import stats
import math
import requests
import ast
import random
from scipy import stats

plt.style.use('fivethirtyeight')


def cluster_variables(all_variables, subset):
    '''
        Args:
            all_variables : array of all census variables to be divided into subsets
            subset: tuple specificying which rows of all variables to cluster

        Returns: 
            cluster : array of census variables in the subset
        
        '''
    cluster = np.array([all_variables[i].tolist() for i in range(subset[0], subset[1]+1)])
    return cluster

def clean_data(frame, term=-666666666.0):
    '''
        Args:
            frame : dataframe to be cleaned
            term (int): specifies which value should removed from each column

        Returns: 
            frame : dataframe with term removed from each column
        
        '''
    for i in frame.columns:
        frame = frame[~(frame[i] == term)]
    return frame


def clean_columns(frame, cluster, index):
    '''
        Args:
            frame : dataframe to be cleaned
            cluster: array of variables corresponding to frame's columns
            index (int): specifies which index of the variable name found in cluster we want to rename each column

        Returns: 
            frame : dataframe with renamed columns 
        
        '''
    frame = frame.rename(columns={cluster[i-8][1]: cluster[i-8][1].split('!!')[index].replace(' ', '_') for i in range(8, 8+len(cluster))})
    return frame


class Grapher(object):
    '''
    Args:
        hypo_test (bool): do we want to do a hypothesis test?
        
        data: Pandas dataframe to be graphed or pandas series if we are graphing a hypothesis test

        cluster: array of variable names that are going to be graphed. Used to make call to national api to get the national mean of each census variable within a census cluster
                one variable name if we are graphing a hypothesis test

        fname (str): name of file of the graph that will be saved

        fig_rows (int): number of rows in the subplot for each census variable

        fig_cols (int): number of columns in the subplot for each census variable

        size_x (int): x component of the figure size

        size_y (int): y component of the figure size

    '''
    
    def __init__(self, hypo_test, data, cluster, fname, fig_rows, fig_cols, size_x, size_y):
        self.hypo_test = hypo_test
        self.data = data
        self.cluster = cluster
        self.fname = fname
        self.fig_rows = fig_rows
        self.fig_cols = fig_cols
        self.size_x = size_x
        self.size_y = size_y 
        
    def _national_means(self):
        '''
        Args:
            self.cluster : array of census variable names that are going to be graphed

        Returns: 
            results : array of national means for each census variable
        
        '''
        results = []
        for i in range(len(self.cluster)):
            search_term = self.cluster[i][0]
            results.append(self._national_call_api(search_term))
        return results

    
    def _national_call_api(self, search_term, key="259ba8642bd19b70be7abaee303575bb2435f9e3"):
        '''
        Args:
            search_term (str): name of census variable to be queried 

        Returns: 
            isolated_value (float): value gathered from query 
        
        '''
        query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=us:1&key={}".format(search_term, key)
        call = requests.get(query).text
        clean_call = ast.literal_eval(call)
        isolated_value =  float(clean_call[1][1])
        return isolated_value

    def _national_distribution(self, search_term, key="259ba8642bd19b70be7abaee303575bb2435f9e3"):
        states = ["%.2d" % i for i in range(1, 57)]
        states.remove('03')
        states.remove('07')
        states.remove('14')
        states.remove('43')
        states.remove('52')
        result = []
        for state in states:
            try:
                query = "https://api.census.gov/data/2017/acs/acs5/profile?get=NAME,{}&for=state:{}&key={}".format(search_term, state, key)
                print('querrying')
                call = requests.get(query).text
                print('cleaning')
                clean_call = ast.literal_eval(call)
                print('isolating')
                isolated_value =  float(clean_call[1][1])
                print(state, isolated_value)
                result.append(isolated_value)
            except:
                import pdb; pdb.set_trace()
        return result

        
    def plot_cluster(self):
        '''
        We use basically all of the class attributes here to either plot histograms for a cluster or a hypothesis test for one census variable
        If we plot a hyothesis test, we will have to find a national distribution.
        If we plot histograms, we will have to call national means to get the national mean for each census variable
        '''
        fig = plt.figure(figsize=(self.size_x, self.size_y))
        if self.hypo_test:
            search_term = self.cluster[0]
            null_sample = self._national_distribution(search_term)
            self._plot_hypo_test(fig.add_subplot(self.fig_rows, self.fig_cols, 1), null_sample)
            plt.savefig(self.fname)
            plt.show()
        else:
            national_mean = self._national_means()
            print(national_mean)
            for i in range(0, len(self.data.columns)):
                self._plot_hist(fig.add_subplot(self.fig_rows, self.fig_cols, i+1), self.data.iloc[:, i], self.data.columns[i], national_mean[i])
            plt.savefig(self.fname)
            plt.show()


    def _plot_hist(self, ax, column, name, national):
        ## no Nan values
        print(column)
        new_column = column[~np.isnan(column)]
        ax.hist(new_column, bins=100)
        ax.axvline(national, color='red')
        ax.set_title(name, fontsize = 12)
        ax.set_ylabel('Frequency')
        ax.set_xlabel('Percentage of Population')

    def _plot_hypo_test(self, ax, null_sample):
        '''
        We use the null_sample to find the null_distribution and self.data fo find the sample_distribution
        Both distributions are plotted and the p-value is found and also plotted.
        '''
        us_sample = np.array(null_sample)
        print(us_sample)
        new_column = self.data[~np.isnan(self.data)]

        samp_mean = np.mean(new_column.to_numpy())
        samp_std = np.std(new_column.to_numpy())/np.sqrt(len(new_column))

        us_mean = np.mean(us_sample)
        us_std = np.std(us_sample)/np.sqrt(len(us_sample))

        null_dist = stats.norm(loc = us_mean, scale = us_std)
        samp_dist = stats.norm(loc = samp_mean, scale = samp_std)
        lower = null_dist.ppf(0.025)
        upper = null_dist.ppf(0.975)
        diff = 2*np.absolute(us_mean-samp_mean)
        us_x_values = np.linspace((us_mean - (10*us_std)), (us_mean + (10*us_std)), 250)
        samp_x_values = np.linspace((samp_mean - (10*samp_std)), (samp_mean + (10*samp_std)), 250)
        null_pdf = null_dist.pdf(us_x_values)
        samp_pdf = samp_dist.pdf(samp_x_values)
        cdf_calc = null_dist.cdf(samp_mean)
        p_value = round((1 - cdf_calc), 2)
        p_string = "p_value = {}".format(p_value)
        ax.plot(us_x_values, null_pdf, label = 'Null Distribution (entire US)', color = 'red')
        ax.plot(samp_x_values, samp_pdf, label = 'Sample Distribution', color = 'blue')
        ax.axvline(samp_mean, color='red', linestyle= '--', linewidth=1)
        ax.axvline(lower, color='green', linestyle= '--', linewidth=1)
        ax.axvline(upper, color='green', linestyle= '--', linewidth=1)
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
        ax.text(0.7, 0.6, p_string, transform=ax.transAxes, fontsize=14,verticalalignment='top', bbox=props)
        ax.set_xlabel('Percentage of the Population')
        ax.set_ylabel('Probability Density')
        ax.legend()
        ax.set_title(self.data.name + ' Hypothesis Test')
     
    




if __name__ == '__main__':
    ## Load Data
    health_data = pd.read_csv('data/ohaver_health_data.csv')
    industry_data = pd.read_csv('data/mesa_industry_data.csv')
    commute_data = pd.read_csv('data/mesa_commute_data.csv')
    income_data = pd.read_csv('data/mesa_income_benefits_data.csv')
    vet_data = pd.read_csv('data/mesa_vet_data.csv')
    internet_data = pd.read_csv('data/mesa_internet_data.csv')
    age_data = pd.read_csv('data/ohaver_age_data.csv')
    gender_data = pd.read_csv('data/ohaver_gender_data.csv')
    race_data = pd.read_csv('data/ohaver_race_data.csv')

    econ_df = pd.read_csv('data/econ_var_names.csv')
    social_df = pd.read_csv('data/social_var_names.csv')
    demo_df = pd.read_csv('data/demo_var_names.csv')
    econ_var_names = econ_df.to_numpy()
    social_var_names = social_df.to_numpy()
    demo_var_names = demo_df.to_numpy()
    
    
    ## Census Variable Clusters
    econ_clusters = {'Industry': (31, 43), 'Commute': (17, 22), 'Income_Benefits': (50, 59), 'Health_Insurance': (94, 96)}
    industry = cluster_variables(econ_var_names, econ_clusters['Industry'])
    commute = cluster_variables(econ_var_names, econ_clusters['Commute'])
    income_benefits = cluster_variables(econ_var_names, econ_clusters['Income_Benefits'])
    health = cluster_variables(econ_var_names, econ_clusters['Health_Insurance'])

    social_clusters = {'Internet': (149, 150), 'Language': (109, 119), 'Education': (57, 65), 'Veteran_Status': (67, 67)}
    internet = cluster_variables(social_var_names, social_clusters['Internet'])
    language = cluster_variables(social_var_names, social_clusters['Language'])
    education = cluster_variables(social_var_names, social_clusters['Education'])
    vet_status = cluster_variables(social_var_names, social_clusters['Veteran_Status'])

    demo_clusters = {'Age': (3, 15), 'Gender': (0, 1), 'Race': (35, 54), 'Latino': (71, 74)}
    age = cluster_variables(demo_var_names, demo_clusters['Age'])
    gender = cluster_variables(demo_var_names, demo_clusters['Gender'])
    race = cluster_variables(demo_var_names, demo_clusters['Race'])

    ## Clean Data
    health_data = clean_data(health_data)
    industry_data = clean_data(industry_data)
    commute_data = clean_data(commute_data)
    income_data = clean_data(income_data)
    vet_data = clean_data(vet_data)
    internet_data = clean_data(internet_data)
    age_data = clean_data(age_data)
    gender_data = clean_data(gender_data)
    race_data = clean_data(race_data)

    health_data = clean_columns(health_data, health, 3)
    # industry_data = clean_columns(industry_data, industry, 3)
    commute_data = clean_columns(commute_data, commute, 3)
    
    vet_data = clean_columns(vet_data, vet_status, 3)
    internet_data = clean_columns(internet_data, internet, 3)
    age_data = clean_columns(age_data, age, 3)
    gender_data = clean_columns(gender_data, gender, 3)
    race_data = clean_columns(race_data, race, 4)


    

    ## Graph
    # graph_obj = Grapher(False, industry_data.iloc[:, 9:], industry, 'mesa_viz/industry.png', 13, 1, 20, 10)
    # graph = graph_obj.plot_cluster()


    


   