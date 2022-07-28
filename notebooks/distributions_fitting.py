# +
import os
import pandas as pd
pd.set_option("display.max_columns", 50)
import matplotlib.pyplot as plt
# %matplotlib inline
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

username = os.environ['RENKU_USERNAME']
hiveaddr = os.environ['HIVE_SERVER2']
print("Operating as: {0}".format(username))
print("Hive server at: {0}".format(hiveaddr))

# +
from pyhive import hive

# Hive host and port number
(hive_host, hive_port) = hiveaddr.split(':')

# create connection
conn = hive.connect(host=hive_host, 
                    port=hive_port,
                    username=username) 
# create cursor
cur = conn.cursor()
# -

# ### Load dataframe using hive
#
# Hive allows us to read and load the pre-processed dataframe into a ```pandas``` dataframe.

dataframe_to_be_loaded = 'dataset_mean_26_05_2022'

# +
query = """
    CREATE DATABASE IF NOT EXISTS {0}
""".format(username)
cur.execute(query)

query = """
    USE {0}
""".format(username)
cur.execute(query)

# +
query = """
    DROP TABLE IF EXISTS {0}.sbb_dataset
""".format(username)
cur.execute(query)

query = """
    CREATE EXTERNAL TABLE {0}.sbb_dataset(
        `date` STRING,
        day INTEGER,
        hour INTEGER,
        stop_name STRING,
        avg_arrival_delay DOUBLE
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
    STORED AS ORC
    LOCATION '/group/big-data-projects/{1}.orc'
""".format(username, dataframe_to_be_loaded)
cur.execute(query)

# +
query = """
SELECT `date`, day, hour, stop_name, avg_arrival_delay FROM {0}.sbb_dataset LIMIT 100
""".format(username)
sbb_dataset = pd.read_sql(query, conn)

print(sbb_dataset)
sbb_dataset.describe()
# -

# Save locally if needed

sbb_dataset.to_csv('../data/{}.csv'.format(dataframe_to_be_loaded))

# Import fitting library

# !pip install fitter

import seaborn as sns
from fitter import Fitter, get_common_distributions, get_distributions
import numpy as np
import time

# ### Fitting different distributions and taking the best only
#
# Here, we perform the distributions fitting using the library ```fitter```, which allows for many distributions to be fitted. As explained in the ```README.md```, we choose to fit an **exponential** and a **normal** distribution for each query in our dataset in order to account for the variety of distributions shapes in our dataset. The cell used to perform serial computations is provided below, while a seperate file called ```Distributions_fitting_parallel.py``` allows for parallelizing to computations.

# +
print('Nb. of stops : {}'.format(len(sbb_dataset.stop_name.unique())))

N = len(sbb_dataset.stop_name.unique())
M = len(sbb_dataset.hour.unique()) * len(sbb_dataset.day.unique()) * len(sbb_dataset.stop_name.unique())

stop_names = sbb_dataset.stop_name.unique()
hours = np.arange(6,23,1)
days = np.arange(1,8,1)

# Create new dataframe
df_dist = pd.DataFrame({
    'number_of_delays':[],
    'stop_name': [],
    'day': [],
    'hour': [],
    'distribution_type': [],
    'mean': [],
    'std': []
})

# List of distributions to be tested
distributions = ['expon','norm']

# Loop through the whole thing
for i, stop_name in enumerate(stop_names):
    print('\rStop name {}/{} '.format(i+1,N), end='')
    df1 = sbb_dataset[(sbb_dataset.stop_name == stop_name)]
    for j, hour in enumerate(hours):
        df2 = df1[(df1.hour == hour)]
        for k, day in enumerate(days):
            
            dist = df2[(df2.day == day)].values[:,4].astype(np.float64)
            
            if dist.shape[0]:
                # Fitting the distribution
                dist_fit = Fitter(dist,
                                  distributions=distributions)
                dist_fit.fit()
                
                distribution = dist_fit.summary().index[np.argmin(dist_fit.summary().sumsquare_error)]
                mean = dist_fit.fitted_param[distribution][0]
                std = dist_fit.fitted_param[distribution][1]
                
            else:
                mean = None
                std = None
            
            # The dataframe to append
            df = pd.DataFrame({
                'number_of_delays': [len(dist)],
                'stop_name': [stop_name],
                'day': [day],
                'hour': [hour],
                'distribution_type': [distribution],
                'mean': [mean],
                'std': [std]
            })
            df_dist = pd.concat([df_dist,df])
# -

# Save dataframe

df_dist.to_csv('../data/dist_all.csv')

# ### Parallel computations
#
# In order to run the distributions fitting in parallel, the file ```Distributions_fitting_parallel.py``` can be run from the command line with no arguments. Just pay attention to stay within the directory containing this notebook when doing that.
