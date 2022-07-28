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
    DROP TABLE IF EXISTS {0}.sbb_dataset1
""".format(username)
cur.execute(query)

query = """
    CREATE EXTERNAL TABLE {0}.sbb_dataset1(
        `date` STRING,
        day INTEGER,
        hour INTEGER,
        stop_name STRING,
        avg_arrival_delay DOUBLE
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
    STORED AS ORC
    LOCATION '/group/big-data-projects/dataset_mean_26_05_2022.orc'
""".format(username)
cur.execute(query)
# -

dataset = 'sbb_dataset1'
query = """
SELECT `date`, day, hour, stop_name, avg_arrival_delay FROM {0}.sbb_dataset1 WHERE stop_name = 'Zürich HB' AND day = 3 AND hour = 8
""".format(username)
sbb_dataset = pd.read_sql(query, conn)

import seaborn as sns
from fitter import Fitter, get_common_distributions, get_distributions
import numpy as np
import time

# +
sns.set_style('white')
sns.set_context("paper", font_scale = 2)

distribution = sbb_dataset[(sbb_dataset.day == 3) &
                           (sbb_dataset.hour == 8) &
                           (sbb_dataset.stop_name == 'Zürich HB')]

sns.displot(data=distribution, x="avg_arrival_delay", kind="hist", bins = 100, aspect = 1.5)

# +
dist_fit = Fitter((distribution.values[:,4]).astype(np.float64),
           distributions=['expon','norm'])
fig = plt.figure(figsize=(13,6))

dist_fit.fit()

dist_fit.summary().index[np.argmin(dist_fit.summary().sumsquare_error)]
plt.xlabel('Time delays'), plt.ylabel('Probability of the delay'), plt.title('Probability distribution for stop name = Zürich HB, hour = 8, day = 3')

plt.savefig('../figs/fit_3.png', dpi=400)
