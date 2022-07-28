# ### Read preprocessed data and calculate the probability based on different distributions

import os
import pandas as pd
pd.set_option("display.max_columns", 50)
import scipy.stats as stats


# Since we've already pre-computed the best fitted distribution, we can now based on the mean and standard deviation of it and create a new dataframe with the delay probability.  
# The calculation is through examing into the **Cumulative Distribution Function(cdf)** of the given distribution.  
# From the probability course we know that, for a given value X, the probability of a variable falls under X is the corresponding value in cdf.  
# With this, we can easily calculate the probability of the given delay time and then feed it to our Route Planning Algorithm.  

def calculate_prob(data_path):
    
    #cleaning the data
    df = pd.read_csv(data_path).drop('Unnamed: 0', axis=1).drop('number_of_delays', axis=1)
    df = df.fillna(0)
    df.sort_values(by=['stop_name', 'day', 'hour'], inplace=True)
    df = df.astype({'day': 'int64', 'hour': 'int64', 'stop_name': 'string', 'distribution_type': 'string'})
    df = df.drop(df[df.hour < 6].index)
    df = df.drop(df[df.hour > 22].index)
    
    
    mean= df['mean'].tolist()
    std= df['std'].tolist()   
    distr = df['distribution_type'].tolist()

    # calculate delay from 0 min to 10 min
    for i in range(11):
        col_name = str(i) + '_min_prob'
        prob = []
        for j in range(len(mean)):
            delay = i * 60
            if(std[j] == 0):
                if(mean[j] == 0 or int(mean[j]/60) <= i-1):
                    prob.append(1.0)  # this means when there is no data -> assume no delay
                    continue

            if(distr[j] == 'expon'): 
                prob.append(stats.expon.cdf(loc=mean[j], x = delay, scale=std[j]))
            elif(distr[j] == 'norm'): 
                prob.append(stats.norm.cdf(loc=mean[j], x = delay, scale=std[j]))

        df[col_name] = prob
    
    df = df.fillna(0).drop('distribution_type', axis=1)
    
    return df

df = calculate_prob('../data/dist_all_both.csv')
df.to_csv('../data/dist_all_delay_prediction.csv')

# ### Define the Pipeline for CSA

import datetime
from math import floor


# +
def read_query(stop_name, date, time, delay):
    # stop_name -> string, date -> tuple(yyyy, mm, dd), time -> int(minute in a day), delay -> int(how many min)
    
    day = datetime.datetime(date[0], date[1], date[2]).weekday()+1
    hour = floor(int(time)/60)
    delay_col = str(delay) + '_min_prob'
    
    df = pd.read_csv('../data/dist_all_delay_prediction.csv').drop('Unnamed: 0', axis=1)
    df = df.astype({'day': 'int64', 'hour': 'int64', 'stop_name': 'string'})
    
    query_res = df.loc[(df['stop_name'] == stop_name) & (df['hour'] == hour) & (df['day'] == day)][delay_col].values.item(0)
    
#     if(query_res == -1.0):
#         # There is no delay record -> return 0?
#         query_res = 1.0 if delay == 0 else 0.0
        
    return query_res
    


# -

# Sample query way 
read_query("Dielsdorf, Wehntalerstrasse", (2019, 5, 15), 1300, 2)


