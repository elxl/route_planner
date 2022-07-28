# -*- coding: utf-8 -*-
"""
Created on Fri May 27 09:36:39 2022

@author: cedri
"""

from fitter import Fitter
import pandas as pd
import numpy as np
import multiprocessing as mp
import sys
from tqdm import tqdm
import functools

sbb_dataset_or = pd.read_csv('../data/dataset_mean_26_05_2022.csv')
sbb_dataset_or.drop('Unnamed: 0',axis=1,inplace=True)

print('Nb. of stops : {}'.format(len(sbb_dataset_or.stop_name.unique())))

N = len(sbb_dataset_or.stop_name.unique())
M = len(sbb_dataset_or.hour.unique()) * len(sbb_dataset_or.day.unique()) * len(sbb_dataset_or.stop_name.unique())

stop_names = tqdm(sbb_dataset_or.stop_name.unique())
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
# distributions = ['expon','norm','lognorm','burr','weibull_min','gamma']
distributions = ['expon','norm']
    
def looping(stop_name, sbb_dataset, dist_list = distributions):
    df_dist_temp = pd.DataFrame({
        'number_of_delays':[],
        'stop_name': [],
        'day': [],
        'hour': [],
        'distribution_type': [],
        'mean': [],
        'std': []
    }) 
    # print >> sys.__stdout__, str('Stop name {}/{} '.format(i+1,N))
    # print('Stop name {}/{} '.format(i+1,N))
    df1 = sbb_dataset[(sbb_dataset.stop_name == stop_name)]
    for j, hour in enumerate(hours):
        df2 = df1[(df1.hour == hour)]
        for k, day in enumerate(days):
            
            dist = df2[(df2.day == day)].values[:,4].astype(np.float64)
            
            if dist.shape[0]:
                # Fitting the distribution
                dist_fit = Fitter(dist,
                                  distributions=dist_list)
                dist_fit.fit()
                
                distribution = dist_fit.summary().index[np.argmin(dist_fit.summary().sumsquare_error)]
                mean = dist_fit.fitted_param[distribution][0]
                std = dist_fit.fitted_param[distribution][1]
                
            else:
                mean = None
                std = None
                distribution = None
            
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
            df_dist_temp = pd.concat([df_dist_temp,df])
            
    return df_dist_temp
            
N = mp.cpu_count()

looping_ = functools.partial(looping, sbb_dataset=sbb_dataset_or, dist_list=distributions)

if __name__ == '__main__':
    
    mp.freeze_support()

    with mp.Pool(processes = N) as p:
        results = list(p.imap(looping_, stop_names))
        
    for res in results:
        df_dist = pd.concat([df_dist,res])
        
    df_dist.to_csv('../data/dist_all_both.csv')