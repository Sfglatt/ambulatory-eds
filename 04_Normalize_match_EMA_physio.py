#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import os
from scipy import stats
import random
from datetime import timedelta, datetime
from scipy.stats import pearsonr
import matplotlib.pyplot as plt

# Load Data
use_participant = 'PR003'
surv = pd.read_csv('/path/to/survey.csv', parse_dates=['timestamp'], infer_datetime_format=True, index_col=[0])
base = pd.read_csv('/path/to/baseline.csv', parse_dates=['timestamp'], infer_datetime_format=True, index_col=[0])
ema = pd.read_csv("/path/to/ema.csv", parse_dates=['ethica_time_utc'], infer_datetime_format=True, index_col=[0])

# Filtering functions
def exp_moving_average(signal, w):
    return pd.Series(signal.ewm(span=w, adjust=True).mean(), signal.index)

def filt_EDA(df_data):
    eda_data = ph.EvenlySignal(values=df_data['EDA'].to_numpy(), sampling_freq=4, signal_type='EDA')
    eda_data = ph.IIRFilter(fp=0.8, fs=1.1, ftype='ellip')(eda_data)
    driver = ph.DriverEstim()(eda_data)
    phasic, tonic, _ = ph.PhasicEstim(delta=0.02)(driver)
    if len(phasic) != len(eda_data.get_values()):
        phasic = np.append(phasic.get_values(), phasic[-1])
        tonic = np.append(tonic.get_values(), tonic[-1])
    df_data['Tonic'] = tonic
    df_data['Phasic'] = phasic
    return df_data  

def filt_TEMP(df_data):
    df_data['TEMP_Filtered'] = exp_moving_average(df_data['TEMP'], 60)
    return df_data

def filter_signals(df_data):
    df_data = filt_EDA(df_data)
    df_data = filt_TEMP(df_data)
    return df_data

# Feature extraction functions
def rms(data):
    return np.sqrt(np.mean(data ** 2))

def feature_extract(df_data):
    result = {'Time': df_data['timestamp'].min()}
    for featbase in ['HR', 'EDA', 'TEMP', 'meanCenteredEDA', 'meanCenteredHR', 'meanCenteredTEMP']:
        result[featbase + '_Mean'] = df_data[featbase].mean()
        result[featbase + '_Minimum'] = df_data[featbase].min()
        result[featbase + '_Maximum'] = df_data[featbase].max()
        result[featbase + '_Stdev'] = df_data[featbase].std()
        result[featbase + '_RMS'] = rms(df_data[featbase])
        result[featbase + '_MAD'] = df_data[featbase].mad()
        result[featbase + '_MAV'] = df_data[featbase].abs().max()
        result[featbase + '_Median'] = df_data[featbase].median()
        result[featbase + '_P25'] = df_data[featbase].quantile(0.25)
        result[featbase + '_P75'] = df_data[featbase].quantile(0.75)
    return pd.Series(result, dtype='object')

def average_calc(df_data):
    result = {'EDA_Mean': df_data['EDA'].mean(), 'EDA_Median': df_data['EDA'].median(),
              'HR_Mean': df_data['HR'].mean(), 'HR_Median': df_data['HR'].median(),
              'TEMP_Mean': df_data['TEMP'].mean(), 'TEMP_Median': df_data['TEMP'].median()}
    return pd.Series(result, dtype='object')

# Matching windows
def windowMatch(features, surveys): 
    matchlist = []
    for _, frow in features.iterrows():
        physiotime = frow['Time']
        for _, srow in surveys.iterrows():
            ematime = srow['ethica_time_utc']
            df3 = ematime - physiotime
            if timedelta(seconds=1) < df3 < timedelta(hours=1):
                matchlist.append(pd.concat([frow, srow], axis=0))
    return pd.DataFrame(matchlist)

# Normalize survey windows!
baseline_values = base.groupby(['code']).apply(average_calc)
edamean = baseline_values.at[0, "EDA_Mean"]
edamed = baseline_values.at[0, "EDA_Median"]
hrmean = baseline_values.at[0, "HR_Mean"]
hrmed = baseline_values.at[0, "HR_Median"]
tempmean = baseline_values.at[0, "TEMP_Mean"]
tempmed = baseline_values.at[0, "TEMP_Median"]

surv['meanCenteredEDA'] = surv['EDA'] - edamean
surv['medianCenteredEDA'] = surv['EDA'] - edamed
surv['meanCenteredHR'] = surv['HR'] - hrmean
surv['medianCenteredHR'] = surv['HR'] - hrmed
surv['meanCenteredTEMP'] = surv['TEMP'] - tempmean
surv['medianCenteredTEMP'] = surv['TEMP'] - tempmed

# Center the EMA data
excl_list = ['ethica_time', 'lag', 'tdif', 'cumsumT', 'ethica_time_utc', 'dayvar', 'beepvar', 'beepconsec']
emafeat = [col for col in ema.columns if col not in excl_list]
for feat in emafeat:
    ema[feat + '_meanCentered'] = ema[feat] - ema[feat].mean()
    ema[feat + '_medCentered'] = ema[feat] - ema[feat].median()

# Prepare physio data for matching
physio = surv.sort_values(['timestamp'], ignore_index=True)
physio = physio.groupby(['event']).apply(feature_extract)

# Match windows
mydf = windowMatch(physio, ema)

# Extract features
featbase = ['HR', 'EDA', 'TEMP', 'meanCenteredEDA', 'meanCenteredHR', 'meanCenteredTEMP']
featstat = ['_Mean', '_Minimum', '_Stdev', '_RMS', '_MAD', '_MAV', '_Median', '_P25', '_P75']
physio_feats = {fb: mydf[[fb + fs for fs in featstat]] for fb in featbase}
physio_feats['All'] = mydf[[fb + fs for fb in featbase for fs in featstat]]

# Clean EMA data for correlations
surfeat = [col for col in ema.columns if col not in excl_list]
ema_feats = mydf[surfeat]

# Correlate physio recordings with EMA data
corrdict = {sur: physio_feats['All'].corrwith(ema_feats[sur], method='pearson') for sur in surfeat}
corr_df = pd.DataFrame(corrdict)

# Scatter plot 
ax1 = mydf.plot.scatter(x='HR_Mean', y='restrict')

# Save results
surv.to_csv(use_participant + ' Survey Windows CENTERED.csv')
corr_df.to_csv(use_participant + ' Survey Correlations.csv')
mydf.to_csv(use_participant + ' Survey Window Summary.csv')
