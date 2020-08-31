import pandas as pd
import numpy as np
from datetime import timedelta, date
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
import pickle

def prepare_models():

    # Gather Data
    df_rj = pd.read_csv('rj_deaths_per_day.csv')
    df_sp = pd.read_csv('sp_deaths_per_day.csv')
    df_mg = pd.read_csv('mg_deaths_per_day.csv')

    print('Data gathered')

    # Wrangle Data
    df_rj.columns = ['UF','Full_Date','Date', 'Deaths']
    df_sp.columns = ['UF','Full_Date','Date', 'Deaths']
    df_mg.columns = ['UF','Full_Date','Date', 'Deaths']

    df_rj['Full_Date'] = pd.to_datetime(df_rj['Full_Date'])
    df_sp['Full_Date'] = pd.to_datetime(df_sp['Full_Date'])
    df_mg['Full_Date'] = pd.to_datetime(df_mg['Full_Date'])
    df_rj['Full_Date'] = df_rj['Full_Date'].dt.date
    df_sp['Full_Date'] = df_sp['Full_Date'].dt.date
    df_mg['Full_Date'] = df_mg['Full_Date'].dt.date

    # Feature Engineering
    df_rj_featured = df_rj.copy()
    df_sp_featured = df_sp.copy()
    df_mg_featured = df_mg.copy()

    print('Feature engineering done')

    # Create new columns
    df_rj_featured['Deaths_Yesterday'] = df_rj_featured['Deaths'].shift()
    df_rj_featured['Deaths_Diff'] = df_rj_featured['Deaths'].diff()
    df_rj_featured = df_rj_featured.dropna()

    df_sp_featured['Deaths_Yesterday'] = df_sp_featured['Deaths'].shift()
    df_sp_featured['Deaths_Diff'] = df_sp_featured['Deaths'].diff()
    df_sp_featured = df_sp_featured.dropna()

    df_mg_featured['Deaths_Yesterday'] = df_mg_featured['Deaths'].shift()
    df_mg_featured['Deaths_Diff'] = df_mg_featured['Deaths'].diff()
    df_mg_featured = df_mg_featured.dropna()

    days_range = df_rj_featured['Date'].tolist()
    days_range = days_range[:-1]
    days_range = np.asarray(days_range)

    # Train the models
    datasets = [df_rj_featured, df_sp_featured, df_mg_featured]

    for df in datasets: 
        
        X = df.drop(['Deaths', 'Full_Date','UF', 'Date'], axis=1)
        y = df['Deaths']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.1, random_state = 42)    
            
        df_uf = df['UF']
            
        if df_uf[1] == 'RJ':
            model_rj = RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=0)
            model_rj.fit(X_train, y_train)
            print('RJ model trained')
                
        if df_uf[1] == 'SP':
            model_sp = RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=0)
            model_sp.fit(X_train, y_train)
            print('SP model trained')
                
        if df_uf[1] == 'MG':
            model_mg = RandomForestRegressor(n_estimators=100, n_jobs=-1, random_state=0)
            model_mg.fit(X_train, y_train)
            print('MG model trained')
        

    # Export the models
    state = ['rj', 'sp', 'mg']

    for uf in state:
        file_name = ('model_'+uf+'.pkl')
        pickle_out = open(file_name, 'wb')

        if uf == 'rj':
            pickle.dump(model_rj, pickle_out)
            pickle_out.close()
            print('RJ pickle dumped')

        if uf == 'sp':
            pickle.dump(model_sp, pickle_out)
            pickle_out.close()
            print('SP pickle dumped')

        if uf == 'mg':
            pickle.dump(model_mg, pickle_out)
            pickle_out.close()
            print('MG pickle dumped')
    

def run_models(uf, param1, param2):
    file_name = ('model_' + uf + '.pkl')
    pickle_in = open(file_name, 'rb')
    model = pickle.load(pickle_in)

    p = model.predict([[param1, param2]])

    print('Prediction for {} using parameters {} and {} = {}'.format(uf, param1, param2, p))

    return p

#print(run_models('rj', 161, -24))
#print(run_models('mg', 107, -23))
#print(run_models('sp', 314, 38))



