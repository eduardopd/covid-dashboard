# airflow DAG
from airflow import models
from airflow import DAG

from operators import SaveToStorage, DataGatherSave, MakePredictions, PrepareModels

# other packages
from datetime import datetime, timedelta

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    # set your start_date : airflow will run previous dags if dags
    # since startdate has not run
    # notify email is a python function that sends notification email upon failure    
    'start_date': datetime(2020, 8, 10, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'project_id' : 'covid-19',
    'retries': 4,
    'on_failure_callback': '',
    'retry_delay': timedelta(minutes=1),
}

with models.DAG(
    dag_id='dag_covid',
    # Continue to run DAG once per day
    schedule_interval = timedelta(days=3),
    catchup = True,
    default_args = default_dag_args) as dag:

    # Gather and save CSV
    task1 = DataGatherSave(
        task_id = 'fetch_from_API'
    )

    task2 = SaveToStorage(
        task_id = 'save_csv_rj',
        bucket_name = 'covid-01',
        blob = 'rj_deaths_per_day.csv',
        file_path = 'rj_deaths_per_day.csv'
    )

    task3 = SaveToStorage(
        task_id = 'save_csv_mg',
        bucket_name = 'covid-01',
        blob = 'mg_deaths_per_day.csv',
        file_path = 'mg_deaths_per_day.csv'
    )
    
    task4 = SaveToStorage(
        task_id = 'save_csv_sp',
        bucket_name = 'covid-01',
        blob = 'sp_deaths_per_day.csv',
        file_path = 'sp_deaths_per_day.csv'
    )

    # Model training per state.
    task5 = PrepareModels(
        task_id = 'prepare_models',
        bucket_name = 'covid-01',        
        )

    # Model exporting to bucket
    task6 = SaveToStorage(
        task_id = 'save_model_rj',
        bucket_name = 'covid-01',
        blob = 'model_rj.pkl',
        file_path = 'model_rj.pkl'
        )
    task7 = SaveToStorage(
        task_id = 'save_model_mg',
        bucket_name = 'covid-01',
        blob = 'model_mg.pkl',
        file_path = 'model_mg.pkl'
        )
    task8 = SaveToStorage(
        task_id = 'save_model_sp',
        bucket_name = 'covid-01',
        blob = 'model_sp.pkl',
        file_path = 'model_sp.pkl'
        )

    # Make predictions
    task9 = MakePredictions(
        task_id = 'make_predictions',
        bucket_name = 'covid-01'
        )

    # Prediction exporting to bucket
    task10 = SaveToStorage(
        task_id = 'save_pred_rj',
        bucket_name = 'covid-01',
        blob = 'rj_prediction.csv',
        file_path = 'rj_prediction.csv'
        )
    task11 = SaveToStorage(
        task_id = 'save_pred_mg',
        bucket_name = 'covid-01',
        blob = 'mg_prediction.csv',
        file_path = 'mg_prediction.csv'
        )
    task12 = SaveToStorage(
        task_id = 'save_pred_sp',
        bucket_name = 'covid-01',
        blob = 'sp_prediction.csv',
        file_path = 'sp_prediction.csv'
        )    

    
task1 >> [task2, task3, task4]
[task2, task3, task4] >> task5
task5 >> [task6, task7, task8]
[task6, task7, task8] >> task9
task9 >> [task10, task11, task12]
 
       
