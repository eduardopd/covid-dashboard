from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
from os import environ
import pandas as pd 
import requests
import logging

# Modules import
from covid_api import uf_deaths_yesterday_diff, uf_deaths_per_day
from ml_model import prepare_models, run_models
import csv

# Google Cloud
import os
from google.cloud import storage

class DataGatherSave(BaseOperator):
    """
    Gather data from the API's transform, export to CSV.  
    """
    @apply_defaults
    def __init__(
        self,
        *args, **kwargs):
        super(DataGatherSave, self).__init__(*args, **kwargs)

    def __gatherData(self, context):
        
        states = ['rj', 'mg', 'sp']
        today = datetime.today()
        
        for uf in states:
        
            uf_selc, date_raw, date, deaths_per_day = uf_deaths_per_day(uf, 150)
            file_name = (uf + '_deaths_per_day.csv')

            with open(file_name, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(zip(uf_selc, date_raw, date, deaths_per_day))

    def execute(self, context):
        self.__gatherData(context)



class PrepareModels(BaseOperator):
    """
    Prepare the models.
    """
    @apply_defaults
    def __init__(
        self,
        bucket_name,
        *args, **kwargs):
        super(PrepareModels, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name

    def __prepareModels(self, context):
        prepare_models()

    def execute(self, context):
        self.__prepareModels(context)


class MakePredictions(BaseOperator):
    """
    Make predictions.
    """
    @apply_defaults
    def __init__(
        self,
        bucket_name,
        *args, **kwargs):
        super(MakePredictions, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name

    def __makePrediction(self, context):

        states = ['rj', 'mg', 'sp']
        today = datetime.today()

        for uf in states:
        
            param1, param2 = uf_deaths_yesterday_diff(uf, today)

            print(param1, param2, uf)
            
            prediction = run_models(uf, param1, param2)

            file_name = (uf + '_prediction.csv')

            with open(file_name, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(zip(uf, prediction))

                print("arquivo csv gerado para uf {}".format(uf))
                print(file_name)


    def execute(self, context):
        self.__makePrediction(context)

class SaveToStorage(BaseOperator):
    """
    Save CSV into Storage bucket
    """
    @apply_defaults
    def __init__(
            self,
            bucket_name,
            blob,
            file_path,
            *args, **kwargs):
            super(SaveToStorage, self).__init__(*args, **kwargs)
            self.bucket_name = bucket_name
            self.blob = blob
            self.file_path = file_path
    
    def __csv_to_storage(self,context):
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        blob = bucket.blob(self.blob)
        blob.upload_from_filename(self.file_path)

    def execute(self, context):
        self.__csv_to_storage(context)
