import os
import tempfile
import urllib.request
import shutil
import glob
import pickle
import pandas as pd  
import datetime
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from minio import Minio

# Test
from sklearn.linear_model import LogisticRegression
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split

# Queries
get_patient_logs_query = gql("""
    query GetPatientLogs($patientId: Int!) {
        getPatientLogs(patientId: $patientId)
    }
""")

# Errors
class DownloadLogsError(Exception):
    def __init__(self, message):
        super().__init__(self.message)

class TrainModelError(Exception):
    def __init__(self):
        super().__init__(self.message)

class SaveModelError(Exception):
    def __init__(self, message):
        super().__init__(self.message)

# Functions
def get_graphql_client():
    transport = RequestsHTTPTransport(
        url='http://file-service:4000/file/graphql/query', 
        use_json=True
    )

    return Client(transport=transport, fetch_schema_from_transport=True)

def get_minio_client():
    return Minio(
        "minio:9000",
        access_key=os.environ['MINIO_ACCESS_KEY'],
        secret_key=os.environ['MINIO_SECRET_KEY'],
    )

def download_logs(patient_id, graphql_client, minio_client):
    # Set params
    params = { "patientId": patient_id }

    # Execute query with params
    result = graphql_client.execute(get_patient_logs_query, variable_values=params)

    logs = result['getPatientLogs']

    # Create temp dir
    os.mkdir('logs')

    # Iterate over log urls and download them in the temp dir
    for index, log in enumerate(logs):

        # Get files from Minio
        minio_client.fget_object('user-%s/logs' % str(patient_id), log, "logs/log_%s.csv" % str(index))

def prepare_data():
    # Get all log files
    logs = glob.glob(os.path.join('logs', "*.csv"))

    # Concate them into one dataframe
    df = pd.concat((pd.read_csv(log) for log in logs))

    return df

def train_model(patient_id):
    try:
        iris = load_iris()
        
        Xtrain, Xtest, Ytrain, Ytest = train_test_split(iris.data, iris.target, test_size=0.3, random_state=4)
        
        model = LogisticRegression(
            C=0.1, 
            max_iter=20, 
            fit_intercept=True, 
            n_jobs=3, 
            solver='liblinear'
        )
        model.fit(Xtrain, Ytrain)
        
        os.mkdir('model')
        with open(os.path.join('model', 'model_%s.pkl' % patient_id), 'wb') as file:
            pickle.dump(model, file)

        print(os.listdir('model'))
    except:
        raise TrainModelError('Something went wrong training the model')


def save_model(patient_id, minio_client):
    try:
        minio_client.fput_object(
            "user-%s/models" % str(patient_id), "model-%s.pkl" % str(datetime.date.today()), "/model/model-%s.pkl" % str(datetime.date.today())
        )
        # with open(os.path.join('model', 'model_%s.pkl' % patient_id), 'rb') as file:
            
    except:
        raise SaveModelError('Something went wrong saving the model')

def main():
    # Get environment vars
    patient_id = os.environ['PATIENT_ID']

    # Get GraphQL client
    graphql_client = get_graphql_client()

    # Get Minio client
    minio_client = get_minio_client()
    
    # Download all logs of the patient
    # temp_dir_path = download_logs(patient_id, client)

    # Prepare data
    # df = prepare_data():

    # Train model
    model = train_model(patient_id)

    # Save model
    save_model(patient_id, minio_client)

main()