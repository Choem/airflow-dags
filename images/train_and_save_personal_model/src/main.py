import os
import tempfile
import urllib.request
import shutil
import glob
import pickle
import pandas as pd  
from datetime import datetime
from sklearn import datasets
from sklearn.svm import SVC


# Queries
get_patient_logs_query = gql("""
    query GetPatientLogs($patientId: Int!) {
        getPatientLogs(patientId: $patientId)
    }
""")

# Mutations
save_patient_model_mutation = gql("""
    mutation SavePatientModel($patientId: Int!, $file: Upload!) {
        savePatientModel(patientId: $patientId, file: $file)
    }
""")

# Errors
class DownloadLogsError(Exception):
    def __init__(self, message='Cannot download logs from bucket'):
        super().__init__(self.message)

class TrainModelError(Exception):
    def __init__(self, message='Something went wrong training the model'):
        super().__init__(self.message)

class SaveModelError(Exception):
    def __init__(self, message='Something went wrong saving the model'):
        super().__init__(self.message)

# Functions
def get_graphql_client():
    transport = RequestsHTTPTransport(
        url='http://file-service:4000/file/graphql/query', 
        use_json=True,
    )

    return Client(transport=transport, fetch_schema_from_transport=True)

def download_logs(patient_id, client):
    # Set params
    params = { "patientId": patient_id }

    # Execute query with params
    result = client.execute(get_patient_logs_query, variable_values=params)

    log_urls = result['getPatientLogs']

    # Create temp dir
    temp_dir_path = tempfile.TemporaryDirectory()

    # Iterate over log urls and download them in the temp dir
    for index, log_url in enumerate(log_urls):

        # Get file from log url and create a file in the temp dir
        with urllib.request.urlopen(log_url) as response, open(os.path.join(temp_dir_path, "log_%s.csv" % str(index)), 'wb') as created_file:

            # Copy content of response to created file
            shutil.copyfileobj(response, created_file)

    return temp_dir_path

def prepare_data(temp_dir_path):
    # Get all log files
    logs = glob.glob(os.path.join(temp_dir_path, "*.csv"))

    # Concate them into one dataframe
    df = pd.concat((pd.read_csv(log) for log in logs))

    return df

def train_model(df):
    raise TrainModelError()


def save_model(model, patient_id, client):
    # Set params
    params = {
        "upload": model, 
        "patientId": patient_id 
    }

    # Execute mutation with params
    result = client.execute(save_patient_model_mutation, variable_values=params, upload_files=True)

    if result['savePatientModel'] is False:
        raise SaveModelError()

def main():
    # Get patient ID from environment vars
    patient_id = os.environ['PATIENT_ID']
    
    # Get GraphQL client
    client = get_graphql_client()
    
    # # Download all logs of the patient
    # temp_dir_path = download_logs(patient_id, client)

    # # Prepare data
    # df = prepare_data(temp_dir_path):

    # Train model
    # model = train_model(df)
    iris = datasets.load_iris()
    print(iris.data[:3])
    clf = SVC()
    model = clf.fit(iris.data, iris.target)
    print(model)
    temp_dir_path = tempfile.TemporaryDirectory()
    print(temp_dir_path)
    pickle.dump(model, open(os.path.join(temp_dir_path, 'model_%s.sav' % patient_id), 'wb'))

    # Save model
    loaded_model = open(os.path.join(temp_dir_path, 'model_%s.sav' % patient_id), 'rb')
    save_model(model, patient_id, client)

main()