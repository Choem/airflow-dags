from pathlib import Path

# Accepts a string as secret name
# Returns a dictionary of key value pairs containing the literals set in the k8s secret
def get_secret(secret_name):
    secrets_dir = Path('/opt/airflow/secrets')
    secret_path = secrets_dir / secret_name
    assert secret_path.exists(), f'could not find {secret_name} at {secret_path}'
    files = {}
    for entity in Path(secret_path).iterdir():
        if entity.is_file():
            pair = entity.read_text().strip().split('=')
            files[pair[0]] = pair[1]
    return files