import os
from mlflow import MlflowClient
from mlflow.server import get_app_client

# mlflow-server-svc.default.svc.cluster.local
# tracking_uri = "http://127.0.0.1:32050/"
tracking_uri = "http://mlflow-server-svc.default.svc.cluster.local:5000"

# ML Flow 사용자를 추가한다.
def create_user(login_id, login_pwd, user_id, user_pwd) :
  os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
  os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd

  auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)
  auth_client.create_user(username=user_id, password=user_pwd)

# ML Flow 사용자를 삭제한다.
def delete_user(login_id, login_pwd, user_id) :
  os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
  os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd

  auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)
  auth_client.delete_user(username=user_id)

# ML Flow 사용자에게 Experiment 사용권한 부여한다.
def apply_experiment_permission(login_id, login_pwd, experiment_name, user_id, permission) :
  # Permission      |  Can read | Can update | Can delete | Can manage
  # READ               Yes          No           No            No
  # EDIT               Yes          Yes          No            No
  # MANAGE             Yes          Yes          Yes           Yes
  # NO_PERMISSIONS     No           No           No            No
  os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
  os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd

  client = MlflowClient(tracking_uri=tracking_uri)
  auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)

  experiment_details = client.get_experiment_by_name(experiment_name)

  if experiment_details :
      experiment_id = experiment_details.experiment_id
  else :
      raise Exception(f'{experiment_name} does not exist.')

  auth_client.create_experiment_permission(experiment_id=experiment_id, username=user_id, permission=permission)

# ML Flow 사용자에게 Experiment 부여된 사용권한 취소한다.
def cancel_experiment_permission(login_id, login_pwd, experiment_name, user_id) :
  os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
  os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd

  client = MlflowClient(tracking_uri=tracking_uri)
  auth_client = get_app_client("basic-auth", tracking_uri=tracking_uri)

  experiment_details = client.get_experiment_by_name(experiment_name)

  if experiment_details :
      experiment_id = experiment_details.experiment_id
  else :
      raise Exception(f'{experiment_name} does not exist.')

  auth_client.delete_experiment_permission(experiment_id=experiment_id, username=user_id)


def delete_experiment(login_id, login_pwd, experiment_name) :
    os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
    os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd

    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_details = client.get_experiment_by_name(experiment_name)
    
    if experiment_details :
      experiment_id = experiment_details.experiment_id
    else :
      raise Exception(f'{experiment_name} does not exist.')

    client.delete_experiment(experiment_id)

def create_experiment(login_id, login_pwd, name : str, artifact_location : str, tags : dict = {} ) :
    os.environ['MLFLOW_TRACKING_USERNAME'] = login_id
    os.environ['MLFLOW_TRACKING_PASSWORD'] = login_pwd
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_id = client.create_experiment(
        name=name,
        artifact_location=artifact_location,
        tags=tags)
    return experiment_id
# def create_registered_model_permission(registered_model_name, username, permission) :

