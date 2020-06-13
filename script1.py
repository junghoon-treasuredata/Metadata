import sys
import os
import os.path
import requests

os.system(f"{sys.executable} -m pip install -U pytd==1.0.0")

import pytd
import pandas as pd
from collections import namedtuple

TD_APIKEY = os.environ.get("td_apikey")
TD_APIKEY_TD1 = "TD1 " + TD_APIKEY
TD_AUTH = {'Authorization': TD_APIKEY_TD1}
TD_API_ENDPOINT = os.environ.get("td_api_endpoint")
TD_WORKFLOW_ENDPOINT = os.environ.get("td_workflow_endpoint")
TD_DATABASE = os.environ.get("td_database")
TABLE_DATABASE_TABLE_USAGE = os.environ.get("table_database_table_usage")
TABLE_USER_LIST = os.environ.get("table_user_list")
TABLE_DATABASE_PERMISSIONS = os.environ.get("table_database_permissions")
TABLE_WORKFLOW_LIST = os.environ.get("table_workflow_list")
TABLE_POLICIES = os.environ.get("table_policies")
TABLE_USER_PERMISSIONS = os.environ.get("table_users_permissions")
TABLE_USER_POLICIES = os.environ.get("table_users_policies")

def uploadDataToTD(dataframe, dest_db, dest_table):
  client = pytd.Client(
    apikey=TD_APIKEY,
    endpoint=TD_API_ENDPOINT,
    database=dest_db)
      
  client.load_table_from_dataframe(
    dataframe
    , dest_table
    , writer='bulk_import'
    , if_exists='overwrite'
    )

def append_table_info(df, db, page):
  response = requests.get(url = TD_API_ENDPOINT+page, headers = TD_AUTH) 
  json_result = response.json()
  print(len(json_result['tables']), "tables")
  for table in json_result["tables"]:
    tmp_table = pd.Series([db.id, db.name, db.owner_id, db.owner_name, table["id"], table["name"],table["num_records"],table["storage_size"]], index=df.columns)
    df = df.append(tmp_table, ignore_index=True)
  
  has_next_page = json_result['pagination']['has_next_page']
  next_page = ""
  if has_next_page:
    next_page = json_result['pagination']['next_page']
    print("next table list page")
  return df, next_page

def database_table_usage():
  df = pd.DataFrame(columns=["db_id", "db_name", "owner_id","owner_name", "table_id", "table_name", "table_num_records", "table_storage_size"])

  response = requests.get(url = TD_API_ENDPOINT+"/v4/databases", headers = TD_AUTH)
  json_result = response.json()
  iLen = len(json_result)
  iCnt=1
  
  NT_DB = namedtuple("NT_DB", "id, name, owner_id, owner_name")
  
  for db in json_result:
    owner_id = ''
    owner_name = ''
    if db.get("owner"):
      owner_id = db["owner"]["id"]
      owner_name = db["owner"]["name"]
    nt_db = NT_DB(db["id"], db["name"], owner_id, owner_name)\
    
    page = "/v4/databases/"+ db["id"] +"/tables?page_size=100"
    while page != "":
      df, page = append_table_info(df, nt_db, page)

    print("database : ", iCnt, "/" ,iLen)
    iCnt+=1

  return df

def users():
  response = requests.get(url = TD_API_ENDPOINT+"/v4/users", headers = TD_AUTH) 
  json_result = response.json()
  len(json_result)
  df = pd.DataFrame(json_result)
  return df

def database_permissions():
  df = pd.DataFrame(columns=["db_id", "db_name", "user_id","user_name", "permission_level"])

  response = requests.get(url = TD_API_ENDPOINT+"/v4/databases", headers = TD_AUTH)
  json_result = response.json()
  iLen = len(json_result)
  iCnt = 1

  for db in json_result:
    response2 = requests.get(url = TD_API_ENDPOINT+"/v4/database_permissions?database_id="+ db['id'], headers = TD_AUTH) 
    json_result2 = response2.json()
    for db2 in json_result2:
      tmp_table = pd.Series([db2['database']['id']
                            ,db2['database']['name']
                            ,db2['user']['id']
                            ,db2['user']['name']
                            ,db2['permission_level']]
                            ,index=df.columns)
      df = df.append(tmp_table, ignore_index=True)
    print("database_permissions : ", iCnt, "/" ,iLen)
    iCnt+=1
  return df


def get_last_revision(project_id):
  json_result = requests.get(url = TD_WORKFLOW_ENDPOINT + '/api/projects/' + str(project_id) + '/revisions', headers = TD_AUTH).json()
  return json_result['revisions'][0]

def get_schedule(workflow):
  schedule = ''
  if workflow.get("config"):
    if workflow.get("config").get("schedule"):
      schedule = workflow.get("config").get("schedule")
  return schedule

def workflows():
  iMaxLen = 10000
  json_result = requests.get(url = TD_WORKFLOW_ENDPOINT + '/api/workflows?count=' + str(iMaxLen), headers = TD_AUTH).json() 
  # json_result['revisions'][0]
  iLen = len(json_result['workflows'])
  if iLen == iMaxLen :
    print("need to increase iMaxLen more than " + str(iMaxLen))
    sys.exit(os.EX_DATAERR)

  df = pd.DataFrame(columns=["workflow_id", "workflow_name", "project_id", "project_name" , "schedule"
                           , "last_create_time", "last_user_id","last_user_name","last_user_email"])
  iCnt =1
  for workflow in json_result['workflows']:
    schedule = get_schedule(workflow)
    last_revision = get_last_revision(workflow['project']['id'])
    tmp_table = pd.Series([workflow['id']
                          , workflow['name']
                          , workflow['project']['id']
                          , workflow['project']['name']
                          , schedule
                          , last_revision['createdAt']
                          , last_revision['userInfo']['td']['user']['id']
                          , last_revision['userInfo']['td']['user']['name']
                          , last_revision['userInfo']['td']['user']['email']]
                          , index=df.columns)
    df = df.append(tmp_table, ignore_index=True)
    print("workflow : ", iCnt, "/" ,iLen)
    iCnt+=1
  return df

def access_control_policies():
  df = pd.DataFrame(columns=["policy_id", "policy_name", "description", "user_count"
                             , "WorkflowProject" , "WorkflowProjectLevel"
                             , "Segmentation", "MasterSegmentConfigs","MasterSegmentConfig"
                             , "SegmentAllFolders", "SegmentFolder", "Authentications"
                             , "Sources", "Integrations", "Destinations"
                            ])

  json_result = requests.get(url = TD_API_ENDPOINT + '/v4/access_control/policies', headers = TD_AUTH).json() 
  iLen = len(json_result)
  iCnt =1
  for policy in json_result:
    json_result2 = requests.get(url = TD_API_ENDPOINT + '/v4/access_control/policies/'+policy['id']+'/permissions', headers = TD_AUTH).json()
    tmp_table = pd.Series([policy['id']
                           , policy['name']
                           , policy['description']
                           , policy['user_count']
                           , json_result2.get('WorkflowProject')
                           , json_result2.get('WorkflowProjectLevel')
                           , json_result2.get('Segmentation')
                           , json_result2.get('MasterSegmentConfigs')
                           , json_result2.get('MasterSegmentConfig')
                           , json_result2.get('SegmentAllFolders')
                           , json_result2.get('SegmentFolder')
                           , json_result2.get('Authentications')
                           , json_result2.get('Sources')
                           , json_result2.get('Integrations')
                           , json_result2.get('Destinations')
                          ], index=df.columns)

    df = df.append(tmp_table, ignore_index=True)
    print("policy : ", iCnt, "/" ,iLen)
    iCnt=iCnt+1
  return df

def access_control_users_permission():
  json_result = requests.get(url = TD_API_ENDPOINT+"/v4/access_control/users", headers = TD_AUTH).json() 

  df = pd.DataFrame(columns=["user_id", "workflowproject"
                             , "workflowprojectlevel", "segmentation"
                             , "mastersegmentconfigs", "mastersegmentconfig"
                             , "segmentallfolders", "segmentfolder"
                             , "authentications", "sources"
                             , "integrations", "destinations"
                            ])

  for user in json_result:
    tmp_table = pd.Series([user['user_id']
                           , user.get('permissions').get('WorkflowProject')
                           , user.get('permissions').get('WorkflowProjectLevel')
                           , user.get('permissions').get('Segmentation')
                           , user.get('permissions').get('MasterSegmentConfigs')
                           , user.get('permissions').get('MasterSegmentConfig')
                           , user.get('permissions').get('SegmentAllFolders')
                           , user.get('permissions').get('SegmentFolder')
                           , user.get('permissions').get('Authentications')
                           , user.get('permissions').get('Sources')
                           , user.get('permissions').get('Integrations')
                           , user.get('permissions').get('Destinations')
                          ], index=df.columns)

    df = df.append(tmp_table, ignore_index=True)
  return df

def access_control_users_policies():
  json_result = requests.get(url = TD_API_ENDPOINT+"/v4/access_control/users", headers = TD_AUTH).json() 

  df = pd.DataFrame(columns=["user_id", "policy_id", 'policy_name'])

  for user in json_result:
    for policy in user['policies']:
  #         print(user['user_id'], policy['id'], policy['name'])
      tmp_table = pd.Series([user['user_id'], policy['id'], policy['name']], index=df.columns)

      df = df.append(tmp_table, ignore_index=True)
  return df

def main():
  print("1. database_table_usage")
  df_result = database_table_usage()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_DATABASE_TABLE_USAGE)
  
  print("2. users")
  df_result = users()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_USER_LIST)
  
  print("3. database_permissions")
  df_result = database_permissions()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_DATABASE_PERMISSIONS)

  print("4. workflows")
  df_result = workflows()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_WORKFLOW_LIST)

  print("5. access_control_policies")
  df_result = access_control_policies()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_POLICIES)

  print("6. access_control_users_permission")
  df_result = access_control_users_permission()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_USER_PERMISSIONS)

  print("7. access_control_users_policies")
  df_result = access_control_users_policies()
  uploadDataToTD(df_result, TD_DATABASE, TABLE_USER_POLICIES)

  
  
  