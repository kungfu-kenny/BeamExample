import os
from dotenv import load_dotenv


load_dotenv()

folder_current = os.getcwd()
folder_csv = 'csv'
file_csv = 'df_vote.csv'
file_current = os.path.join(folder_current, folder_csv, file_csv)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.path.join(
    os.getcwd(), 
    os.getenv('JSON_NAME')
)
schema = 'Sex:string, Age:string, Year:string, Month:string, Day:string,'\
        'Street_Number:string, Birthday:string,Name:string, Surname:string,'\
        'City:string, Street:string, State:string, State_Voted:string'

project_id = os.getenv('PROJECT_ID')
dataset = os.getenv('DATASET')
table_id = os.getenv('TABLE_ID')

temp_location = os.getenv("TEMP_LOCATION")
staging_location=os.getenv("STAGING_LOCATION")
output=os.getenv('OUTPUT')