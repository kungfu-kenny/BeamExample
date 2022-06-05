import os
from dotenv import load_dotenv


load_dotenv()

folder_current = os.getcwd()
file_current = os.path.join(
    folder_current, 
    'csv', 
    'df_vote.json' # 'df_vote.csv'
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=os.path.join(
    os.getcwd(), 
    os.getenv('JSON_NAME')
)

temp_input = os.getenv('INPUT', '')
project_id = os.getenv('PROJECT_ID', '')
dataset = os.getenv('DATASET', '')
table_id = os.getenv('TABLE_ID', '')
bucket = os.getenv('BUCKET', '')

schema = {
    'fields': [
        {
            'name': 'Sex', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Age', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Year', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Month', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Day', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Street_Number', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Birthday', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Name', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Surname', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'City', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'Street', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'
        }, 
        {
            'name': 'State_Voted', 'type': 'STRING', 'mode': 'NULLABLE'
        }
    ]
}