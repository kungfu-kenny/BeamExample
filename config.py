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

temp_input = os.getenv('INPUT', '')
temp_output = os.getenv('OUTPUT', '')

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
project_id = os.getenv('PROJECT_ID')
dataset = os.getenv('DATASET')
table_id = os.getenv('TABLE_ID')

header_bool = bool(os.getenv("HEADER_NEW", 0))
header_test = [k.get("name", '') for k in schema.get('fields', [])] #TODO change here