from pandas import read_csv, read_sql_query
import sqlite3
import requests
from io import StringIO
import os

DATA_URL = 'https://raw.githubusercontent.com/4GeeksAcademy/decision-tree-project-tutorial/main/diabetes.csv'

relative_paths = {
    'raw_data' : os.path.join('data', 'raw', 'diabetes_data'),
}

paths = {
    'current_dir' : os.path.dirname(os.path.abspath(__file__)),
    'root_dir' : os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
}

def csv_to_db_file(csv_data:str):
    diabetes_db_path = os.path.join(paths['root_dir'], relative_paths['raw_data']+'.db')
    create_db()
    with sqlite3.connect(diabetes_db_path) as connection:
        try:
            df = read_csv(StringIO(csv_data))
            df.to_sql('diabetes_data', connection, if_exists='replace')
        except ValueError as e:
            print(f'An error occurred while adding the table to the database, this is likely because the database already contains that table:\n    {e}')

def load_diabetes_db():
    diabetes_db_path = os.path.join(paths['root_dir'], relative_paths['raw_data']+'.db')
    with sqlite3.connect(diabetes_db_path) as connection:
        try:
            query = 'SELECT * FROM diabetes_data'
            df = read_sql_query(query, connection, index_col='index')
            return df
        except Exception as e:
            print(f'An error occurred while reading the database: {e}')


def save_raw_csv(data:str):
    diabetes_csv_path = os.path.join(paths['root_dir'], relative_paths['raw_data']+'.csv')
    with open(path, 'w') as f:
        f.write(data)
    path = os.path.join(paths['root_dir'], relative_paths['raw_data']+'.csv')

def fetch_data() -> str:   
    try:
        with requests.get(DATA_URL, timeout = 20) as response:
            response.raise_for_status()
            data = response.text
            return data
            
    except requests.exceptions.HTTPError as e:
        print(f'Error while fetching the data from {DATA_URL[50]}...: {e}')
    
def create_db():
    diabetes_db_path = os.path.join(paths['root_dir'], relative_paths['raw_data']+'.db')
    with sqlite3.connect(diabetes_db_path) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS diabetes_data(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Pregnancies INT,
            Glucose INT,
            BloodPressure INT,
            SkinThickness INT,
            Insulin INT,
            BMI DECIMAL(3,1),
            DiabetesPedigreeFunction DECIMAL(4,3),
            Age INT,
            Outcome BOOL
            );
            """
        )

if __name__ == "__main__":
    csv_data = fetch_data()
    save_raw_csv(csv_data)
    csv_to_db_file(csv_data)
    
