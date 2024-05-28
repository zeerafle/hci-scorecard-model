import csv
import sqlite3
import pandas as pd
import os


def create_table(name, dataframe):
    create_table_query = ""
    foreign_key = []
    for k, v in dataframe.dtypes.to_dict().items():
        if k == 'SK_ID_CURR':
            if name == 'application':
                create_table_query += f"{k} INTEGER PRIMARY KEY,\n"
            else:
                create_table_query += f"{k} INTEGER,\n"
                foreign_key.append('SK_ID_CURR')
        elif k == 'SK_ID_PREV':
            if name == 'previous_application':
                create_table_query += f"{k} INTEGER PRIMARY KEY,\n"
            else:
                create_table_query += f"{k} INTEGER,\n"
                foreign_key.append('SK_ID_PREV')
        elif k == 'SK_ID_BUREAU':
            if name == 'bureau':
                create_table_query += f"{k} INTEGER PRIMARY KEY,\n"
            else:
                create_table_query += f"{k} INTEGER,\n"
                foreign_key.append('SK_ID_BUREAU')
        elif v == 'object':
            create_table_query += f"{k} TEXT,\n"
        elif v == 'int64':
            create_table_query += f"{k} INTEGER,\n"
        elif v == 'float64':
            create_table_query += f"{k} REAL,\n"

    for key in foreign_key:
        if key == 'SK_ID_CURR' and name != 'application':
            create_table_query += f"FOREIGN KEY({key}) REFERENCES application({key}),\n"
        elif key == 'SK_ID_PREV' and name != 'previous_application':
            create_table_query += f"FOREIGN KEY({key}) REFERENCES previous_application({key}),\n"
        elif key == 'SK_ID_BUREAU' and name != 'bureau':
            create_table_query += f"FOREIGN KEY({key}) REFERENCES bureau({key}),\n"
    create_table_query = f"CREATE TABLE IF NOT EXISTS {name} ({create_table_query[:-2]});"
    print(create_table_query)
    cursor.execute(create_table_query)


# Connect to the SQLite database
conn = sqlite3.connect('hci_application.db')
cursor = conn.cursor()

excluded_files = ['HomeCredit_columns_description.csv', 'application_test.csv', 'sample_submission.csv']
primary_tables = ['application_train.csv', 'previous_application.csv', 'bureau.csv']
dataset_files = os.listdir('dataset')
for data in primary_tables + dataset_files:
    if data.endswith('.csv') and data not in excluded_files:
        table_name = data.split('.')[0] if not data.startswith('application') else 'application'
        df = pd.read_csv(f'dataset/{data}')
        # create table
        create_table(table_name, df)
        print(f"Table {table_name} created successfully")
        # insert data
        try:
            df.to_sql(table_name, conn, if_exists='fail', index=False)
        except ValueError as e:
            print('Table already exists')
        print(f"Data inserted into {table_name} successfully")
        # commit the transaction
        conn.commit()


# Close the connection
conn.close()
