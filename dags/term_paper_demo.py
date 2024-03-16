from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import joblib
from io import StringIO
from io import BytesIO

default_args = {
    'owner': 'caozhen',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'term_paper_demo_v2',
    default_args=default_args,
    description='demo for the term paper',
    start_date=datetime(2023, 3, 15),
    schedule_interval='@daily'
)


def query_stock_info(**context):
    tickers = ['AAPL']
    # Task 1: Query stock information from Yahoo Finance
    end_date = context['execution_date'].strftime('%Y-%m-%d')
    start_date = (context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    print(start_date)
    print(end_date)

    def get_stock(ticker, start_date, end_date, s_window, l_window):
        df = yf.download(ticker, start=start_date, end=end_date)
        df['Return'] = df['Adj Close'].pct_change()
        df['Return'].fillna(0, inplace=True)
        df['Date'] = df.index
        df['Date'] = pd.to_datetime(df['Date'])
        df['Month'] = df['Date'].dt.month
        df['Year'] = df['Date'].dt.year
        df['Day'] = df['Date'].dt.day
        for col in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
            df[col] = df[col].round(2)
        df['Weekday'] = df['Date'].dt.day_name()
        df['Week_Number'] = df['Date'].dt.strftime('%U')
        df['Year_Week'] = df['Date'].dt.strftime('%Y-%U')
        df['Short_MA'] = df['Adj Close'].rolling(window=s_window, min_periods=1).mean()
        df['Long_MA'] = df['Adj Close'].rolling(window=l_window, min_periods=1).mean()
        col_list = ['Date', 'Year', 'Month', 'Day', 'Weekday',
                    'Week_Number', 'Year_Week', 'Open',
                    'High', 'Low', 'Close', 'Volume', 'Adj Close',
                    'Return', 'Short_MA', 'Long_MA']
        num_lines = len(df)
        df = df[col_list]
        print('read ', num_lines, ' lines of data for ticker: ', ticker)
        return df

    def create_table_if_not_exist():
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_info (
                    Stock_Name VARCHAR(10),
                    Date DATE,
                    Year INT,
                    Month INT,
                    Day INT,
                    Weekday VARCHAR(20),
                    Week_Number VARCHAR(20),
                    Year_Week VARCHAR(20),
                    Open FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    Close FLOAT,
                    Volume BIGINT,
                    Adj_Close FLOAT,
                    Return FLOAT,
                    Short_MA FLOAT,
                    Long_MA FLOAT
                )
            """)
        return

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_if_not_exist()

    for ticker in tickers:
        df = get_stock(ticker, start_date=start_date, end_date=end_date, s_window=14, l_window=50)
        df.insert(0, 'Stock_Name', ticker)
        context['task_instance'].xcom_push(key='stock_data', value=df)

        csv_data = StringIO()
        df.to_csv(csv_data, sep='\t', index=False, header=False)
        csv_data.seek(0)

        # Check for duplicates before inserting data
        cursor.execute("""
            SELECT COUNT(*) FROM stock_info 
            WHERE Stock_Name = %s AND Date = ANY(%s)
        """, (ticker, df['Date'].tolist()))
        num_duplicates = cursor.fetchone()[0]

        if num_duplicates > 0:
            print(f"Skipping insertion of {num_duplicates} duplicate records.")
        else:
            cursor.copy_from(csv_data, 'stock_info', sep='\t', null='')
            print('Data inserted into PostgreSQL database successfully!')

    conn.commit()
    conn.close()

    print('Data inserted into PostgreSQL database successfully!')


def clean_and_process_data(**context):
    # Task 2: Clean and process the queried stock data
    df = context['task_instance'].xcom_pull(task_ids='query_stock_info', key='stock_data')
    if df.empty:
        print("DataFrame is empty. Exiting clean_and_process_data function.")
        return

    df = df[['Stock_Name', 'Date', 'Open', 'Close']]
    ticker = df['Stock_Name'].iloc[0]
    csv_data = StringIO()
    df.to_csv(csv_data, sep='\t', index=False, header=False)

    def create_table_if_not_exist():
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_stock_data (
                        Stock_Name VARCHAR(10),
                        Date DATE,
                        Open FLOAT,
                        Close FLOAT
                )
            """)
        return

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_if_not_exist()

    csv_data.seek(0)

    # Check for duplicates before inserting data
    cursor.execute("""
        SELECT COUNT(*) FROM processed_stock_data 
        WHERE Stock_Name = %s AND Date = ANY(%s)
    """, (ticker, df['Date'].tolist()))
    num_duplicates = cursor.fetchone()[0]

    if num_duplicates > 0:
        print(f"Skipping insertion of {num_duplicates} duplicate records.")
    else:
        cursor.copy_from(csv_data, 'processed_stock_data', sep='\t', null='')
        print('Data inserted into PostgreSQL database successfully!')

    conn.commit()
    conn.close()

    print('Filtered data inserted into PostgreSQL database successfully!')


def linear_regression_predictor():
    # Task 3: load model and predict
    pass


def logistic_regression_predictor():
    # Task 4: load model and predict
    tickers = ['AAPL']
    minio_conn_id = 'minio_conn'
    bucket_name = 'airflow'
    model_key = 'LogisticRegression.pkl'

    minio_hook = S3Hook(minio_conn_id)
    client = minio_hook.get_conn()

    # Use the client to download the object
    obj = client.get_object(Bucket=bucket_name, Key=model_key)

    # Read the object's body directly into a BytesIO buffer without decoding
    model_stream = BytesIO(obj['Body'].read())

    # Load the model from the BytesIO buffer
    loaded_model = joblib.load(model_stream)

    print(f"Model loaded from MinIO: {model_key}")


def LSTM_predictor():
    # Task 5: load model and predict
    pass


def make_investment_decision():
    # Task 6: Make investment decision based on Task 3 and Task 4
    # predict true when and only when Task 3 and Task 4 predict true
    pass


task1 = PythonOperator(
    task_id='query_stock_info',
    python_callable=query_stock_info,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='clean_and_process_data',
    python_callable=clean_and_process_data,
    provide_context=True,
    dag=dag,
)

model_sensor = S3KeySensor(
    task_id='check_models_in_minio',
    bucket_name='airflow',
    bucket_key=['LogisticRegression.pkl'],
    aws_conn_id='minio_conn',
    mode='poke',
    poke_interval=10,
    timeout=180,
    dag=dag,
)

task3 = PythonOperator(
    task_id='linear_regression_predictor',
    python_callable=linear_regression_predictor,
    dag=dag,
)

task4 = PythonOperator(
    task_id='logistic_regression_predictor',
    python_callable=logistic_regression_predictor,
    dag=dag,
)

task5 = PythonOperator(
    task_id='LSTM_predictor',
    python_callable=LSTM_predictor,
    dag=dag,
)

task6 = PythonOperator(
    task_id='make_investment_decision',
    python_callable=make_investment_decision,
    dag=dag,
)
task1 >> task2 >> model_sensor >> [task3, task4, task5] >> task6
