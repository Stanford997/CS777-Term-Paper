from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceAsyncSensor
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import joblib
from io import StringIO
from io import BytesIO
from keras.models import load_model
import os

default_args = {
    'owner': 'caozhen',
    'retries': 0,
    'retry_delay': timedelta(seconds=3),
    'depends_on_past': True
}

# Define the DAG
dag = DAG(
    'term_paper_demo_gcloud',
    default_args=default_args,
    description='demo for the term paper',
    start_date=datetime(2024, 3, 10),
    schedule_interval='@daily'
)

# Stock name
tickers = ['NVDA', 'GOOGL', 'AMZN']


def query_stock_info(**context):
    """
    Task 1: Query stock information from Yahoo Finance
    """

    end_date = (context['execution_date'] + timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = context['execution_date'].strftime('%Y-%m-%d')

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
        print('\nread ', num_lines, ' lines of data for ticker: ', ticker)
        print()
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

    # Create a PostgresHook instance with the connection ID 'postgres_localhost'
    # You need to build up the connection in airflow UI first
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_if_not_exist()

    for ticker in tickers:
        df = get_stock(ticker, start_date=start_date, end_date=end_date, s_window=14, l_window=50)
        df.insert(0, 'Stock_Name', ticker)

        # Push the data to Xcom
        context['task_instance'].xcom_push(key=ticker, value=df)

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
            print(f"\nData already exists, skip insertion.\n")
        else:
            cursor.copy_from(csv_data, 'stock_info', sep='\t', null='')
            print('\nData inserted into PostgreSQL database successfully!\n')

    conn.commit()
    conn.close()


def processed_stock_data(**context):
    """
    Task 2: Save the projection of column ['Stock_Name', 'Date', 'Open', 'Close'] to table 'processed_stock_data'
    """

    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for ticker in tickers:
        # Pull the data from Xcom
        df = context['task_instance'].xcom_pull(task_ids='query_stock_info', key=ticker)
        if df.empty:
            print("\nDataFrame is empty. Exiting clean_and_process_data function.\n")
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

        create_table_if_not_exist()

        csv_data.seek(0)

        cursor.execute("""
            SELECT COUNT(*) FROM processed_stock_data 
            WHERE Stock_Name = %s AND Date = ANY(%s)
        """, (ticker, df['Date'].tolist()))
        num_duplicates = cursor.fetchone()[0]

        if num_duplicates > 0:
            print(f"\nData already exists, skip insertion.\n")
        else:
            cursor.copy_from(csv_data, 'processed_stock_data', sep='\t', null='')
            print('\nData inserted into PostgreSQL database successfully!\n')

    conn.commit()
    conn.close()


def load_recent_data_from_postgres(ticker, date):
    """
    This function reads 5 most recent days stock info from database.

    Args:
        ticker (String): stock name.

    Returns:
        df: dataframe of stock info.
    """
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    ten_days_ago = date - timedelta(days=10)

    # Fetch data for the closest 5 days within the 10 days range
    cursor.execute(f"""
        SELECT * 
        FROM processed_stock_data 
        WHERE Stock_Name = '{ticker}' 
        AND date <= '{date}' 
        AND date >= '{ten_days_ago}'
        ORDER BY ABS(date - '{date}') ASC
        LIMIT 5
    """)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    print()
    print(df)
    print()
    conn.close()
    return df


def linear_regression_predictor(**context):
    """
    Task 3: load linear regression model and predict
    """
    # You need to create the connection between minio and airflow using airflow UI
    # You also need to start the minio container
    google_cloud_conn_id = 'google_cloud_storage_default'
    bucket_name = 'term_paper_airflow'

    result_df = pd.DataFrame(columns=['ticker', 'predicted_labels'])
    date = context['execution_date']

    for ticker in tickers:
        model_key = f'LinearRegression_{ticker}.pkl'
        gcs_hook = GoogleCloudStorageHook(google_cloud_conn_id)

        # load data
        recent_data = load_recent_data_from_postgres(ticker, date)
        if len(recent_data) < 5:
            return -1
        recent_data['Price_Diff'] = recent_data['close'] - recent_data['open']
        X = recent_data['Price_Diff'].values.reshape(1, -1)

        # Download the model from Google Cloud Storage
        model_stream = gcs_hook.download(bucket_name=bucket_name, object_name=model_key, filename=f'/tmp/{model_key}')
        # Load the model
        loaded_model = joblib.load(model_stream)
        if os.path.exists(f'/tmp/{model_key}'):
            os.remove(f'/tmp/{model_key}')
        # predict
        predicted_labels = loaded_model.predict(X)[0]
        predicted_labels = 1 if predicted_labels > 0 else 0

        result_df = pd.concat([result_df, pd.DataFrame({'ticker': [ticker], 'predicted_labels': [predicted_labels]})],
                              ignore_index=True)
    print()
    print(result_df)
    print()
    context['task_instance'].xcom_push(key='stock_prediction_linear_regression', value=result_df)


def logistic_regression_predictor(**context):
    """
    Task 4: load logistic regression model and predict
    """
    google_cloud_conn_id = 'google_cloud_storage_default'
    bucket_name = 'term_paper_airflow'

    result_df = pd.DataFrame(columns=['ticker', 'predicted_labels'])
    date = context['execution_date']

    for ticker in tickers:
        model_key = f'LinearRegression_{ticker}.pkl'
        gcs_hook = GoogleCloudStorageHook(google_cloud_conn_id)

        # load data
        recent_data = load_recent_data_from_postgres(ticker, date)
        if len(recent_data) < 5:
            return -1
        recent_data['Price_Diff'] = recent_data['close'] - recent_data['open']
        X = recent_data['Price_Diff'].values.reshape(1, -1)

        # Download the model from Google Cloud Storage
        model_stream = gcs_hook.download(bucket_name=bucket_name, object_name=model_key, filename=f'/tmp/{model_key}')
        # Load the model
        loaded_model = joblib.load(model_stream)
        if os.path.exists(f'/tmp/{model_key}'):
            os.remove(f'/tmp/{model_key}')
        # predict
        predicted_labels = loaded_model.predict(X)[0]

        result_df = pd.concat([result_df, pd.DataFrame({'ticker': [ticker], 'predicted_labels': [predicted_labels]})],
                              ignore_index=True)
    print()
    print(result_df)
    print()
    context['task_instance'].xcom_push(key='stock_prediction_logistic_regression', value=result_df)


def LSTM_predictor(**context):
    """
    Task 5: load LSTM model and predict
    """
    google_cloud_conn_id = 'google_cloud_storage_default'
    bucket_name = 'term_paper_airflow'

    result_df = pd.DataFrame(columns=['ticker', 'predicted_labels'])
    date = context['execution_date']

    for ticker in tickers:
        # load data
        recent_data = load_recent_data_from_postgres(ticker, date)
        if len(recent_data) < 5:
            return -1
        recent_data['Price_Diff'] = recent_data['close'] - recent_data['open']
        X = recent_data['Price_Diff'].values.reshape(1, -1)

        model_key = f'LSTM_{ticker}.h5'
        gcs_hook = GoogleCloudStorageHook(google_cloud_conn_id)
        # First download .h5 model file from minio and then load model
        model_stream = gcs_hook.download(bucket_name=bucket_name, object_name=model_key, filename=f'/tmp/{model_key}')
        loaded_model = load_model(model_stream)
        if os.path.exists(f'/tmp/{model_key}'):
            os.remove(f'/tmp/{model_key}')
        # predict
        predicted_labels = loaded_model.predict(X)[0]
        predicted_labels = 1 if predicted_labels > 0 else 0

        result_df = pd.concat([result_df, pd.DataFrame({'ticker': [ticker], 'predicted_labels': [predicted_labels]})],
                              ignore_index=True)
    print()
    print(result_df)
    print()
    context['task_instance'].xcom_push(key='stock_prediction_LSTM', value=result_df)


def make_investment_decision(**context):
    """
    Task 6: Make investment decision based on Task 3,4,5
    """
    # Check if Task 3，4，5 has returned -1
    task3_status = context['ti'].xcom_pull(task_ids='linear_regression_predictor')
    task4_status = context['ti'].xcom_pull(task_ids='logistic_regression_predictor')
    task5_status = context['ti'].xcom_pull(task_ids='LSTM_predictor')
    if task3_status == -1 or task4_status == -1 or task5_status == -1:
        print("\nTask 3, Task 4, or Task 5 returned -1. Exiting task.\n")
        return -1

    def create_table_if_not_exist():
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prediction (
                        Date DATE,
                        Stock_Name VARCHAR(10),
                        Prediction VARCHAR(10)
                )
            """)
        return

    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_if_not_exist()

    for ticker in tickers:
        date = (context['execution_date'] + timedelta(days=1)).strftime('%Y-%m-%d')
        df_linear_regression = context['task_instance'].xcom_pull(task_ids='linear_regression_predictor',
                                                                  key='stock_prediction_linear_regression')
        df_logistic_regression = context['task_instance'].xcom_pull(task_ids='logistic_regression_predictor',
                                                                    key='stock_prediction_logistic_regression')
        df_LSTM = context['task_instance'].xcom_pull(task_ids='LSTM_predictor', key='stock_prediction_LSTM')
        dfs = [df_linear_regression, df_logistic_regression, df_LSTM]
        count = 0
        # Only if the prediction results of more than or equal to two models are good stocks,
        # the final result is a good stock.
        for df in dfs:
            predicted_labels = df[df['ticker'] == ticker]['predicted_labels'].astype(int)
            count += predicted_labels.sum()
        final_result = 'Yes' if count >= 2 else 'No'
        predict_df = pd.DataFrame(columns=['date', 'ticker', 'predicted_labels'])
        predict_df = pd.concat([predict_df, pd.DataFrame(
            {'date': date, 'ticker': [ticker],
             'predicted_labels': [final_result]})],
                               ignore_index=True)
        print()
        print(predict_df)
        print()
        csv_data = StringIO()
        predict_df.to_csv(csv_data, sep='\t', index=False, header=False)
        csv_data.seek(0)

        cursor.execute("""
            SELECT COUNT(*) FROM stock_prediction
            WHERE  Stock_Name = %s AND Date = %s
        """, (ticker, date))

        num_duplicates = cursor.fetchone()[0]
        if num_duplicates > 0:
            print(f"\nResult already exists, skip insertion.\n")
        else:
            cursor.copy_from(csv_data, 'stock_prediction', sep='\t', null='')
            print('\nResult inserted into PostgreSQL database successfully!\n')

    conn.commit()
    conn.close()


task1 = PythonOperator(
    task_id='query_stock_info',
    python_callable=query_stock_info,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='processed_stock_data',
    python_callable=processed_stock_data,
    provide_context=True,
    dag=dag,
)

# model_sensor = GCSObjectExistenceAsyncSensor(
#     task_id='check_models_in_gcs',
#     google_cloud_conn_id='google_cloud_storage_default',
#     bucket='term_paper_airflow',
#     object=[f'LinearRegression_{ticker}.pkl' for ticker in tickers] +
#            [f'LogisticRegression_{ticker}.pkl' for ticker in tickers] +
#            [f'LSTM_{ticker}.h5' for ticker in tickers],
#     timeout=5,
#     poke_interval=3,
#     mode='poke',
#     dag=dag,
# )

task3 = PythonOperator(
    task_id='linear_regression_predictor',
    python_callable=linear_regression_predictor,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='logistic_regression_predictor',
    python_callable=logistic_regression_predictor,
    provide_context=True,
    dag=dag,
)

task5 = PythonOperator(
    task_id='LSTM_predictor',
    python_callable=LSTM_predictor,
    provide_context=True,
    dag=dag,
)

task6 = PythonOperator(
    task_id='make_investment_decision',
    python_callable=make_investment_decision,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> [task3, task4, task5] >> task6
