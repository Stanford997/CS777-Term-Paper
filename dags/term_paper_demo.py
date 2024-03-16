from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from io import StringIO

default_args = {
    'owner': 'caozhen',
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    'term_paper_demo_v2',
    default_args=default_args,
    description='demo for the term paper',
    start_date=datetime(2024, 3, 10),
    schedule_interval='@daily'
)


def query_stock_info(**context):
    # Task 1: Query stock information from Yahoo Finance
    ticker = 'AAPL'
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

    def check_and_create_table(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'stock_info')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            cursor.execute("""
                    CREATE TABLE stock_info (
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
            conn.commit()
            print('Table "stock_info" created successfully!')
        else:
            print('Table "stock_info" already exists.')

    df = get_stock(ticker, start_date=start_date, end_date=end_date, s_window=14, l_window=50)
    context['task_instance'].xcom_push(key='stock_data', value=df)

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    check_and_create_table(conn)

    csv_data = StringIO()
    df.to_csv(csv_data, sep='\t', index=False, header=False)
    csv_data.seek(0)

    cursor.execute("""
            CREATE TEMP TABLE temp_table (
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

    cursor.copy_from(csv_data, 'temp_table', sep='\t', null='')
    cursor.execute("""
            INSERT INTO stock_info (Date, Year, Month, Day, Weekday, Week_Number, Year_Week, Open, High, Low, Close, Volume, Adj_Close, Return, Short_MA, Long_MA)
            SELECT Date, Year, Month, Day, Weekday, Week_Number, Year_Week, Open, High, Low, Close, Volume, Adj_Close, Return, Short_MA, Long_MA
            FROM temp_table
        """)

    conn.commit()
    conn.close()

    print('Data inserted into PostgreSQL database successfully!')


def clean_and_process_data(**context):
    # Task 2: Clean and process the queried stock data
    df = context['task_instance'].xcom_pull(task_ids='query_stock_info', key='stock_data')
    csv_data = StringIO()
    df.to_csv(csv_data, sep='\t', index=False, header=False)
    df = df[['Date', 'Open', 'Close']]

    def check_and_create_table(conn):
        cursor = conn.cursor()
        cursor.execute(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'processed_stock_data')")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            cursor.execute("""
                    CREATE TABLE processed_stock_data (
                        Date DATE,
                        Open FLOAT,
                        Close FLOAT
                    )
                """)
            conn.commit()
            print('Table "processed_stock_data" created successfully!')
        else:
            print('Table "processed_stock_data" already exists.')

    hook = PostgresHook(postgres_conn_id='postgres_localhost')
    conn = hook.get_conn()
    cursor = conn.cursor()

    check_and_create_table(conn)

    cursor.execute("""
        CREATE TEMP TABLE temp_table (
            Date DATE,
            Open FLOAT,
            Close FLOAT
        )
    """)

    csv_data = StringIO()
    df.to_csv(csv_data, sep='\t', index=False, header=False)
    csv_data.seek(0)
    cursor.copy_from(csv_data, 'temp_table', sep='\t', null='')

    cursor.execute("""
        INSERT INTO processed_stock_data (Date, Open, Close)
        SELECT Date, Open, Close
        FROM temp_table
    """)

    conn.commit()
    conn.close()

    print('Filtered data inserted into PostgreSQL database successfully!')


def linear_regression_predictor():
    # Task 3: load model and predict
    pass


def LSTM_predictor():
    # Task 4: load model and predict
    pass


def make_investment_decision():
    # Task 5: Make investment decision based on Task 3 and Task 4
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

task3 = PythonOperator(
    task_id='linear_regression_predictor',
    python_callable=linear_regression_predictor,
    dag=dag,
)

task4 = PythonOperator(
    task_id='LSTM_predictor',
    python_callable=LSTM_predictor,
    dag=dag,
)

task5 = PythonOperator(
    task_id='make_investment_decision',
    python_callable=make_investment_decision,
    dag=dag,
)
task1 >> task2 >> [task3, task4] >> task5
