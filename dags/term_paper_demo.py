from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
    'term_paper_demo_v1',
    default_args=default_args,
    description='demo for the term paper',
    start_date=datetime(2024, 3, 15),
    schedule_interval='@daily'
)


def query_stock_info(**context):
    ticker = 'AAPL'
    end_date = context['execution_date'].strftime('%Y-%m-%d')
    start_date = (context['execution_date'] - timedelta(days=1)).strftime('%Y-%m-%d')

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


def clean_and_process_data():
    # Task 2: Clean and process the queried stock data
    pass


def store_data_in_db():
    # Task 3: Store the processed data in the database
    pass


def make_investment_decision():
    # Task 4: Make investment decision based on the processed data
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
    dag=dag,
)

task3 = PythonOperator(
    task_id='store_data_in_db',
    python_callable=store_data_in_db,
    dag=dag,
)

task4 = PythonOperator(
    task_id='make_investment_decision',
    python_callable=make_investment_decision,
    dag=dag,
)

task1 >> task2 >> task3 >> task4
