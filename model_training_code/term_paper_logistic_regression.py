import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import joblib

tickers = ['NVDA', 'GOOGL', 'AMZN']


def load_data_from_postgres(ticker_name):
    conn = psycopg2.connect(
        dbname="test",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM processed_stock_data WHERE stock_name = '{ticker_name}'")
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)

    conn.close()
    return df


def prepare_data(df):
    df['Price_Diff'] = df['close'] - df['open']
    df = df[['date', 'Price_Diff']].dropna()

    for i in range(1, 6):
        df[f'Price_Diff_{i}'] = df['Price_Diff'].shift(i)

    df.dropna(inplace=True)

    X = df.drop(['date', 'Price_Diff'], axis=1)
    y = (df['Price_Diff'] > 0).astype(int)

    return X, y


def train_logistic_regression(X, y, model_save_path):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    clf = LogisticRegression()
    clf.fit(X_train, y_train)

    # save the model
    joblib.dump(clf, model_save_path)
    print(f'Model saved to: {model_save_path}')

    y_pred = clf.predict(X_test)
    comparison_df = pd.DataFrame({'y_test': y_test, 'y_prediction': y_pred})
    print(comparison_df)


def train_logistic_regression_on_whole_dataset(X, y, model_save_path):
    clf = LogisticRegression()
    clf.fit(X, y)
    joblib.dump(clf, model_save_path)
    print(f'Model saved to: {model_save_path}')


for ticker in tickers:
    df = load_data_from_postgres(ticker)
    X, y = prepare_data(df)

    model_save_path = f'LogisticRegression_{ticker}.pkl'
    train_logistic_regression_on_whole_dataset(X, y, model_save_path)
