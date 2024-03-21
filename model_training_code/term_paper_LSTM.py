import psycopg2
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from keras.models import Sequential
from keras.layers import LSTM, Dense

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


def build_sample_sequences(data, sequence_length=6):
    sequences = []
    for i in range(sequence_length, len(data)):
        sequence = data[i - sequence_length:i]['close'] - data[i - sequence_length:i]['open']
        sequences.append(sequence.values)
    return sequences


def split_features_labels(sequences):
    X = np.array([sequence[:-1] for sequence in sequences])
    y = np.array([sequence[-1] for sequence in sequences])
    return X, y


def reshape_for_lstm(X, y):
    X = np.reshape(X, (X.shape[0], X.shape[1], 1))
    y = np.reshape(y, (y.shape[0], 1))
    return X, y


def train_LSTM(X, y, model_save_path):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    X_train, y_train = reshape_for_lstm(X_train, y_train)
    X_test, y_test = reshape_for_lstm(X_test, y_test)

    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], 1)))
    model.add(LSTM(units=50))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X_train, y_train, epochs=100, batch_size=32, validation_data=(X_test, y_test))
    model.save(model_save_path)


def train_LSTM_on_whole_dataset(X, y, model_save_path):
    X, y = reshape_for_lstm(X, y)

    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(X.shape[1], 1)))
    model.add(LSTM(units=50))
    model.add(Dense(units=1))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X, y, epochs=200, batch_size=32)
    model.save(model_save_path)


for ticker in tickers:
    df = load_data_from_postgres(ticker)
    sequences = build_sample_sequences(df)
    X, y = split_features_labels(sequences)

    model_save_path = f'LSTM_{ticker}.h5'
    train_LSTM_on_whole_dataset(X, y, model_save_path)
