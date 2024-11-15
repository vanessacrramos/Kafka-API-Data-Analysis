from kafka import KafkaConsumer
import json
import pandas as pd

all_responses = []

def analyze_data(df): 
    # get the minimum, maximum, last buy prices and average buy prices for each currency
    buy_prices = df.groupby('symbol')['buy'].agg(min_buy='min', max_buy='max',last_buy='last',mean_buy='mean')
    print(buy_prices)
    # save processed data to a csv file
    buy_prices.to_csv('buy_prices.csv', mode='a', header=not pd.io.common.file_exists('buy_prices.csv'))


consumer = KafkaConsumer(
    'topic_bitcoin_prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    data = message.value
    # Example Processing: Convert to DataFrame to perform analysis
    df = pd.DataFrame.from_dict(data, orient='index')
    all_responses.append(df)
    historical_data = pd.concat(all_responses, ignore_index=True)

    analyze_data(historical_data)
    # save the raw data to a csv file
    historical_data.to_csv('historical_data.csv', mode='a', header=not pd.io.common.file_exists('historical_data.csv'), index=False)