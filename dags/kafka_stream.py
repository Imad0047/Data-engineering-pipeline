from datetime import datetime
from airflow import DAG
from kafka import KafkaProducer
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 0)
}

def get_data():
    import requests
    res = requests.get("https://api.tfl.gov.uk/BikePoint/")
    res = res.json()
    if isinstance(res, list) and len(res) > 0:
        return res
    return []


def format_data(res):
    data = {}

    data['id'] = res.get('id')
    data['name'] = res.get('commonName')
    data['latitude'] = res.get('lat')
    data['longitude'] = res.get('lon')

    props = {p['key']: p['value'] for p in res.get('additionalProperties', [])}

    data['num_bikes'] = int(props.get('NbBikes', 0))
    data['num_ebikes'] = int(props.get('NbEBikes', 0))
    data['num_standard_bikes'] = int(props.get('NbStandardBikes', 0))
    data['num_empty_docks'] = int(props.get('NbEmptyDocks', 0))
    data['num_docks'] = int(props.get('NbDocks', 0))
    data['installed'] = props.get('Installed', 'false') == 'true'
    data['locked'] = props.get('Locked', 'true') == 'true'
    data['temporary'] = props.get('Temporary', 'false') == 'true'

    return data




def stream_data():
    import json
    from kafka import KafkaProducer
    import time

    res = get_data()
    
   
    if not res:
        print("No data received from API")
        return
    
    
    

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ms = 5000)

    print(f"{len(res)} station trouvees . envoi en cours ... ")

    for station in res :
        formatted = format_data(station)
        producer.send('bikepoint_stream', json.dumps(formatted).encode('utf-8'))


    producer.flush()
    producer.close()
    print("✅ Envoi terminé avec succès !")
    

with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
     
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data 
    )
