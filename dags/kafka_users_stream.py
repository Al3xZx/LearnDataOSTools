import uuid

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    # print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    current_time = time.time()
    while True:
        if(time.time() > current_time + 60): #1 minute
            break
        try:
            res = format_data(get_data())
            encode = json.dumps(res).encode('utf-8')
            producer.send('users_created', encode)
            # log.info(encode)
        except Exception as e:
            log.error(f'An error occured: {e}')
            continue




def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data


# stream_data()

default_args = {
    'owner': 'Alessandro Molinaro',
    'start_date': airflow.utils.dates.days_ago(1)
}

with DAG('users_stream',
         default_args=default_args,
         schedule_interval=None) as dag:
    streaming_task = PythonOperator(task_id='stream_data_from_api', python_callable=stream_data)
