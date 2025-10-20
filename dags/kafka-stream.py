from datetime import datetime
# DAG (Directed Acyclic Graph) is a collection of all the tasks you want to run, defines the workflow and their execution order
from airflow import DAG
#Imports the PythonOperator, which allows you to run a Python function as a task in your Airflow DAG.
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 10, 9,10,00)}

def get_data():
     import requests

     res = requests.get('https://randomuser.me/api/')
     res = res.json()
     res = res['results'][0]
     return res

def format_data(res):
     data={}
     data['first_name'] = res['name']['first']
     data['last_name'] = res['name']['last']    
     data['gender'] = res['gender']
     data['adress'] = str(res['location']['street']['number']) + " " + res['location']['street']['name'] + ", " + res['location']['city'] + ", " + res['location']['state'] + ", " + res['location']['country'] + ", " + str(res['location']['postcode'])     
     data['email'] = res['email']
     data['username'] = res['login']['username']
     data['dob'] = res['dob']['date']
     data['registered'] = res['registered']['date']
     data['phone'] = res['phone']
     data['picture'] = res['picture']['medium']
     return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    #res = get_data()

    #res = format_data(res)
    #print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],max_block_ms=5000)
    curr_time = time.time()
    while True:
         if time.time() > curr_time + 60:
              break
         try:     
              res = get_data()

              res = format_data(res)    

              producer.send('users_created', json.dumps(res).encode('utf-8'))
         except Exception as e:
              logging.error(f"Error sending data to Kafka: {e}")
              continue


   

# Define the DAG
#catchup=False means that if the DAG is started after its scheduled start date, it won't try to "catch up" and run all the missed intervals; it will only run from the current date onward.
#user_automation = le DAG qui orchestre la collecte et l’envoi des utilisateurs aléatoires vers Kafka.

#Son rôle = automatiser le flux de données sans que tu aies à lancer Python manuellement.

#Airflow gère la planification, l’exécution et le suivi.
with DAG('user_automation',
          default_args=default_args, 
          schedule_interval='@daily', 
          catchup=False) as dag:
    stream_task = PythonOperator(

        task_id='stream_data_from_api',
        python_callable=stream_data
    )

#stream_data()


