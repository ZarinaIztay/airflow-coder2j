from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    'owner': 'airflow'
}

@dag(dag_id='dag_with_taskflow_api_v02', 
    default_args=default_args, 
    start_date=datetime(2024, 7, 1), 
    schedule_interval='@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'firstname': 'Jerry', 'lastname': 'Iztay'}
    
    @task()
    def get_age():
        return 23
    
    @task()
    def greet(firstname, lastname, age):
        print(f"Hello! My name is {firstname} {lastname}", f" I am {age} y.o.")

    name_dict= get_name()
    age = get_age()
    greet(firstname=name_dict['firstname'], lastname=name_dict['lastname'], age=age)

greet_dag = hello_world_etl()