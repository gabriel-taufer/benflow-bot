from datetime import datetime, timedelta
from typing import List

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from bs4 import BeautifulSoup

from subdags.tweet_dall_e_image import generate_tweet_with_dall_e_image_DAG

default_args = {
    "owner": "gabrieltaufer",
    "depends_on_past": False,
    "start_date": datetime(2022, 6, 1),
    "email": ["taufergabrielangelo@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_random_celebrity_name() -> str:
    response = requests.get("https://www.randomcelebritygenerator.com/")
    soup = BeautifulSoup(response.text, "html.parser")
    celebrity_name = soup.find('div', {"class": "celeb"}).find('h1').text
    return celebrity_name



def generate_dall_e_text(celebrity_name: str) -> str:
    return f"{celebrity_name} in RuPaul's Drag Race"



def get_celebrities_name_and_generate_text(task_instance: PythonOperator) -> str:
    xcom_result = task_instance.xcom_pull(task_ids=['generate_celebrity_name_task'])
    random_celebrity_name = xcom_result[0]
    return generate_dall_e_text(celebrity_name=random_celebrity_name)


with DAG("twitter_celebrity_in_rupaul", catchup=False, default_args=default_args, schedule_interval=timedelta(hours=6)) as dag:
    generate_celebrity = PythonOperator(
        task_id="generate_celebrity_name_task",
        python_callable=generate_random_celebrity_name
    )

    generate_celebrity_in_rupaul = PythonOperator(
        task_id="generate_celebrity_in_rupaul_text_task",
        python_callable=get_celebrities_name_and_generate_text
    )

    generate_image_and_tweet = SubDagOperator(
        task_id="generate_image_and_tweet",
        subdag=generate_tweet_with_dall_e_image_DAG(
            dall_e_text_task_id='generate_celebrity_in_rupaul_text_task',
            parent_dag_name="twitter_celebrity_in_rupaul",
            task_id="generate_image_and_tweet",
            default_args=default_args
        )
    )

    generate_celebrity >> generate_celebrity_in_rupaul >> generate_image_and_tweet
