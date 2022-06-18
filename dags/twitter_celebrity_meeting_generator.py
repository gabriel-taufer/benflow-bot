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


def generate_n_random_celebrity_names(number_of_celebrity_names: int) -> List[str]:
    return [generate_random_celebrity_name() for i in range(number_of_celebrity_names)]


def generate_dall_e_text(first_celebrity_name: str, second_celebrity_name: str) -> str:
    return f"{first_celebrity_name} meeting {second_celebrity_name}"


def get_celebrities_name_and_generate_text(task_instance: PythonOperator) -> str:
    xcom_result = task_instance.xcom_pull(task_ids=['generate_random_celebrity_names'])
    random_celebrity_names = xcom_result[0]
    first_celebrity_name, second_celebrity_name = random_celebrity_names
    return generate_dall_e_text(
        first_celebrity_name=first_celebrity_name,
        second_celebrity_name=second_celebrity_name
    )


with DAG("twitter_celebrity_meeting_dag", catchup=False, default_args=default_args, schedule_interval=timedelta(1)) as dag:
    generate_celebrities = PythonOperator(
        task_id="generate_random_celebrity_names",
        python_callable=generate_n_random_celebrity_names,
        op_kwargs={
            'number_of_celebrity_names': 2
        }
    )

    generate_celebrities_meeting_text = PythonOperator(
        task_id="generate_celebrities_meeting_text",
        python_callable=get_celebrities_name_and_generate_text
    )

    generate_image_and_tweet = SubDagOperator(
        task_id="generate_image_and_tweet",
        subdag=generate_tweet_with_dall_e_image_DAG(
            dall_e_text_task_id='generate_celebrities_meeting_text',
            parent_dag_name="twitter_celebrity_meeting_dag",
            task_id="generate_image_and_tweet",
            default_args=default_args
        )
    )

    generate_celebrities >> generate_celebrities_meeting_text >> generate_image_and_tweet
