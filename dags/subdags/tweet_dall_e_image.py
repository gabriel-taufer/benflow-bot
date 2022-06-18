import base64
import io
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
import tweepy
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from requests.adapters import HTTPAdapter, Retry

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


def retry_session(retries: int, session: Optional[requests.Session] = None,
                  backoff_factor: int = 1) -> requests.Session:
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503],
        method_whitelist=frozenset(['GET', 'POST']),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def post_tweet(tweet_text: str, base_64_image: str):
    twitter_auth_keys = {
        "consumer_key": Variable.get('TWITTER_API_KEY'),
        "consumer_secret": Variable.get('TWITTER_API_SECRET'),
        "access_token": Variable.get('TWITTER_ACCESS_TOKEN'),
        "access_token_secret": Variable.get('TWITTER_ACCESS_TOKEN_SECRET'),
        "bearer_token": Variable.get('TWITTER_BEARER_TOKEN')
    }
    auth = tweepy.OAuthHandler(
        twitter_auth_keys['consumer_key'],
        twitter_auth_keys['consumer_secret']
    )
    auth.set_access_token(
        twitter_auth_keys['access_token'],
        twitter_auth_keys['access_token_secret']
    )
    api = tweepy.API(auth)
    twitter_client = tweepy.Client(**twitter_auth_keys)

    file_name = ''.join(tweet_text.lower().split(' '))
    media = api.media_upload(file_name, file=io.BytesIO(base64.b64decode(str(base_64_image).replace('\n', ''))))
    return twitter_client.create_tweet(text=tweet_text, media_ids=[media.media_id_string])


def generate_dall_e_image(task_instance: PythonOperator, dall_e_text_task_id: str, dag_id: str) -> str:
    logging.getLogger().setLevel(logging.DEBUG)

    xcom_result = task_instance.xcom_pull(
        task_ids=[dall_e_text_task_id],
        dag_id=dag_id
    )
    dall_e_text = xcom_result[0]

    session = retry_session(50)
    dall_e_response = session.post("https://bf.dallemini.ai/generate", json={
        "prompt": dall_e_text,
    })

    b64_image = dall_e_response.json()['images'][0]

    return b64_image


def tweet_dall_e_image(task_instance: PythonOperator, dall_e_text_task_id: str, dag_id: str):
    subdag_xcom_result = task_instance.xcom_pull(
        task_ids=['generate_dall_e_image_task']
    )
    parent_xcom_result = task_instance.xcom_pull(
        task_ids=[dall_e_text_task_id],
        dag_id=dag_id
    )

    dall_e_image = subdag_xcom_result[0]
    dall_e_text = parent_xcom_result[0]

    post_tweet(dall_e_text, dall_e_image)


def generate_tweet_with_dall_e_image_DAG(
        parent_dag_name: str,
        task_id: str,
        dall_e_text_task_id: str,
        default_args = {}
):
    subdag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",
        default_args=default_args
    )

    generate_dall_e_image_task = PythonOperator(
        task_id="generate_dall_e_image_task",
        python_callable=generate_dall_e_image,
        op_kwargs={
            'dall_e_text_task_id': dall_e_text_task_id,
            'dag_id': parent_dag_name
        },
        dag=subdag
    )

    tweet_dall_e_image_task = PythonOperator(
        task_id='tweet_dall_e_image_task',
        python_callable=tweet_dall_e_image,
        op_kwargs={
            'dall_e_text_task_id': dall_e_text_task_id,
            'dag_id': parent_dag_name
        },
        dag=subdag
    )

    generate_dall_e_image_task >> tweet_dall_e_image_task
    return subdag
