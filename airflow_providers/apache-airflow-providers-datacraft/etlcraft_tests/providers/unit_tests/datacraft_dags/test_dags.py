from airflow.decorators import dag

from pendulum import datetime
from airflow.operators.empty import EmptyOperator


def test_run_dag():
    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag():
        EmptyOperator(task_id="first_task")

    dag_object = test_dag()
    dag_object.test()


def test_run_dag2():
    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag():
        EmptyOperator(task_id="first_task2")

    dag_object = test_dag()
    dag_object.test()
