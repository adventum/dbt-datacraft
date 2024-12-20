# from apache_airflow_providers_datacraft_dags import get_configs
from airflow.providers.datacraft.dags.configs.get_configs import get_configs
from datetime import datetime


from airflow.decorators import dag, task


class DagBuilder:
    def create_dags(self):
        @dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
        def hello_hello():
            @task(task_id="hello", retries=2)
            def hello_task():
                print(get_configs())

            hello_task()

        hello_hello()
