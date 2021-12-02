#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "leah-playground")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "cluster-0c23")
REGION = os.environ.get("GCP_LOCATION", "us-central1")

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

# PYSPARK_JOB = {
#     "reference": {"project_id": "leah-playground"},
#     "placement": {"cluster_name": CLUSTER_NAME},
#     "pyspark_job": {"main_python_file_uri": "gs://leah-playground/word-count.py", "args": ["gs://leah-playground/input", "gs://leah-playground/output-0"]},
# }
### ALTERNATIVE BQ CONNECTOR
PYSPARK_JOB = {
    "reference": {"project_id": "leah-playground"},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "gs://leah-playground/word-count.py", "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]},
}
# [END how_to_cloud_dataproc_pyspark_config]


with models.DAG("example_gcp_dataproc", start_date=days_ago(1), schedule_interval=None) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # # [END how_to_cloud_dataproc_create_cluster_operator]


    # [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    # pyspark_task = DataprocSubmitJobOperator(
    #     task_id="pyspark_task", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID
    # )
    ### ALTERNATIVE w/ bigquery connector
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_connector", job=PYSPARK_JOB, location=REGION, project_id=PROJECT_ID 
    )
    # [END how_to_cloud_dataproc_submit_job_to_cluster_operator]


    # # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # # [END how_to_cloud_dataproc_delete_cluster_operator]
    create_cluster >> pyspark_task >> delete_cluster
   
