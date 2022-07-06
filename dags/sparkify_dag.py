from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG default args

default_args = {
    'owner': 'joh',
    'start_date': datetime(2022, 1, 7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# S3 Variables

S3_BUCKET = 'udacity-dend'
S3_PREFIX_LOG = 'log_data'
S3_PREFIX_SONG = 'song_data'
LOG_JSON_PATH='s3://udacity-dend/log_json_path.json'

# Data Quality Variables
TABLE_DICT = {'songplays':'playid',
        'artists':'artistid',
        'songs':'songid',
        'time':'start_time',
        'users':'userid'
        }

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='Create_tables',
    postgres_conn_id='redshift',
    sql='create_tables.sql',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_LOG,
    copy_options=f"FORMAT JSON AS '{LOG_JSON_PATH}'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_SONG,
    copy_options="FORMAT JSON AS 'auto'",
    dag=dag
)

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table='songplays',
    select_query=SqlQueries.songplay_table_insert,
    is_full_refresh=False,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table='users',
    select_query=SqlQueries.user_table_insert,
    is_full_refresh=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table='songs',
    select_query=SqlQueries.song_table_insert,
    is_full_refresh=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table='artists',
    select_query=SqlQueries.artist_table_insert,
    is_full_refresh=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table='time',
    select_query=SqlQueries.time_table_insert,
    is_full_refresh=True,
    dag=dag
)

run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    table_dict=TABLE_DICT,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG Dependencies

start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_user_dimension_table
load_songplays_fact_table >> load_song_dimension_table
load_songplays_fact_table >> load_artist_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_user_dimension_table >> run_data_quality_checks 
load_song_dimension_table >> run_data_quality_checks
load_artist_dimension_table >> run_data_quality_checks
load_time_dimension_table >> run_data_quality_checks

run_data_quality_checks >> end_operator

