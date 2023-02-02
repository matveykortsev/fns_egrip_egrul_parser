from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

pilot_name = 'fns_egr'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1, 0, 0),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG('dcld_fns_egr',
         default_args=default_args,
         description='Daily load fns_egr',
         max_active_runs=1,
         catchup=False,
         schedule_interval='0 21 * * *') as dag:

    from fns_egr.fns_egr_tasks import is_service_available,\
        is_new_files_available, download_files, unzip_files, create_main_table, remove_main_table, create_parquets,\
        merge_parquets, upload_to_ftps, upload_to_s3, send_notification_task
    
    xml_root = f'{{{{ var.value.DATA_DIR }}}}/{pilot_name}/xml'
    parquet_root = f'{{{{ var.value.DATA_DIR }}}}/{pilot_name}/parquet'
    ftps_root = '{{ var.value.FNS_EGR_FTPS_ROOT }}'
    bucket_name = '{{ var.value.FNS_EGR_BUCKET_NM }}'
    
    egrip_table_list = {
        'ip_info',
        'fl_info',
        'citizenship_info',
        'regip_info',
        'regorg_info',
        'status_info',
        'stop_info',
        'uchetno_info',
        'regpf_info',
        'regfss_info',
        'okved_info',
        'lic_info',
        'noteegrip_info',
        'fiozags_info',
        'adremail_info',
    }
    
    egrul_table_list = {
        'ul_info',
        'ul_name',
        'ul_adress',
        'ulemail_info',
        'ulreg_info',
        'ulregorg_info',
        'ulstatus_info',
        'ulstop_info',
        'ulrules_info',
        'ulnouch_info',
        'ulpfreg_info',
        'ulfssreg_info',
        'ulustcap_info',
        'ulpolnom_info',
        'uluprorg_info',
        'ul_dover_info',
        'ul_korp_contr_info',
        'uluch_ru_info',
        'uluch_in_info',
        'uluch_fl_info',
        'uluch_sub_info',
        'uluch_pif_info',
        'uluch_doginv_info',
        'ulustcapdol_info',
        'ulacsderg_info',
        'ulokved_info',
        'ullicenses_info',
        'ulfilial_info',
        'ulrepresent_info',
        'ulreorg_info',
        'pravopred_info',
        'krestxoz',
        'ulpreemnik_info',
        'ulkvhp',
        'ulegrul_info'
    }

    is_service_available_task = PythonOperator(
        task_id='is_service_available_task',
        python_callable=is_service_available
    )

    is_new_egrul_files_available_task = PythonOperator(
        task_id='is_new_egrul_files_available_task',
        provide_context=True,
        python_callable=is_new_files_available,
        op_kwargs={'flag': 'egrul', 'schema': 'new'}
    )

    is_new_egrip_files_available_task = PythonOperator(
        task_id='is_new_egrip_files_available_task',
        provide_context=True,
        python_callable=is_new_files_available,
        op_kwargs={'flag': 'egrip', 'schema': 'new'}
    )

    download_egrul_file_task = PythonOperator(
        task_id='download_egrul_file_task',
        provide_context=True,
        python_callable=download_files,
        op_kwargs={'flag': 'egrul', 'schema': 'new'}
    )

    download_egrip_file_task = PythonOperator(
        task_id='download_egrip_file_task',
        provide_context=True,
        python_callable=download_files,
        op_kwargs={'flag': 'egrip', 'schema': 'new'}
    )

    unzip_egrul_file_task = PythonOperator(
        task_id='unzip_egrul_file_task',
        python_callable=unzip_files,
        op_kwargs={'flag': 'egrul', 'schema': 'new'},
        trigger_rule='none_failed'
    )

    unzip_egrip_file_task = PythonOperator(
        task_id='unzip_egrip_file_task',
        python_callable=unzip_files,
        op_kwargs={'flag': 'egrip', 'schema': 'new'},
        trigger_rule='none_failed'
    )

    create_main_table_egrul_task = PythonOperator(
        task_id='create_main_table_egrul_task',
        python_callable=create_main_table,
        op_kwargs={'flag': 'egrul',
                   'xml_root': xml_root,
                   'parquet_root': parquet_root
                   }
    )

    create_main_table_egrip_task = PythonOperator(
        task_id='create_main_table_egrip_task',
        python_callable=create_main_table,
        op_kwargs={'flag': 'egrip',
                   'xml_root': xml_root,
                   'parquet_root': parquet_root
                   }
    )

    remove_main_table_egrip_task = PythonOperator(
        task_id='remove_main_table_egrip_task',
        python_callable=remove_main_table,
        op_kwargs={'flag': 'egrip', 'parquet_root': parquet_root}
    )

    remove_main_table_egrul_task = PythonOperator(
        task_id='remove_main_table_egrul_task',
        python_callable=remove_main_table,
        op_kwargs={'flag': 'egrul', 'parquet_root': parquet_root}
    )

    create_egrip_parquets_tasks = [PythonOperator(task_id=f'create_egrip_{table}',
                                                  python_callable=create_parquets,
                                                  pool='fns_egr_egrip_pool',
                                                  op_kwargs={'flag': 'egrip',
                                                             'table_name': table,
                                                             'parquet_root': parquet_root,
                                                             }
                                                  ) for table in egrip_table_list
                                   ]

    create_egrul_parquets_tasks = [PythonOperator(task_id=f'create_egrul_{table}',
                                                  python_callable=create_parquets,
                                                  pool='fns_egr_egrul_pool',
                                                  op_kwargs={'flag': 'egrul',
                                                             'table_name': table,
                                                             'parquet_root': parquet_root,
                                                             }
                                                  ) for table in egrul_table_list
                                   ]

    merge_egrip_parquets_tasks = [PythonOperator(task_id=f'merge_egrip_{table}',
                                                 python_callable=merge_parquets,
                                                 pool='fns_egr_egrip_pool',
                                                 op_kwargs={'flag': 'egrip',
                                                            'table_name': table,
                                                            'parquet_root': parquet_root
                                                            }
                                                 ) for table in egrip_table_list
                                  ]

    merge_egrul_parquets_tasks = [PythonOperator(task_id=f'merge_egrip_{table}',
                                                 python_callable=merge_parquets,
                                                 pool='fns_egr_egrul_pool',
                                                 op_kwargs={'flag': 'egrul',
                                                            'table_name': table,
                                                            'parquet_root': parquet_root
                                                            }
                                                 ) for table in egrul_table_list
                                  ]
    egrul_upload_to_ftps = PythonOperator(
        task_id='egrul_upload_to_ftps',
        python_callable=upload_to_ftps,
        op_kwargs={
            'flag': 'egrul',
            'tables_list': egrul_table_list,
            'parquet_root': parquet_root,
            'ftps_root': ftps_root,
        }
    )

    egrip_upload_to_ftps = PythonOperator(
        task_id='egrip_upload_to_ftps',
        python_callable=upload_to_ftps,
        op_kwargs={
            'flag': 'egrip',
            'tables_list': egrip_table_list,
            'parquet_root': parquet_root,
            'ftps_root': ftps_root,
        }
    )

    egrul_upload_to_s3 = PythonOperator(
        task_id='egrul_upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'flag': 'egrul',
            'tables_list': egrul_table_list,
            'parquet_root': parquet_root,
            'bucket_name': bucket_name,
        }
    )

    egrip_upload_to_s3 = PythonOperator(
        task_id='egrip_upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'flag': 'egrip',
            'tables_list': egrip_table_list,
            'parquet_root': parquet_root,
            'bucket_name': bucket_name,
        }
    )

    trigger_dmz_dag = PythonOperator(
        task_id='trigger_dmz_dag',
        python_callable=send_notification_task,
        op_kwargs={
            'queue': '{{ var.value.RABBIT_QUEUE_NM }}',
            'bucket': bucket_name,
        },
        trigger_rule='none_failed'
    )

    is_new_egrul_files_available_old_schema_task = PythonOperator(
        task_id='is_new_egrul_files_available_old_schema_task',
        provide_context=True,
        python_callable=is_new_files_available,
        op_kwargs={'flag': 'egrul', 'schema': 'old'}
    )

    is_new_egrip_files_available_old_schema_task = PythonOperator(
        task_id='is_new_egrip_files_available_old_schema_task',
        provide_context=True,
        python_callable=is_new_files_available,
        op_kwargs={'flag': 'egrip', 'schema': 'old'}
    )

    download_egrul_file_old_schema_task = PythonOperator(
        task_id='download_egrul_file_old_schema_task',
        provide_context=True,
        python_callable=download_files,
        op_kwargs={'flag': 'egrul', 'schema': 'old'}
    )

    download_egrip_file_old_schema_task = PythonOperator(
        task_id='download_egrip_file_old_schema_task',
        provide_context=True,
        python_callable=download_files,
        op_kwargs={'flag': 'egrip', 'schema': 'old'}
    )

is_service_available_task >> [is_new_egrul_files_available_task, is_new_egrip_files_available_task]
# Rows 291-293 might be removed after history downloaded and transferred into hadoop, same as tasks *old_schema
is_service_available_task >> [is_new_egrul_files_available_old_schema_task, is_new_egrip_files_available_old_schema_task]
is_new_egrul_files_available_old_schema_task >> download_egrul_file_old_schema_task >> unzip_egrul_file_task
is_new_egrip_files_available_old_schema_task >> download_egrip_file_old_schema_task >> unzip_egrip_file_task
is_new_egrul_files_available_task >> download_egrul_file_task >> unzip_egrul_file_task >> create_main_table_egrul_task >> create_egrul_parquets_tasks
is_new_egrip_files_available_task >> download_egrip_file_task >> unzip_egrip_file_task >> create_main_table_egrip_task >> create_egrip_parquets_tasks
create_egrul_parquets_tasks >> remove_main_table_egrul_task >> merge_egrul_parquets_tasks >> egrul_upload_to_ftps >> egrul_upload_to_s3
create_egrip_parquets_tasks >> remove_main_table_egrip_task >> merge_egrip_parquets_tasks >> egrip_upload_to_ftps >> egrip_upload_to_s3
egrul_upload_to_s3 >> trigger_dmz_dag
egrip_upload_to_s3 >> trigger_dmz_dag