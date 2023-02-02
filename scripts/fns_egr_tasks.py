import os
import logging
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

pilot_name = 'fns_egr'


def is_service_available():
    from fns_egr.fns_egr_main import FNSConnector
    from util.notifications import MqSender
    scripts_dir = os.path.join(Variable.get("SCRIPTS_DIR"), pilot_name)
    egrul_cert_path = os.path.join(scripts_dir, 'certs', '$202701.p12')
    egrip_cert_path = os.path.join(scripts_dir, 'certs', '$202702.p12')
    FNSConnector(
        url=Variable.get('FNS_FTPS'),
        egrul_cert_path=egrul_cert_path,
        egrul_cert_pswd=Variable.get('EGRUL_CERT_PSWD_SECRET'),
        egrip_cert_path=egrip_cert_path,
        egrip_cert_pswd=Variable.get('EGRIP_CERT_PSWD_SECRET'),
        data_path=os.path.join(Variable.get("DATA_DIR"), pilot_name),
        proxy=BaseHook.get_connection('proxy').get_uri(),
        mq_sender=MqSender(BaseHook.get_connection('mq')),
        rabbit_queue_nm=Variable.get('RABBIT_QUEUE_NM'),
        schema='new'
    ).is_service_available()


def is_new_files_available(flag, schema, **kwargs):
    from fns_egr.fns_egr_main import FNSConnector
    from util.notifications import MqSender
    scripts_dir = os.path.join(Variable.get("SCRIPTS_DIR"), pilot_name)
    egrul_cert_path = os.path.join(scripts_dir, 'certs', '$202701.p12')
    egrip_cert_path = os.path.join(scripts_dir, 'certs', '$202702.p12')
    FNSConnector(
        url=Variable.get('FNS_FTPS'),
        egrul_cert_path=egrul_cert_path,
        egrul_cert_pswd=Variable.get('EGRUL_CERT_PSWD_SECRET'),
        egrip_cert_path=egrip_cert_path,
        egrip_cert_pswd=Variable.get('EGRIP_CERT_PSWD_SECRET'),
        data_path=os.path.join(Variable.get("DATA_DIR"), pilot_name),
        proxy=BaseHook.get_connection('proxy').get_uri(),
        mq_sender=MqSender(BaseHook.get_connection('mq')),
        rabbit_queue_nm=Variable.get('RABBIT_QUEUE_NM'),
        schema=schema
    ).is_new_files_available(flag, kwargs['next_execution_date'])


def download_files(flag, schema, **kwargs):
    from fns_egr.fns_egr_main import FNSConnector
    from util.notifications import MqSender
    scripts_dir = os.path.join(Variable.get("SCRIPTS_DIR"), pilot_name)
    egrul_cert_path = os.path.join(scripts_dir, 'certs', '$202701.p12')
    egrip_cert_path = os.path.join(scripts_dir, 'certs', '$202702.p12')
    FNSConnector(
        url=Variable.get('FNS_FTPS'),
        egrul_cert_path=egrul_cert_path,
        egrul_cert_pswd=Variable.get('EGRUL_CERT_PSWD_SECRET'),
        egrip_cert_path=egrip_cert_path,
        egrip_cert_pswd=Variable.get('EGRIP_CERT_PSWD_SECRET'),
        data_path=os.path.join(Variable.get("DATA_DIR"), pilot_name),
        proxy=BaseHook.get_connection('proxy').get_uri(),
        mq_sender=MqSender(BaseHook.get_connection('mq')),
        rabbit_queue_nm=Variable.get('RABBIT_QUEUE_NM'),
        schema=schema
    ).download_files(flag, kwargs['next_execution_date'])


def unzip_files(flag, schema):
    from fns_egr.fns_egr_main import FNSConnector
    from util.notifications import MqSender
    scripts_dir = os.path.join(Variable.get("SCRIPTS_DIR"), pilot_name)
    egrul_cert_path = os.path.join(scripts_dir, 'certs', '$202701.p12')
    egrip_cert_path = os.path.join(scripts_dir, 'certs', '$202702.p12')
    FNSConnector(
        url=Variable.get('FNS_FTPS'),
        egrul_cert_path=egrul_cert_path,
        egrul_cert_pswd=Variable.get('EGRUL_CERT_PSWD_SECRET'),
        egrip_cert_path=egrip_cert_path,
        egrip_cert_pswd=Variable.get('EGRIP_CERT_PSWD_SECRET'),
        data_path=os.path.join(Variable.get("DATA_DIR"), pilot_name),
        proxy=BaseHook.get_connection('proxy').get_uri(),
        mq_sender=MqSender(BaseHook.get_connection('mq')),
        rabbit_queue_nm=Variable.get('RABBIT_QUEUE_NM'),
        schema=schema
    ).unzip_files(flag)


def create_main_table(flag, xml_root, parquet_root):
    from fns_egr.fns_egr_converter import create_main_parquet, rename_parquet, getSparkSessionInstance
    from shutil import rmtree
    parquet_path = os.path.join(parquet_root, flag)
    xml_path = os.path.join(xml_root, flag)
    os.makedirs(parquet_path, exist_ok=True)
    spark = getSparkSessionInstance()  # TODO: add app_name parametr in spark builder to detect perfomance
    for file in os.listdir(xml_path):
        file_name = os.path.basename(file).split('.XML')[0]
        parquet_file_path = os.path.join(parquet_path, file_name)
        full_xml_path = os.path.join(xml_path, file)
        logging.info(f'Converting {full_xml_path} into parquet...')
        create_main_parquet(spark, full_xml_path, parquet_file_path, flag)
        rename_parquet(parquet_file_path, parquet_path, file_name)
        rmtree(parquet_file_path)
        logging.info(f'File: {full_xml_path} converted into parquet -> {os.path.join(parquet_path, file_name)}.parquet')
        os.remove(full_xml_path)


def create_parquets(flag, table_name, parquet_root):
    import fns_egr.fns_egr_converter as converter
    from shutil import rmtree
    parquet_path = os.path.join(parquet_root, flag)
    for file in os.listdir(parquet_path):
        if os.path.isfile(os.path.join(parquet_path, file)):
            logging.info(f'Processing file {file}...')
            logging.info(f'Reading parquet {file}....')
            df = getattr(converter, 'read_parquet')(os.path.join(parquet_path, file))
            logging.info(f'Creating {table_name}...')
            table = getattr(converter, f'create_{table_name}')(df)
            logging.info(f'Successfully created {table_name}')
            table_directory = os.path.join(parquet_path, table_name)
            os.makedirs(table_directory, exist_ok=True)
            parquet_name = f'{table_name}{file.split(".parquet")[0].split(f"{flag.upper()}")[-1]}'
            parquet_tmp_dir = os.path.join(table_directory, parquet_name)
            logging.info(f'Saving parquet {parquet_name} into temp dir {parquet_tmp_dir}')
            getattr(converter, 'save_parquet')(table, parquet_tmp_dir)
            logging.info(f'Moving temp parquet from {parquet_tmp_dir} to {table_directory}')
            getattr(converter, 'rename_parquet')(parquet_tmp_dir, table_directory, parquet_name)
            rmtree(parquet_tmp_dir)
            logging.info(f'Successfully saved {parquet_name} in {table_directory}')


def remove_main_table(flag, parquet_root):
    parquet_dir = os.path.join(parquet_root, flag)
    for parquet in os.listdir(parquet_dir):
        full_parquet_path = os.path.join(parquet_dir, parquet)
        if os.path.isfile(full_parquet_path):
            os.remove(full_parquet_path)


def merge_parquets(flag, table_name, parquet_root):
    from itertools import groupby
    import fns_egr.fns_egr_converter as converter
    from shutil import rmtree
    parquet_path = os.path.join(parquet_root, flag, table_name)
    logging.info(f'Merging {table_name} parquets...')
    parquet_dict_by_dates = dict([(k, list(g)) for k, g in groupby(os.listdir(parquet_path), key=lambda x: x.split('_')[-2])])  # TODO: add doc whats doing here
    for date_key in parquet_dict_by_dates.keys():
        parquets_list = [os.path.join(parquet_path, parquet) for parquet in parquet_dict_by_dates.get(date_key)]
        logging.info(f'Parquets for merge for date: {date_key} found: {parquets_list}')
        df = getattr(converter, 'merge_parquets')(parquets_list)
        parquet_name = f'{table_name}_{date_key}'
        parquet_tmp_dir = os.path.join(parquet_path, parquet_name)
        logging.info(f'Saving merged parquets into temp dir {parquet_tmp_dir}')
        getattr(converter, 'save_parquet')(df, parquet_tmp_dir)
        logging.info(f'Moving temp parquet from {parquet_tmp_dir} to {parquet_path}')
        getattr(converter, 'rename_parquet')(parquet_tmp_dir, parquet_path, parquet_name)
        rmtree(parquet_tmp_dir)
        logging.info(f'Successfully saved {parquet_name} in {parquet_path}')
        logging.info('Removing small parquets...')
        for parquet_part in parquets_list:
            os.remove(parquet_part)
        logging.info('Removing done.')


def upload_to_s3(flag, tables_list, parquet_root, bucket_name):
    from fns_egr.fns_egr_s3 import S3Connector, s3_upload
    s3 = S3Connector(s3_conn=BaseHook.get_connection('s3_endpoint'), proxy=BaseHook.get_connection('proxy').get_uri()).connect()
    parquet_path = os.path.join(parquet_root, flag)
    for table in tables_list:
        logging.info(f'Uploading parquets for table {table}')
        table_parquets_path = os.path.join(parquet_path, table)
        parquets_list = [os.path.join(table_parquets_path, parquet) for parquet in os.listdir(table_parquets_path)]
        s3_upload(s3, flag, parquets_list, bucket_name)


def upload_to_ftps(flag, tables_list, parquet_root, ftps_root):
    from time import sleep
    from airflow.contrib.hooks.ftp_hook import FTPSHook
    from datetime import datetime as dt
    ftps = FTPSHook('ftps')
    ftps.get_conn().prot_p()
    parquet_path = os.path.join(parquet_root, flag)
    uploaded_parquets = ftps.list_directory(ftps_root)
    current_date = dt.now()
    for parquet in uploaded_parquets:
        uploaded_parquet_full_path = os.path.join(ftps_root, parquet)
        load_date = dt.strptime(parquet.split('.parquet')[0], '%Y-%m-%d_%H-%M-%S')
        date_diff = current_date - load_date
        if date_diff.days >= 21:
            logging.info(f'File {uploaded_parquet_full_path} older than 21 day, removing....')
            ftps.delete_file(uploaded_parquet_full_path)
    for table in tables_list:
        logging.info(f'Uploading parquets for table {table} to {ftps_root}')
        table_parquets_path = os.path.join(parquet_path, table)
        parquets_list = [os.path.join(table_parquets_path, parquet) for parquet in os.listdir(table_parquets_path)]
        for parquet_full_path in parquets_list:
            ftps_parquet_name = dt.now().strftime('%Y-%m-%d_%H-%M-%S')
            logging.info(f'Loading {parquet_full_path} to {ftps_parquet_name}')
            ftps.store_file(os.path.join(ftps_root, f"{ftps_parquet_name}.parquet"), parquet_full_path)
            sleep(1)


def send_notification_task(queue, bucket):
    from util.notifications import send_notification
    notification_body = {
        'bucket_name': bucket,
        'pilot_name': pilot_name,
        'mem_limit': 30,
        's3_clean_if_exist': True,
        'format_check': True,
        'hist': True,
        'file_mask': ['.parquet'],
        'destination': ['drp'],
    }
    send_notification(BaseHook.get_connection('mq'), queue, 'upload_to_ftps', 'success', notification_body)