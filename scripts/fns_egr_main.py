import logging
import os
import shutil
import zipfile

from requests_pkcs12 import get
from bs4 import BeautifulSoup

from airflow.exceptions import AirflowSkipException


class FNSConnector:
    def __init__(self, url, egrul_cert_path, egrul_cert_pswd, egrip_cert_path, egrip_cert_pswd, data_path, proxy, mq_sender, rabbit_queue_nm, schema):
        self.url = url
        self.egrul_cert_path = egrul_cert_path
        self.egrul_cert_pswd = egrul_cert_pswd
        self.egrip_cert_path = egrip_cert_path
        self.egrip_cert_pswd = egrip_cert_pswd
        self.egrul_dirs = '?dir=EGRUL_406' if schema == 'new' else '?dir=EGRUL'
        self.egrip_dirs = '?dir=EGRIP_405' if schema == 'new' else '?dir=EGRIP'
        self.archive_path = os.path.join(data_path, 'archive')
        self.xml_path = os.path.join(data_path, 'xml')
        self.arch_egrul_dir = os.path.join(self.archive_path, 'egrul')
        self.arch_egrip_dir = os.path.join(self.archive_path, 'egrip')
        self.xml_egrul_dir = os.path.join(self.xml_path, 'egrul')
        self.xml_egrip_dir = os.path.join(self.xml_path, 'egrip')
        self.proxy = {"https": proxy, "http": proxy}
        self.mq_sender = mq_sender
        self.rabbit_queue_nm = rabbit_queue_nm

    def get_response(self, url):
        if 'EGRUL' in url:
            return get(
                url,
                data='{"folder": "/"}',
                headers={'Content-Type': 'application/json'},
                verify=False,
                pkcs12_filename=self.egrul_cert_path,
                pkcs12_password=self.egrul_cert_pswd,
                proxies=self.proxy
            )
        elif 'EGRIP' in url:
            return get(
                url,
                data='{"folder": "/"}',
                headers={'Content-Type': 'application/json'},
                verify=False,
                pkcs12_filename=self.egrip_cert_path,
                pkcs12_password=self.egrip_cert_pswd,
                proxies=self.proxy
            )
        else:
            return get(
                url,
                data='{"folder": "/"}',
                headers={'Content-Type': 'application/json'},
                verify=False,
                pkcs12_filename=self.egrul_cert_path,
                pkcs12_password=self.egrul_cert_pswd,
                proxies=self.proxy
            )

    def is_service_available(self):
        try:
            logging.info('Checking access to FNS service...')
            response = self.get_response(self.url)
            if response.status_code <= 200:
                logging.info('FNS service available!')
            else:
                logging.error(f'FNS service answer with status code {response.status_code}. Failed to get access!')
                message = f"Код: FNS_EGR_FILECOPY_01 \nОписание: ИС 1505_01: Ошибка проверки доступа к источнику: {response.status_code}\n."
                body = {"message": message, "subject": f"FNS_EGR_FILECOPY_01"}
                self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
                response.raise_for_status()
        except Exception as e:
            logging.error('Failed to make request to FNS service!')
            logging.error(e)
            message = f"Код: FNS_EGR_FILECOPY_01 \nОписание: ИС 1505_01: Ошибка проверки доступа к источнику.\n"
            body = {"message": message, "subject": f"FNS_EGR_FILECOPY_01"}
            self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
            raise e

    def is_new_files_available(self, flag, next_execution_date):
        if flag == 'egrul':
            dir = self.egrul_dirs
        elif flag == 'egrip':
            dir = self.egrip_dirs
        if next_execution_date.day == 1 and next_execution_date.month == 1:
            datapath_dir = os.path.join(self.url, dir, f'{next_execution_date.strftime("%d.%m.%Y")}_FULL')
        else:
            datapath_dir = os.path.join(self.url, dir, next_execution_date.strftime("%d.%m.%Y"))
        try:
            response = self.get_response(datapath_dir)
            page = BeautifulSoup(response.text, 'lxml')
            error_tag = page.find('b')
            if response.status_code <= 200:
                if error_tag is not None: # Checking for ERROR tag <b>ERROR:</b>
                    logging.error(f'New files for date {next_execution_date.strftime("%d.%m.%Y")} not found for {dir}!')
                    message = f"Код: FNS_EGR_FILECOPY_02 \nОписание: ИС 1505_01: Ошибка проверки наличия файлов/новых данных.\n" \
                              f'Файл: {next_execution_date.strftime("%d.%m.%Y")} в директории {dir}'
                    body = {"message": message, "subject": f"FNS_EGR_FILECOPY_02"}
                    self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
                    raise AirflowSkipException
                elif error_tag is None:
                    logging.info(f'Files for {dir} and {next_execution_date.strftime("%d.%m.%Y")} found. Going to downloading...')
                    return True
            elif response.status_code > 200:
                logging.error(f'Failed to check {dir} for files for {next_execution_date.strftime("%d.%m.%Y")}')
                logging.error(f'For {dir} status code is {response.status_code}.')
                message = f"Код: FNS_EGR_FILECOPY_02 \nОписание: ИС 1505_01: Ошибка проверки наличия файлов/новых данных.\n" \
                          f'Файл: {next_execution_date.strftime("%d.%m.%Y")} в директории {dir}'
                body = {"message": message, "subject": f"FNS_EGR_FILECOPY_02"}
                self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
                raise ConnectionError
        except Exception as e:
            logging.error('Failed to make request to FNS service!')
            logging.error(e)
            message = f"Код: FNS_EGR_FILECOPY_02 \nОписание: ИС 1505_01: Ошибка проверки наличия файлов/новых данных.\n" \
                      f'Файл: {next_execution_date.strftime("%d.%m.%Y")} в директории {dir}'
            body = {"message": message, "subject": f"FNS_EGR_FILECOPY_02"}
            self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
            raise e

    def download_files(self, flag, next_execution_date):
        os.makedirs(self.archive_path, exist_ok=True)
        if flag == 'egrul':
            dir = self.egrul_dirs
            archive_dir = self.arch_egrul_dir
        elif flag == 'egrip':
            dir = self.egrip_dirs
            archive_dir = self.arch_egrip_dir
        if os.path.exists(archive_dir):
            logging.info(f'Deleting old files from {archive_dir}...')
            shutil.rmtree(archive_dir)
        os.makedirs(archive_dir, exist_ok=True)
        if next_execution_date.day == 1 and next_execution_date.month == 1:
            datapath_dir = os.path.join(self.url, dir, f'{next_execution_date.strftime("%d.%m.%Y")}_FULL')
        else:
            datapath_dir = os.path.join(self.url, dir, next_execution_date.strftime("%d.%m.%Y"))
        try:
            response_num_dir = self.get_response(datapath_dir)
        except Exception as e:
            logging.error('Failed to make request to FNS service!')
            logging.error(e)
            message = f"Код: FNS_EGR_FILECOPY_03 \nОписание: ИС 1505_01: Ошибка загрузки данных в T1-Cloud.\n"
            body = {"message": message, "subject": f"FNS_EGR_FILECOPY_03"}
            self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
            raise e
        page = BeautifulSoup(response_num_dir.text, 'lxml')
        links = [clearfix['href'] for clearfix in page.find_all('a', attrs={'class': 'clearfix'}, href=True) if clearfix['href'] != os.path.join(self.url, dir)]
        if not links:
            logging.info(f'New files for date {next_execution_date.strftime("%d.%m.%Y")} not found for {dir}!')
            message = f"Код: FNS_EGR_FILECOPY_02 \nОписание: ИС 1505_01: Ошибка проверки наличия файлов/новых данных.\n" \
                      f'Файл: {next_execution_date.strftime("%d.%m.%Y")} в директории {dir}'
            body = {"message": message, "subject": f"FNS_EGR_FILECOPY_02"}
            self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
            raise AirflowSkipException
        downloaded_files = {}
        for link in links:
            filename = os.path.basename(link)
            local_path = os.path.join(archive_dir, filename)
            remote_path = os.path.join(self.url, link)
            logging.info(f'Downloading file {remote_path} to {local_path}')
            try:
                with self.get_response(remote_path) as r:
                    r.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=512 * 1024):
                            f.write(chunk)
                downloaded_files[filename] = os.stat(local_path).st_size
                logging.info(f'Successfully downloaded file {local_path}')
            except Exception as e:
                logging.error(f'Failed to download file {remote_path}')
                logging.error(e)
                message = f"Код: FNS_EGR_FILECOPY_03 \nОписание: ИС 1505_01: Ошибка загрузки данных в T1-Cloud.\n" \
                          f'Файл: {remote_path}'
                body = {"message": message, "subject": f"FNS_EGR_FILECOPY_03"}
                self.mq_sender.send_notification(self.rabbit_queue_nm, 'email', 'failed', body)
                continue
        return downloaded_files

    def unzip_files(self, flag):
        os.makedirs(self.xml_path, exist_ok=True)
        if flag == 'egrul':
            xml_dir = self.xml_egrul_dir
            archive_dir = self.arch_egrul_dir
        elif flag == 'egrip':
            xml_dir = self.xml_egrip_dir
            archive_dir = self.arch_egrip_dir
        os.makedirs(xml_dir, exist_ok=True)
        for file in os.listdir(archive_dir):
            full_arch_dir = os.path.join(archive_dir, file)
            try:
                with zipfile.ZipFile(full_arch_dir, 'r') as zip_ref:
                    zip_ref.extractall(xml_dir)
                os.remove(full_arch_dir)
                logging.info(f'Successfully extracted zip file {full_arch_dir} -> {xml_dir}')
            except Exception as e:
                logging.error(f'Failed to extract file {full_arch_dir}')
                logging.error(e)
                continue