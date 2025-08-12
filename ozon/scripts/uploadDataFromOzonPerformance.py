
# %% Определение всех функций
import requests
import json
import time
from datetime import date,datetime,timedelta
import pandas as pd
import shutil
import os
import csv
import zipfile
from zipfile import ZipFile
import numpy as np
from openpyxl import Workbook
import re
from loguru import logger
import getopt
import sys


# Файл с настройками и номером клиента
# from options import settings, client_number

from ozon.scripts.constants import (
    client_name,
    marketplace_dir_name,
    ozon_performance_api_url,
    client_id_performance,
    client_secret_performance
)

# Функция создания нужных директорий
def create_dirs_performance():
    print('jopa')


# Функция генерации дат
def generateDates(days_ago = 30):
    date_end = str(date.today()- timedelta(days=1)) + 'T23:59:59.000Z'
    date_start = str(date.today()- timedelta(days=1) - timedelta(days_ago)) + 'T00:00:00.000Z'
    logger.info(f"Uploading orders from "+ date_start + " to " + date_end)
    return date_end,date_start #,date_end_file,date_start_file

# Функция сохранения дат в csv
def save_dates_to_csv(date_start, date_end):
    # Формат дат для названий файла
    date_end_file = datetime.fromisoformat(date_end).strftime('%Y-%m-%d')
    date_start_file = datetime.fromisoformat(date_start).strftime('%Y-%m-%d')
    # Датафрейм с датами начала и окончания выгрузки данных
    df_dates = pd.DataFrame({'date_end': date_end,
                             'date_start': date_start,
                             'date_end_file': date_end_file,
                             'date_start_file': date_start_file},
                             index=[0])
    df_dates.to_csv(f"{uploaddir_today}/{str(date.today())}_dates_from_to.csv", sep=';', index=False)


# Функция получения токена авторизации
def getAuthorizationToken(client_id_performance, client_secret_performance):
    # URL запроса
    url = ozon_performance_api_url
    endpoint = "/api/client/token"
    # Заголовки запроса
    headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
    }
    # Параметры запроса
    params = {
    "client_id": client_id_performance,
    "client_secret": client_secret_performance,
    "grant_type":"client_credentials"
    }
    token_dict = requests.post(url + endpoint, headers=headers, json=params).json()
    # Добавляем поля даты и времени создания токена и истечения срока его действия
    token_creation_date = datetime.now()
    token_expiration_date = token_creation_date + timedelta(seconds=token_dict['expires_in']) - timedelta(seconds=60)
    token_dict['date_created'] = token_creation_date
    token_dict['date_expires'] = token_expiration_date
    if 'access_token' not in token_dict.keys():
        print('Error getting access token')
    else:
        return token_dict


# Функция проверки действительности токена
def check_token_exp(auth_token, client_id_performance, client_secret_performance):
    # Делаем копию для избежания изменений в оригинале
    new_auth_token = auth_token.copy()
    # Сохраняем текущую дату и время в переменную
    current_datetime = datetime.now()
    # Если текущая дата больше даты истечения срока действия токена, запрашиваем новый
    if current_datetime >= auth_token['date_expires']:
        logger.info("Auth token expired, requesting new token")
        new_auth_token = getAuthorizationToken(client_id_performance, client_secret_performance)

    return new_auth_token


# Список кампаний
def getCompanyList(auth_token, companies_list=[]):
    # Авторизация
    headers = {
        'Authorization': auth_token['token_type'] + ' ' + auth_token['access_token']
    }
    # Параметры запроса
    params_companies = {
        "campaignIds": companies_list,
        "advObjectType": [],
        "state": [],
    }
    resp_data_companies = (
        requests.get(
            f"{ozon_performance_api_url}:443/api/client/campaign",
            headers=headers, params=params_companies
        )
        .json()
    )
    df_companies = pd.DataFrame(resp_data_companies['list'])
    df_companies = df_companies.rename(columns={
        'id': 'Номер кампании'
    })
    return df_companies


# Статистика по компаниям
def getCompanyStatistics(
        date_start,
        date_end,
        auth_token,
        df_companies,
        companies_stats_dir,
        date_from='',
        date_to='',
        task_id = None
):
    # Создаем копию для избежания изменений в оригинальном df
    df_companies_ = df_companies.copy()
    # Диапазоны выгрузок (с шагом в 10 кампаний)
    step = 10
    df_companies_ = df_companies_.reset_index(drop=True)
    df_companies_['chunks'] = df_companies_.index.map(lambda x: int(x/step) + 1)
    # Сколько всего получилось диапазонов
    total_chunks = max(df_companies_['chunks'].unique())
    # Сколько всего получилось кампаний
    total_companies = df_companies_.shape[0]
    # Сколько выгружено кампаний
    uploaded_companies_amount = 0
    # Цикл по каждому диапазону
    for chunk in df_companies_['chunks'].unique():
        # Проверяем срок действия токена
        auth_token = check_token_exp(auth_token, client_id_performance, client_secret_performance)
        # Авторизация
        headers = {
            'Authorization': auth_token['token_type'] + ' ' + auth_token['access_token']
        }

        # Формируем список кампаний из диапазона
        companies_list = df_companies_.loc[df_companies_['chunks'] == chunk, 'Номер кампании'].to_list()
        # Переводим список кампаний в строковый тип
        companies_list_str = [str(elem) for elem in companies_list]
        # Сколько получилось кампаний
        current_companies_amount = len(companies_list_str)
        # Сколько выгружается компаний + сколько уже выгружено
        # current_companies_amount =+ uploaded_companies_amount
        # Параметры запроса
        params_companies_stats = {
            "campaigns": companies_list_str,
            "from": date_start,
            "to": date_end,
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupBy": "NO_GROUP_BY"
        }
        # Удаляем дату со временем из запроса если они не указаны
        if date_start == '' or date_end == '':
            params_companies_stats.pop('from')
            params_companies_stats.pop('to')
        # Если не задан ID задания на генерацию отчета, то создаем задание на генерацию отчета
        # Если он задан (например, мы до этого запускали и не получили отчеты с  этого задания),
        # то пропускаем этот этап
        if task_id is None:
            # Флаг проверки того, что прошел запрос на формирование отчета
            report_task_ok = False
            # Делаем запросы, пока не пройдет запрос на формирование отчета
            while not report_task_ok:
                # Запрос к апи
                resp_data_company_stats = (
                    requests
                    .post(
                        f"{ozon_performance_api_url}:443/api/client/statistics",
                        headers=headers,
                        json=params_companies_stats
                    )
                    .json()
                )
                # Если при запросе выходит ошибка, выводим текст ошибки
                if 'error' in resp_data_company_stats.keys():
                    # Получаем текст ошибки
                    error_message = resp_data_company_stats['error']
                    logger.info(f"{error_message}")
                    # Делаем паузу перед следующей отправкой запроса на генерацию
                    time.sleep(5)
                # Если ошибки нет, то ставим флаг того, что запрос прошел и идем дальше
                else:
                    report_task_ok = True

            # Получаем номер задания на генерацию отчета
            task_id = resp_data_company_stats['UUID']

        # Получаем статус отчета
        report_status = '' # Начальное значение статуса отчета
        logger.info("Getting companies stats report status")
        # Делаем запросы, пока отчет не сформируется, либо не будет ошибка
        while report_status not in ['OK', 'ERROR']:
            # Заново запрашиваем токен авторизации, если истек текущий
            auth_token = check_token_exp(auth_token, client_id_performance, client_secret_performance)
            # Авторизация
            headers = {
                'Authorization': auth_token['token_type'] + ' ' + auth_token['access_token']
            }
            # Делаем запрос
            resp_data_report_status = requests.get(f"{ozon_performance_api_url}:443/api/client/statistics/{task_id}",
                                                headers=headers).json()
            # Получаем статус отчета
            report_status = resp_data_report_status['state']
            # Выводим сообщение, сколько уже выгружено кампаний и сколько выгружается
            logger.info(
                f"\nUploading "
                f"{current_companies_amount} of {total_companies} companies\n"
                f"(Chunk {chunk} of {total_chunks})\n"
                f"Already uploaded: {uploaded_companies_amount} companies\n"
                f"Dates: {date_start} - {date_end}\n"
                f"Report creation status: {report_status}\n"
                f"Task id: {task_id}"
            )
            # logger.info(f"Report creation status: {report_status}")
            # Ограничение апи - один запрос в 5 секунд
            time.sleep(5)

        # Если отчет сформирован успешно, то скачиваем его
        if resp_data_report_status['state'] == 'OK':
            logger.info("Companies stats report created, downloading")
            params_get_report = {
                "UUID": resp_data_report_status['UUID']
            }
            resp_data_report = requests.get(f"{ozon_performance_api_url}:443/api/client/statistics/report",
                                params=params_get_report,
                                headers=headers)
            # Определяем расширение файла
            report_type = resp_data_report.headers['Content-Type'].split(';')[0]
            # Получаем имя файла
            file_name = resp_data_report.headers['Content-Disposition'].split(';')[1]
            # Удаляем лишние символы из имени файла
            replace_dict = {
                '"': '',
                ' ': '',
                'filename=': '',
            }
            # Заменяем символы
            for old, new in replace_dict.items():
                file_name = file_name.replace(old, new)
            # Если csv, то просто сохраняем файл
            if 'csv' in report_type:
                file_type = 'csv'
                with open(f"{companies_stats_dir}/{file_name}", 'wb') as w:
                    w.write(resp_data_report.content)
                    w.close()
            # Если zip, сохраняем архив, затем распаковываем его
            else:
                file_type = 'zip'
                with open(f"{companies_stats_dir}/{str(date.today())}_Кампании_{chunk}.{file_type}", 'wb') as w:
                    w.write(resp_data_report.content)
                    w.close()
                with ZipFile(f"{companies_stats_dir}/{str(date.today())}_Кампании_{chunk}.{file_type}", 'r') as zip_ref:
                    zip_ref.extractall(companies_stats_dir)

            # Увеличиваем число выгруженных кампаний
            uploaded_companies_amount =+ current_companies_amount



# Отчёт по товарам в продвижении в поиске
def getProductsSearchPromote(auth_token, date_start, date_end):
    # Даты формирования отчета
    params_report_id = {
    "from": date_start,
    "to": date_end
    }
    # Авторизация
    headers = {
        'Authorization': auth_token['token_type'] + ' ' + auth_token['access_token']
    }
    # Получаем идентификатор отчета
    resp_data_report_id = requests.post(f"{ozon_performance_api_url}:443/api/client/statistic/products/generate",
                              headers=headers,
                              json=params_report_id).json()
    # Получаем статус отчета
    report_status = ''
    logger.info("Getting products promote report status")
    while report_status not in ['OK', 'ERROR']:
        resp_data_report_status = requests.get(f"{ozon_performance_api_url}:443/api/client/statistics/{resp_data_report_id['UUID']}",
                                            headers=headers).json()
        report_status = resp_data_report_status['state']
        time.sleep(5)

    # Если отчет сформирован успешно, то скачиваем его
    if resp_data_report_status['state'] == 'OK':
        logger.info("Products promote report created, downloading")
        params_get_report = {
            "UUID": resp_data_report_status['UUID']
        }
        resp_data_report = requests.get(f"{ozon_performance_api_url}:443/api/client/statistics/report",
                              params=params_get_report,
                              headers=headers)
        with open(f"{uploaddir_today}/{str(date.today())}_Отчет_Товары_Продвижение.csv", 'wb') as w:
            w.write(resp_data_report.content)
            w.close()
        logger.info("Done uploading products promote report")
    else:
        logger.info("Error creating products promote report")


# Вызов всех функций
def upload_data_performane(
        date_start,
        date_end,
        client_id_performance,
        client_secret_performance,
        companies_stats_dir,
        df_companies=None,
        task_id=None,
):
    # Получаем токен авторизации
    auth_token = getAuthorizationToken(client_id_performance, client_secret_performance)

    # Если df со списком кампаний не задан, то список кампаний берем по апи
    if df_companies is None:
        df_companies_for_statistic = getCompanyList(auth_token)
    # Если df со списком кампаний задан, то кампании берем из этого df
    else:
        df_companies_for_statistic = df_companies.copy()

    getCompanyStatistics(
        date_start,
        date_end,
        auth_token,
        df_companies_for_statistic,
        companies_stats_dir,
        task_id
    )
    print(f"\033[44m\033[37mDONE UPLOADING OZON PERFORMANCE FOR CLIENT {client_name}")


# %% Вызов всех функций
if __name__ == '__main__':
    # создаем отдельную папку для текущей выгрузки
    uploaddir = f"{marketplace_dir_name}Clients/{client_name}/UploadFilesPerformance"
    if not os.path.exists(uploaddir):
        os.makedirs(uploaddir)
    uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"
    if os.path.exists(uploaddir_today):
        shutil. rmtree(uploaddir_today)
    # new_dir = os.mkdir(f"{uploaddir}/UploadFiles_"+str(date.today()))
    os.mkdir(uploaddir_today)
    companies_stats_dir = f"{uploaddir_today}/{str(date.today())}_Кампании"
    os.mkdir(companies_stats_dir)

    date_start = '2025-04-01T00:00:00Z'
    date_end = '2025-04-30T23:59:59Z'
    auth_token = getAuthorizationToken(client_id_performance, client_secret_performance,)

    df_companies = getCompanyList(auth_token)
    getCompanyStatistics(date_start, date_end, auth_token, df_companies, companies_stats_dir)
    print(f"\033[44m\033[37mDONE UPLOADING OZON PERFORMANCE FOR CLIENT {client_name}")
    # getProductsSearchPromote(auth_token, date_start, date_end)
