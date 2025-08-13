import requests
import json
import time
from datetime import date,datetime,timedelta
import pandas as pd
import shutil
import os
import csv
import numpy as np
import re
from loguru import logger
import getopt
import sys
from ast import literal_eval

from ozon.scripts.constants import (
    headers,
    ozon_seller_api_url,
    client_name
)

# Функция получения списка кластеров
def get_cluster_list(headers):
    logger.info("Getting cluster list")

    # df, куда будем помещать результат запросов к АПИ
    df_cluster_list = pd.DataFrame()

    # Список типов кластеров
    cluster_types = ['CLUSTER_TYPE_OZON', 'CLUSTER_TYPE_CIS']

    for cluster_type in cluster_types:
        # Параметры запроса
        params_cluster_list = json.dumps({
            # "cluster_ids": [],
            "cluster_type": cluster_type
        })

        # Запрос к апи
        resp_data_cluster_list = requests.post(
            f"{ozon_seller_api_url}/v1/cluster/list",
            headers=headers,
            data=params_cluster_list
        ).json()

        # Переводим в df
        tmp_df_clusters = pd.DataFrame(resp_data_cluster_list['clusters'])
        # Переименовываем колонки с информацией о кластерах
        tmp_df_clusters = tmp_df_clusters.rename(columns={
            'name': 'cluster_name',
            'type': 'cluster_type'
        })
        # Объединяем с предыдущим проходом цикла
        df_cluster_list = pd.concat([df_cluster_list, tmp_df_clusters])
        # Делаем паузу в запросах
        time.sleep(2)

    # Сбрасываем index после concat
    df_cluster_list = df_cluster_list.reset_index(drop=True)
    # Распаковываем информацию о складах
    df_clusters_unpacked = df_cluster_list.explode('logistic_clusters')
    # Получаем информацию о складах в конкретном кластере
    df_clusters_unpacked = df_clusters_unpacked.assign(**{
        'warehouses': [w.get('warehouses') for w in df_clusters_unpacked.logistic_clusters]
    })
    # Распаковываем все склады в конкретном кластере
    df_clusters_unpacked = df_clusters_unpacked.explode('warehouses')
    # Сбрасываем индекс после всех распаковок
    df_clusters_unpacked = df_clusters_unpacked.reset_index()
    # Получаем информацию о конкретном складе в кластере
    df_warehouses = (
        pd.json_normalize(df_clusters_unpacked['warehouses'])
        .rename(columns={
            'type': 'warehouse_type',
            'name': 'warehouse_name'
        })
    )
    # Объединяем со списком кластеров
    df_clusters_and_warehouses = pd.concat([
        df_clusters_unpacked,
        df_warehouses
    ], axis=1)

    # Сортируем по кластеру
    df_clusters_and_warehouses = df_clusters_and_warehouses.sort_values(
        by=['cluster_name'],
        ignore_index=True
    )
    # Добавляем колонку с датой выгрузки
    df_clusters_and_warehouses['date_updated'] = str(date.today())
    # Выбираем нужные колонки
    df_clusters_and_warehouses = df_clusters_and_warehouses.loc[:, df_clusters_and_warehouses.columns.isin([
        'cluster_name',
        'cluster_type',
        'warehouse_name',
        'warehouse_type',
        'warehouse_id',
        'date_updated'
    ])]

    return df_clusters_and_warehouses
    # Сохраняем список кластеров и их складов
    # df_clusters_and_warehouses.to_csv('clusters_and_warehouses.csv', sep=';', index=False)


# Функция обновления файла с кластерами
def update_cluster_list(headers):
    # Получаем список кластеров и их складов по апи
    df_clusters = get_cluster_list(headers)
    # Проверяем, есть ли сохраненный файл с предыдущего запуска
    if os.path.isfile('clusters_and_warehouses.csv'):
        # Считываем старый файл с кластерами
        df_clusters_old = pd.read_csv('clusters_and_warehouses.csv', sep=';')
        # Убираем колонку с датой обновления, чтобы исключить её из сравнения
        df_clusters = df_clusters.loc[:, ~df_clusters.columns.isin(['date_updated'])]
        df_clusters_old = df_clusters_old.loc[:, ~df_clusters_old.columns.isin(['date_updated'])]
        # Если старый список кластеров отличается от нового,
        # то сохраняем новый список кластеров, если нет - оставляем старый
        if not df_clusters_old.equals(df_clusters):
            df_clusters.to_csv('clusters_and_warehouses.csv', sep=';', encoding='utf-8-sig', index=False)
            logger.info("Cluster list has been updated")
    else:
        logger.info("Cluster list has not changed")

if __name__ == '__main__':
    update_cluster_list(headers)
