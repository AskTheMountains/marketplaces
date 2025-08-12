# %% Определение всех функций
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


# Файл с некоторыми константами
from ozon.scripts.constants import (
    headers,
    ozon_seller_api_url,
    client_name,
    marketplace_dir_name,
)

# Функция обновления кластеров Озон
from ozon.scripts.update_clusters import update_cluster_list

# Пример использования
def usage():
   print("Usage: script.py -days=31")
   return

def read_params():
    # Обработка опций и аргументов
    days_to_process = 2
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hd:", ["help", "days="])
    except getopt.GetoptError as err:
        logger.error("Option not recognized")
        usage()
        sys.exit(2)

    if len(opts) == 0:
        logger.error("No option was specified")
        usage()
        sys.exit()

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d", "--days"):
            days_to_process = int(a)
        else:
            assert False, "unhandled option"

    logger.info(f'Days to process: {days_to_process}')

    return days_to_process


# Функция создания директорий для текущей выгрузки
def create_dirs():
    # создаем отдельную папку для текущей выгрузки
    uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
    if not os.path.exists(uploaddir):
        os.makedirs(uploaddir)
    uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"
    if os.path.exists(uploaddir_today):
        shutil. rmtree(uploaddir_today)
    new_dir = os.mkdir(f"{uploaddir}/UploadFiles_"+str(date.today()))


def generateDates(days_ago = 30):
    date_end = str(date.today()- timedelta(days=1)) + 'T23:59:59.000Z'
    date_start = str(date.today()- timedelta(days=1) - timedelta(days_ago)) + 'T00:00:00.000Z'
    logger.info(f"Generated dates: {date_start} - {date_end}")

    return date_start, date_end #,date_end_file,date_start_file

# GPT START ----
# Функция создания диапазона дат с указанной разницей
def generate_dates_new(days_ago=30):
    # Получаем сегодняшнюю дату и округляем к 23:59:59 предыдущих суток
    end_date = datetime.now()
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0) - timedelta(days=1)

    # Вычисляем начальную дату на 00:00:00
    start_date = end_date - timedelta(days=days_ago) + timedelta(days=1)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # Устанавливаем конечную дату на 23:59:59
    # end_date = start_date + timedelta(days=days_ago) - timedelta(seconds=1)

    # Преобразуем даты в нужный формат
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    return start_date_str, end_date_str
# GPT END ----


# Сохранение дат текущей выгрузки в csv
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


# Отчет по товарам (список товаров)
def getOzonProduct(headers, to_save=True):
    # Параметры запроса на создание отчета по товарам
    params = json.dumps({
    "language": "DEFAULT",
    "offer_id": [],
    "search": "",
    "sku": [],
    "visibility": "ALL"
    })

    # Отправляем запроса на создание отчета по товарам
    result = requests.post(f"{ozon_seller_api_url}/v1/report/products/create",headers=headers, data=params).json()
    logger.info('Get name product report')
    code_id = json.dumps({
    "code": result["result"]["code"]
    })

    # Ждем 5 секунд, пока отчет будет создаваться
    time.sleep(5)
    resp_data = {"result":{"file":""}}
    # Делаем запросы получения информации об отчете до тех пор, пока не получим ссылку на отчет
    while not resp_data["result"]["file"]:
        resp_data = requests.post(f"{ozon_seller_api_url}/v1/report/info", headers=headers,data=code_id).json()

    # Получаем ссылку на отчет по товарам
    path_csv = resp_data["result"]["file"]
    logger.info('Get link on product report')

    r = requests.get(path_csv, stream=True)
    logger.info('Upload products on '+ str(date.today()))
    if r.status_code == 200:
        # Считываем отчет по товарам (список товаров)
        df_products = pd.read_csv(path_csv, sep=';')
        # Убираем лишние символы из артикула
        df_products['Артикул'] = df_products['Артикул'].str.replace("'", "", regex=False)
        if to_save:
            df_products.to_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';')
        else:
            return df_products
    else:
        logger.error('Error getting products report')

        # with open(f"{uploaddir_today}/{str(date.today())}_Товары.csv", 'wb') as f:
        # # with open(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Товары'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv', 'wb') as f:
        #     r.raw.decode_content = True
        #     shutil.copyfileobj(r.raw, f)
        # f.close()


# Цены на товары
def get_products_prices(headers, df_products=None):
    # Если список товаров не задан, считываем его из текущей выгрузки
    if df_products is None:
        df_products_ = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';')
    # Если список товаров задан, берем его из переданного аргумента
    else:
        df_products_ = df_products.copy()

    if 'РРЦ' in df_products_.columns:
        df_products_ = df_products_.drop(columns=['РРЦ'])
    # df_products['Артикул'] = df_products['Артикул'].str.replace("'", "", regex=False)
    # df для цен
    df_products_prices = pd.DataFrame()
    # Разбиваем df с товарами на равные части по 100 товаров
    # l = list(range(0, len(df_products), 1000)) + [len(df_products)]
    df_products_ = df_products_.reset_index(drop=True)
    step = 1000
    df_products_['chunks'] = df_products_.index // step + 1
    last_id = ''
    logger.info("Get products prices")
    # for z in zip(l[:-1], l[1:]):
    for chunk in df_products_['chunks'].unique():
        # Выборка 1000 id товаров из df с товарами
        # product_list = df_products.iloc[z[0]:z[1], df_products.columns.get_loc('Ozon Product ID')].to_list()
        product_list = df_products_.loc[df_products_['chunks'] == chunk, 'Ozon Product ID'].to_list()
        product_list_string = [str(element) for element in product_list]
        # Передача списка товаров в параметры запроса
        params = json.dumps({
            "filter": {
                "offer_id": [],
                "product_id": product_list_string,
                "visibility": "ALL"
                },
            "cursor": last_id,
            "limit": 1000
        })
        resp_data = requests.post(f"{ozon_seller_api_url}/v5/product/info/prices", headers=headers,data=params).json()
        # if 'result' in resp_data.keys():
        #     product_id = resp_data['result']['items'][0]['product_id']
        #     product_price = resp_data['result']['items'][0]['price']['marketing_price']
        #     df_products.loc[df_products['Ozon Product ID'] == product_id, 'РРЦ'] = product_price
        # по last_id будет выгружаться следующая страница в следующем проходе цикла
        last_id = resp_data['cursor']
        # В промежуточный df складываем данные текущей страницы
        tmp_df = pd.DataFrame(resp_data['items'])
        # Добавляем строки предыдущей страницы к текущей (по ходу цикла)
        df_products_prices = pd.concat([df_products_prices, tmp_df])

    # Сбрасываем index после concat
    df_products_prices = df_products_prices.reset_index(drop=True)
    # Получаем список цен
    df_products_prices_all = pd.concat([
        df_products_prices.loc[:, ~df_products_prices.columns.isin(['price'])],
        pd.json_normalize(df_products_prices['price']),
    ], axis=1)
    # Переименовываем цены в соответствии с документацией
    df_products_prices_all = df_products_prices_all.rename(columns={
        'offer_id': 'Артикул',
        'product_id': 'Ozon Product ID',
        'marketing_price': 'Цена с учетом всех акций',
        'marketing_seller_price': 'Цена с учетом акций продавца',
        'min_price': 'Минимальная цена после применения всех скидок',
        'old_price': 'Цена до учета скидок (зачеркнутая)',
        'price': 'Цена с учетом скидок (на карточке товара)',
        'retail_price': 'Цена поставщика'
    })
    # Выбираем нужные колонки
    df_products_prices_all = df_products_prices_all.loc[:, df_products_prices_all.columns.isin([
        'Артикул',
        # 'Ozon Product ID',
        'Цена с учетом всех акций',
        'Цена с учетом акций продавца',
        'Минимальная цена после применения всех скидок',
        'Цена до учета скидок (зачеркнутая)',
        'Цена с учетом скидок (на карточке товара)',
        'Цена поставщика'
    ])]
    # Объединяем с df товаров
    df_products_with_prices = (
        df_products_
        .loc[:, df_products_.columns.isin(['Артикул'])]
        .merge(
            df_products_prices_all,
            how='left',
            on='Артикул'
        )
    )
    # Если стоит флаг сохранения, то сохраняем файл
    # if to_save:
    #     df_products_with_prices.to_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';', index=False)
    # # иначе: возвращаем df с товарами и ценами
    # else:
    return df_products_with_prices


# Характеристики товаров
def get_products_attributes(headers, df_products=None):

    logger.info("Getting products attributes")

    # Если список товаров не задан, считываем его из текущей выгрузки
    if df_products is None:
        df_products_ = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';')
    # Если список товаров задан, берем его из переданного аргумента
    else:
        df_products_ = df_products.copy()

    # df, в который будем помещать результаты метода АПИ
    df_products_attributes = pd.DataFrame()

    # Разбиваем df с товарами на равные части по 100 товаров
    df_products_ = df_products_.reset_index(drop=True)
    step = 1000
    df_products_['chunks'] = df_products_.index // step + 1
    # Пагинация
    last_id = ''
    # Цикл по каждому интервалу
    for chunk in df_products_['chunks'].unique():
        # Выборка 1000 id товаров из df с товарами
        product_list = df_products_.loc[df_products_['chunks'] == chunk, 'Ozon Product ID'].to_list()
        product_list_string = [str(element) for element in product_list]
        # Передача списка товаров в параметры запроса
        params = json.dumps({
            "filter": {
                "offer_id": [],
                "product_id": product_list_string,
                "sku": [],
                "visibility": "ALL"
                },
            "last_id": last_id,
            "sort_by": 'offer_id',
            "sort_id": 'asc',
            "limit": 1000
        })
        resp_data = requests.post(f"{ozon_seller_api_url}/v4/product/info/attributes", headers=headers,data=params).json()
        last_id = resp_data['last_id']
        # В промежуточный df складываем данные текущей страницы
        tmp_df = pd.DataFrame(resp_data['result'])
        # Добавляем строки предыдущей страницы к текущей (по ходу цикла)
        df_products_attributes = pd.concat([df_products_attributes, tmp_df])

    # Сбрасываем index после concat
    df_products_attributes = df_products_attributes.reset_index(drop=True)

    # Переименовываем колонки для удобства
    df_products_attributes = df_products_attributes.rename(columns={
        'name': 'Название товара',
        'offer_id': 'Артикул',
        'sku': 'SKU',
        'id': 'Ozon Product ID',
    })
    # Получаем информацию о количестве моделей в артикуле
    df_products_model_info = (
        pd.concat([
            df_products_attributes,
            pd.json_normalize(df_products_attributes['model_info']), # Достаем характеристики из словаря в df
            ],
            axis=1
        )
        .rename(columns={
            'count': 'model_count'
        })
        .loc[:, ['Артикул', 'model_id', 'model_count', 'attributes']]
    )

    # Получаем df с характеристиками товара
    df_attributes_all = (
            df_products_model_info
            # Выбираем нужные колонки
            .loc[:, ['Артикул', 'model_id', 'model_count', 'attributes']]
            # Распаковываем столбец со словарями характеристик товаров
            .explode(['attributes'])
            # Делаем сброс индекса после распаковки
            .reset_index(drop=True)
            # Объединяем df со списком товаров и df с характеристиками
            .pipe(lambda df:
                  pd.concat([
                        df, #.loc[:, ['Артикул']],
                        pd.json_normalize(df['attributes']), # Достаем характеристики из словаря в df
                    ],axis=1)
            )
        # Переименовываем столбец с id характеристики
        .rename(columns={
            'id': 'attribute_id'
        })
        # Распаковываем столбец со словарями значений характеристик товаров
        .explode(['values'])
        # Делаем сброс индекса после распаковки
        .reset_index(drop=True)
        # Объединяем df со списком товаров и df со значениями характеристик товаров
        .pipe(lambda df:
              pd.concat([
                    df, #.loc[:, ['Артикул']],
                    pd.json_normalize(df['values']), # Достаем характеристики из словаря в df
                ], axis=1)
        )
        # Переименовываем столбец со значением характеристики товара
        .rename(columns={
            'value': 'attribute_value'
        })
    )

    # Создаем словарь характеристик товаров
    product_attributes_dict = {
        'Цвет': 10096,
        'Размер': 9533,
    }

    # df, в который будем добавлять характеристики товаров
    df_products_attributes_final = df_products_model_info.copy()

    # Добавляем характеристики в df с товарами
    for attribute_name, attribute_id in product_attributes_dict.items():
        tmp_df_attributes = (
            df_attributes_all
            # Выбираем из df с характеристиками нужную характеристику товара
            .loc[df_attributes_all['attribute_id'] == attribute_id, ['Артикул', 'attribute_value']]
            # Переименовываем столбец со значением характеристики в соответствии со словарем выше
            .rename(columns={
                'attribute_value': attribute_name
            })
            # Делаем группировку по артикулу для избежания дубликатов
            .groupby(['Артикул'])
            # Если на один товар несколько характеристик, делаем их в одну строку,
            # разделяя точкой с запятой
            .agg({
                attribute_name: ';'.join
            })
            # Достаем столбец с артикулом из индекса после группировки
            .reset_index()
        )
        # Мерджим со списком товаров
        df_products_attributes_final = df_products_attributes_final.merge(
            tmp_df_attributes,
            how='left',
            on='Артикул'
        )
        # Заполняем пропуски в атрибутах
        df_products_attributes_final[attribute_name] = df_products_attributes_final[attribute_name].fillna(0)

    # Выбираем нужные колонки
    df_products_attributes_final = (
        df_products_attributes_final
        .loc[:, df_products_attributes_final.columns.isin(
            ['Артикул', 'model_id', 'model_count'] + list(product_attributes_dict.keys())
        )]
    )

    # # Если стоит флаг сохранения, то сохраняем файл в папку текущей выгрузки
    # if to_save:
    #     df_products_with_attributes.to_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';', index=False)
    # # иначе - возвращаем df с товарами
    # else:

    # return  df_products_model_info
    return df_products_attributes_final


# Функция объединения различных данных по товарам
def merge_products_data(df_products, df_products_with_attributes, df_products_with_prices):
    # Создаем список df для джойна
    dfs_to_join = [
        df_products_with_attributes,
        df_products_with_prices
    ]
    # df, в который будем помещать результаты джойна
    df_products_all_data = df_products.copy()
    # Джойним датафреймы в цикле
    for df in dfs_to_join:
        df_products_all_data = df_products_all_data.merge(df, how='left', on='Артикул')

    return df_products_all_data


# Функция выгрузки списка товаров и их характеристик
def get_ozon_product(headers, to_save=True):
    # Выгрузка списка товаров
    df_products = getOzonProduct(headers, to_save=False)
    # Получение цен товаров
    df_products_with_prices = get_products_prices(headers, df_products)
    # Получение характеристик товаров
    df_products_with_attributes = get_products_attributes(headers, df_products)
    # Объединение данных по товарам в один df
    df_products_all_data = merge_products_data(
        df_products,
        df_products_with_attributes,
        df_products_with_prices,
    )
    # Если стоит флаг сохранения, то сохраняем файл в папку текущей выгрузки
    if to_save:
        df_products_all_data.to_csv(
            f"{uploaddir_today}/{str(date.today())}_Товары.csv",
            sep=';',
            encoding='utf-8-sig',
            index=False
        )
    # иначе - возвращаем df с товарами
    else:
        return df_products_all_data


# Отчет по отправлениям (заказы)
def getOrders(headers, date_start, date_end, delivery_schema="fbo", to_save=True):

    # забираем наименование отчета с заказами за установленный период
    params = json.dumps({
        "filter": {
            "processed_at_from": date_start,
            "processed_at_to": date_end,
            "delivery_schema": [
                delivery_schema

            ],
            "sku": [],
            "cancel_reason_id": [],
            "offer_id": "",
            "status_alias": [],
            "statuses": [],
            "title": ""
        },
        "language": "DEFAULT"
    })

    result = requests.post(f"{ozon_seller_api_url}/v1/report/postings/create",headers=headers, data=params).json()
    code_id = json.dumps({
    "code": result["result"]["code"]
    })
    logger.info('Get name orders report')
    logger.info(result["result"]["code"])
    time.sleep(5)

    # забираем ссылку на отчет с закзами
    resp_data = {"result":{"file":""}}
    while not resp_data["result"]["file"]:
        resp_data = requests.post(f"{ozon_seller_api_url}/v1/report/info", headers=headers,data=code_id).json()
    time.sleep(5)
    path_csv = resp_data["result"]["file"]
    logger.info('Get link on orders report')

    # Если стоит флаг сохранения, то сохраняем файл
    if to_save:
        # Пишем заказы во временнй файлик
        r = requests.get(path_csv, stream=True)
        if r.status_code == 200:
            # with open(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы_temp'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv', 'wb') as f:
            df_orders = pd.read_csv(path_csv, sep=';')
            df_orders.to_csv(
                f"{uploaddir_today}/{str(date.today())}_Заказы_temp_{delivery_schema}.csv",
                sep=';',
                encoding='utf-8-sig',
                index=False
            )
            # with open(f"{uploaddir_today}/{str(date.today())}_Заказы_temp_{delivery_schema}.csv", 'wb') as f:
            #     r.raw.decode_content = True
            #     shutil.copyfileobj(r.raw, f)
            # f.close()
    # Если флаг сохранения False, возвращаем df с заказами
    else:
        df_orders = pd.read_csv(path_csv, sep=';')
        return df_orders


# Кластеры отправлений (FBO)
def getFinalOrders_fbo(headers):
    # забираем кластер по номеру заказа
    # df = pd.read_csv(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы_temp'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv',delimiter=";")
    df = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_temp_fbo.csv", sep=";")
    posting_numbers = df['Номер отправления'].to_list()
    # Если были заказы FBO, начинаем выгружать по ним кластеры
    if len(posting_numbers) > 0:
        df['Кластер отправления'] = ''
        df['Кластер доставки'] = ''
        df['Склад'] = ''
        total_posting_count = len(posting_numbers)
        loaded_posting_count = 0
        for number in posting_numbers:
            params = json.dumps({
                "posting_number": number,
                "translit": True,
                "with": {
                "analytics_data": True,
                "financial_data": True
                }
            })


            try:
            # записываем заказы и кластера в файл с заказами
                result = requests.post(f"{ozon_seller_api_url}/v2/posting/fbo/get",headers=headers, data=params).json()
                cluster_from = result['result']['financial_data']['cluster_from']
                cluster_to = result['result']['financial_data']['cluster_to']
                warehouse_name = result['result']['analytics_data']['warehouse_name']
                df.loc[df["Номер отправления"] == number, 'Склад'] = warehouse_name
                df.loc[df["Номер отправления"] == number, 'Кластер доставки'] = cluster_to
                df.loc[df["Номер отправления"] == number, 'Кластер отправления'] = cluster_from
                orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['created_at']))
                # Увеличиваем кол-во загруженных товаров на 1
                loaded_posting_count += 1
                # Сколько осталось выгрузить заказов в процентах
                loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
                logger.info(str(loaded_percent) + ' % ' + "Upload FBO orders on "+ orderDate.replace("['",'').replace("']",''))
                # time.sleep(1)

                #df.to_csv(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv',sep=";", index = False)
                df.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbo.csv", sep=";", index = False)
            except Exception as e:
                print(f"{e}")
                logger.info("Connection is lost. Waiting  2 minites and continue connection")
                time.sleep(120)
                result = requests.post(f"{ozon_seller_api_url}/v2/posting/fbo/get",headers=headers, data=params).json()
                cluster_from = result['result']['financial_data']['cluster_from']
                df.loc[df["Номер отправления"] == number, 'Кластер отправления'] = cluster_from
                cluster_to = result['result']['financial_data']['cluster_to']
                df.loc[df["Номер отправления"] == number, 'Кластер доставки'] = cluster_to
                warehouse_name = result['result']['analytics_data']['warehouse_name']
                df.loc[df["Номер отправления"] == number, 'Склад'] = warehouse_name
                orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['created_at']))
                # Увеличиваем кол-во загруженных товаров на 1
                loaded_posting_count += 1
                # Сколько осталось выгрузить заказов в процентах
                loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
                logger.info(str(loaded_percent) + ' % ' + "Upload FBO orders on "+ orderDate.replace("['",'').replace("']",''))
                # time.sleep(1)
                # df.to_csv(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv',sep=";", index = False)
        # os.remove(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы_temp'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv')
                df.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbo.csv",sep=";", index = False)
        #os.remove(f"{uploaddir_today}/{str(date.today())}_Заказы_temp_fbo.csv")

    # Если заказов FBO не было, сохраняем пустой df
    else:
        df_fbo_orders = pd.DataFrame(columns = ['Артикул', 'Кластер доставки', 'Статус', 'Сумма отправления', 'Объемный вес товаров, кг'])
        df_fbo_orders.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbo.csv",sep=";", index = False)


# Кластеры отправлений (FBS)
def getFinalOrders_fbs(headers):
    df = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_temp_fbs.csv", sep=";")
    df['Кластер отправления'] = ''
    df['Кластер доставки'] = ''
    df['Склад'] = ''
    # Если есть заказы по схеме fbs
    if df.shape[0]> 0:
        posting_numbers = df['Номер отправления'].to_list()
        total_posting_count = len(posting_numbers)
        loaded_posting_count = 0
        for number in posting_numbers:
            params = json.dumps({
            "posting_number": number,
            "with": {
                "analytics_data": True,
                "barcodes": False,
                "financial_data": True,
                "product_exemplars": False,
                "translit": False
            }
        })
            try:
            # записываем заказы и кластера в файл с заказами
                result = requests.post(f"{ozon_seller_api_url}/v3/posting/fbs/get",headers=headers, data=params).json()
                cluster_from = result['result']['financial_data']['cluster_from']
                df.loc[df["Номер отправления"] == number, 'Кластер отправления'] = cluster_from
                cluster_to = result['result']['financial_data']['cluster_to']
                df.loc[df["Номер отправления"] == number, 'Кластер доставки'] = cluster_to
                warehouse_name = result['result']['analytics_data']['warehouse']
                df.loc[df["Номер отправления"] == number, 'Склад'] = warehouse_name
                orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['in_process_at']))
                # Увеличиваем кол-во загруженных товаров на 1
                loaded_posting_count += 1
                # Сколько осталось выгрузить заказов в процентах
                loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
                logger.info(str(loaded_percent) + ' % ' + "Upload FBS orders on "+ orderDate.replace("['",'').replace("']",''))
                # time.sleep(1)

                #df.to_csv(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv',sep=";", index = False)
                df.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbs.csv", sep=";", index = False)
            except Exception as e:
                print(f"{e}")
                logger.info("Connection is lost. Waiting  2 minites and continue connection")
                time.sleep(120)
                result = requests.post(f"{ozon_seller_api_url}/v3/posting/fbs/get",headers=headers, data=params).json()
                cluster_from = result['result']['financial_data']['cluster_from']
                df.loc[df["Номер отправления"] == number, 'Кластер отправления'] = cluster_from
                cluster_to = result['result']['financial_data']['cluster_to']
                df.loc[df["Номер отправления"] == number, 'Кластер доставки'] = cluster_to
                warehouse_name = result['result']['analytics_data']['warehouse']
                df.loc[df["Номер отправления"] == number, 'Склад'] = warehouse_name
                orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['created_at']))
                # Увеличиваем кол-во загруженных товаров на 1
                loaded_posting_count += 1
                # Сколько осталось выгрузить заказов в процентах
                loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
                logger.info(str(loaded_percent) + ' % ' + "Upload FBS orders on "+ orderDate.replace("['",'').replace("']",''))
                # time.sleep(1)
            # df.to_csv(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv',sep=";", index = False)
    # os.remove(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Заказы_temp'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv')
        df.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbs.csv",sep=";", index = False)
    else:
        df.to_csv(f"{uploaddir_today}/{str(date.today())}_Заказы_fbs.csv",sep=";", index = False)

    #os.remove(f"{uploaddir_today}/{str(date.today())}_Заказы_temp_fbs.csv")


# Остатки fbo (старый метод)
def getStockRemainders_fbo(headers):
    logger.info("Upload stock remainders on  "+ str(date.today()))
    # df, куда будет помещаться итоговый результат
    df_stock_rem_fbo = pd.DataFrame()
    # забираем остатки во временный файл
    for i in range(0,10000,1000):
        params = json.dumps({
        "limit": 1000,
        "offset": i,
        "warehouse_type": "ALL"
        })

        result = requests.post(f"{ozon_seller_api_url}/v2/analytics/stock_on_warehouses",headers=headers, data=params)
        # Если есть остатки, то сохраняем результат запроса в файл и начинаем обработку файла
        if result.status_code == 200 and result.json()['result']['rows']:
            remind = result.json()
            with open(f"{uploaddir_today}/{str(date.today())}_Остатки_temp.csv", "a", newline="",encoding="utf-8-sig") as f:
            # with open(f"UploadFiles/UploadFiles_"+str(date.today())+'/'+str(date.today())+'_Остатки_temp'+'_from_'+date_start_file+'_to_'+date_end_file+'.csv', "a", newline="",encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, remind['result']['rows'][0].keys(), delimiter =',')
                w.writeheader()
                for row in remind['result']['rows']:
                    w.writerow(row)
                f.close()
        # Если остатков нет, сохраняем пустой файл
        else:
            df_stock_rem_fbo = pd.DataFrame(columns=['sku','Кластер','Название склада','Артикул','Название товара','Товары в пути','Доступный к продаже товар','Резерв'])
            df_stock_rem_fbo.to_csv(f"{uploaddir_today}/{str(date.today())}_Остатки.csv", sep=";", index = False)

    # Если есть остатки, начинаем обработку файла с остатками
    if os.path.exists(f"{uploaddir_today}/{str(date.today())}_Остатки_temp.csv"):
        headerList = ['sku','Название склада','Артикул','Название товара','Товары в пути','Доступный к продаже товар','Резерв']
        df = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Остатки_temp.csv", delimiter=",")
        if 'idc' in df.columns:
            df.drop(columns=['idc'], inplace=True)
        df = df[df['sku'] != 'sku']
        df.to_csv(f"{uploaddir_today}/{str(date.today())}_Остатки_all_temp.csv", sep=";",header=headerList)
        # джойним остатки с кластерами и записываем в файл с остатками
        colnames=['sku','warehouse_name','item_code','item_name','promised_amount','free_to_sell_amount','reserved_amount']
        df_stok_rem = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Остатки_all_temp.csv", delimiter=";",names=colnames,skiprows=1)
        colnames_df2 = ['warehouse_name', 'cluster']
        df_cluster = pd.read_csv('clasters_warehouse.csv',delimiter=";", names=colnames_df2,skiprows=1)
        data_with_cluster = df_stok_rem.merge(df_cluster, left_on='warehouse_name',right_on='warehouse_name',how='left')
        new_df = data_with_cluster[['sku','cluster','warehouse_name','item_code','item_name','promised_amount','free_to_sell_amount','reserved_amount']]
        headerList = ['sku','Кластер','Название склада','Артикул','Название товара','Товары в пути','Доступный к продаже товар','Резерв']
        # Кластеры, которых нет в списке, помечаем как "Неизвестный кластер"
        new_df['cluster'] = new_df['cluster'].fillna('Неизвестный кластер')
        new_df.to_csv(f"{uploaddir_today}/{str(date.today())}_Остатки.csv",sep=";",header=headerList, index = False)
        os.remove(f"{uploaddir_today}/{str(date.today())}_Остатки_all_temp.csv")
        os.remove(f"{uploaddir_today}/{str(date.today())}_Остатки_temp.csv")
        # Выводим предупреждение, если есть кластеры, отсутствующие в файле
        warehouse_list_stock_rem = pd.DataFrame({'warehouse_name':df_stok_rem['warehouse_name'].unique()})
        df_all_clusters = pd.merge(df_cluster, warehouse_list_stock_rem,
                                on='warehouse_name',
                                how='outer',
                                indicator=True)
        missing_clusters = df_all_clusters.loc[df_all_clusters['_merge'] == 'right_only', 'warehouse_name'].to_list()
        if len(missing_clusters) > 0:
            logger.warning(f"No cluster found for warehouses {missing_clusters}")


# Остатки FBO
def getStockReminders_fbo_v2(headers, df_products=None, to_save=True):

    logger.info("Uploading FBO reminders")

    # Если список товаров не задан, считываем его из текущей выгрузки
    if df_products is None:
        df_products_ = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Товары.csv", sep=';')
    # Если список товаров задан, берем его из переданного аргумента
    else:
        df_products_ = df_products.copy()
    # Удаляем на всякий случай дубликаты
    df_products_ = df_products_.drop_duplicates(subset=['SKU'])
    # Удаляем товары с SKU = 0 (скрытые товары)
    df_products_ = df_products_.loc[~df_products_['SKU'].isin([0, '0']), :]
    # Сбрасываем индекс
    df_products_ = df_products_.reset_index(drop=True)
    # Разбиваем список товаров на диапазоны по 100 шт
    step = 100
    df_products_['chunks'] = (df_products_.index // step) + 1

    # df, куда будем помещать результаты выгрузки АПИ
    df_fbo_reminders_api = pd.DataFrame()

    # Цикл по каждому интервалу
    for chunk in df_products_['chunks'].unique():
        # Делаем выборку одного интервала
        df_products_chunk = df_products_.loc[df_products_['chunks'] == chunk, :]
        # Формируем лист с SKU
        sku_list = df_products_chunk['SKU'].to_list()
        # Переводим SKU в строку
        sku_list = [str(sku) for sku in sku_list]

        # Параметры запроса
        params_fbo_reminders = json.dumps({
            'skus': sku_list
        })

        # Запрос к АПИ
        resp_data_reminders_fbo = requests.post(
            f"{ozon_seller_api_url}//v1/analytics/stocks",
            headers=headers,
            data=params_fbo_reminders
        ).json()

        # Переводим в df
        tmp_df_fbo_reminders = pd.DataFrame(resp_data_reminders_fbo['items'])

        # Объединяем с предыдущим проходом цикла
        df_fbo_reminders_api = pd.concat([df_fbo_reminders_api, tmp_df_fbo_reminders])

    # Переименовываем и выбираем нужные колонки
    rename_dict = {
        'sku': 'SKU',
        'offer_id': 'Артикул',
        'name': 'Наименование товара',
        'cluster_name': 'Кластер',
        'warehouse_name': 'Склад',
        'return_from_customer_stock_count': 'Товары в процессе возврата',
        'requested_stock_count': 'Товары в заявках на поставку',
        'transit_stock_count': 'Товары в пути',
        'available_stock_count': 'Доступный к продаже товар'
    }
    df_fbo_reminders_api = df_fbo_reminders_api.rename(columns=rename_dict)

    df_fbo_reminders = (
        df_fbo_reminders_api
        .loc[:, df_fbo_reminders_api.columns.isin([
            'SKU',
            'Артикул',
            'Наименование товара',
            'Кластер',
            'Склад',
            'Товары в процессе возврата',
            'Товары в заявках на поставку',
            'Товары в пути',
            'Доступный к продаже товар'
        ])]
    )
    # Если пришел пустой df, то сохраняем df с колонками, которые приходят по апи
    if df_fbo_reminders.empty:
        df_fbo_reminders = pd.DataFrame(columns=list(rename_dict.values()))
    # Если стоит флаг сохранения, то сохраняем файл в папку текущей выгрузки
    if to_save:
        df_fbo_reminders.to_csv(f"{uploaddir_today}/{str(date.today())}_Остатки.csv",sep=";", index = False)

    # иначе - возвращаем df с товарами
    else:
        return df_fbo_reminders


# Остатки FBS
def getStockReminders_fbs(headers, to_save=True):
    # Колонки, которые всегда возвращаются по апи в таком порядке
    fbs_reminders_columns = ['Идентификатор склада', 'Название склада', 'Артикул', 'Наименование товара', 'Доступно на моем складе, шт', 'Зарезервировано на моем складе, шт']
    # Получаем список складов FBS
    resp_data_fbs_warehouses= requests.post(f"{ozon_seller_api_url}/v1/warehouse/list", headers=headers).json()
    fbs_warehouses = pd.DataFrame(resp_data_fbs_warehouses['result'])
    # Если нет складов fbs, то сохраняем пустой df
    if fbs_warehouses.shape[0] == 0:
        fbs_reminders = pd.DataFrame(columns=fbs_reminders_columns)
    # Если есть склады FBS, забираем по ним остатки
    else:
        params_warehouses = json.dumps({"language": "DEFAULT",
                                        "warehouseId": fbs_warehouses['warehouse_id'].to_list()})
        result = requests.post(f"{ozon_seller_api_url}/v1/report/warehouse/stock", headers=headers, data=params_warehouses).json()
        code_id = json.dumps({
        "code": result["result"]["code"]
        })
        logger.info('Get name of fbs warehouses report')
        logger.info(result["result"]["code"])
        time.sleep(5)

        # забираем ссылку на отчет с остатками на складе fbs
        resp_data = {"result":{"file":""}}
        while not resp_data["result"]["file"]:
            resp_data = requests.post(f"{ozon_seller_api_url}/v1/report/info", headers=headers, data=code_id).json()
        time.sleep(5)
        path_csv = resp_data["result"]["file"]
        logger.info('Get link on FBS warehouses report')
        df_fbs_reminders = pd.read_excel(path_csv)
        # Если на складах есть остатки, то переименовываем колонки в нормальные и сохраняем в csv
        if df_fbs_reminders.shape[0] > 0:
            df_fbs_reminders.columns = fbs_reminders_columns
        # Если нет, то пустой df
        else:
            df_fbs_reminders = pd.DataFrame(columns=fbs_reminders_columns)

    # Если стоит флаг сохранения, то сохраняем файл в папку текущей выгрузки
    if to_save:
        df_fbs_reminders.to_csv(f"{uploaddir_today}/{str(date.today())}_Остатки_fbs.csv",sep=";", encoding='utf-8-sig', index = False)
    # иначе - возвращаем df с товарами
    else:
        return df_fbs_reminders


# Отчет по транзакциям
def getTransactionReport(headers, date_start, date_end):
    # Разбиваем диапазон дат на периоды по 1 месяцу каждый

    # GPT START ----
    # Преобразование строковых дат в datetime
    dt_start = pd.to_datetime(date_start).tz_localize(None)
    dt_end = pd.to_datetime(date_end).tz_localize(None)

    # Создание списка интервалов
    intervals = []

    # Первый интервал от начальной даты до конца месяца
    first_month_end = (dt_start + pd.DateOffset(months=1)).replace(day=1) - pd.Timedelta(seconds=1)
    if first_month_end > dt_end:
        first_month_end = dt_end

    intervals.append({
        'date_start': dt_start.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'date_end': first_month_end.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'dt_start': dt_start,
        'dt_end': first_month_end
    })

    # Следующий интервал от начала следующего месяца до конца месяца
    current_start = first_month_end + pd.Timedelta(seconds=1)
    while current_start <= dt_end:
        monthly_start = current_start.replace(day=1)
        monthly_end = (monthly_start + pd.DateOffset(months=1)).replace(day=1) - pd.Timedelta(seconds=1)

        if monthly_end > dt_end:
            monthly_end = dt_end

        intervals.append({
            'date_start': monthly_start.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'date_end': monthly_end.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'dt_start': monthly_start,
            'dt_end': monthly_end
        })

        # Переход к следующему месяцу
        current_start = monthly_start + pd.DateOffset(months=1)

    # Создание датафрейма
    date_range_df = pd.DataFrame(intervals)
    # GPT END ----

    # df, в который будем помещать результаты
    df_transaction_list = pd.DataFrame()

    for i in range(date_range_df.shape[0]):
        # Начальные значения для цикла
        page = 1
        page_count = 2
        while page_count > 0:
            # Выгружаем отдельно каждую страницу отчета
            params = json.dumps({
                "filter": {
                    "date": {
                        "from": date_range_df['date_start'][i],
                        "to": date_range_df['date_end'][i]
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all"
                },
                "page": page,
                "page_size": 1000
            })

            resp_data_transaction_list = requests.post(f"{ozon_seller_api_url}/v3/finance/transaction/list", headers=headers, data=params).json()
            # Сколько нужно выгрузить страниц
            page_count =  resp_data_transaction_list['result']['page_count']
            # Увеличиваем страницу 1 для выгрузки следующей страницы
            page = page + 1
            # print(resp_data_transaction_list)
            # Промежуточный df, в который помещаем результаты текущей страницы
            tmp_df = pd.DataFrame(resp_data_transaction_list['result']['operations'])
            # Добавляем даты, за который был выгружен отчет по транзакциям
            tmp_df = tmp_df.assign(
                dt_start=date_range_df['dt_start'][i],
                dt_end=date_range_df['dt_end'][i]
            )
            # Объединяем с предыдущей страницей
            df_transaction_list = pd.concat([df_transaction_list, tmp_df])

    # Убираем дубликаты из index
    df_transaction_list = df_transaction_list.reset_index(drop=True)
    # Количество товаров в одной операции
    df_transaction_list['items_amount'] = df_transaction_list['items'].apply(lambda x: len(x))
    df_transaction_list['services_amount'] = df_transaction_list['services'].apply(lambda x: len(x))
    # Сохраняем csv
    df_transaction_list.to_csv(f"{uploaddir_today}/{str(date.today())}_Список_транзакций.csv", sep=';', index=False)


# Продажи FBO
def getSalesFBO(headers):
    # Считываем файл со списком транзакций
    # BUG: при чтении excel все словари в колонках переводятся в строки. Приходится их переводить обратно при помощи eval()
    df_transaction_list = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Список_транзакций.csv", sep=';')
    df_transaction_list['posting'] = df_transaction_list['posting'].apply(literal_eval)
    # Достаем информацию об отправлениях из списка транзакций
    postings = pd.json_normalize(df_transaction_list['posting'])
    # Добавляем тип (заказ\возврат), стоимость и количество товаров в отправлении
    postings = pd.concat([postings, df_transaction_list[['type', 'operation_date', 'operation_type', 'operation_type_name', 'amount', 'accruals_for_sale', 'items_amount']]], axis=1)

    # Делаем выборку только по FBO и считаем итоговую цену для каждого отправления
    postings_fbo = (
        postings
        # Продажей считаем тип начисления - Доставка покупателю
        .loc[(postings['delivery_schema'] == 'FBO')& (postings['operation_type_name'].isin(['Доставка покупателю'])), :]
        .groupby(['posting_number', 'type', 'order_date'])
        .agg(amount=('amount', 'sum'))
        .reset_index()
    )
    # Делаем фильтр по типу отправления, нужны только заказы (orders) без возвратов
    postings_fbo = (
        postings_fbo
        .loc[postings_fbo['type'] == 'orders', :]
        .drop_duplicates(subset=['posting_number'])
        .sort_values(['order_date'])
    )

    # Если есть продажи по FBO
    if postings_fbo.shape[0] > 0:
        # Выгружаем данные по отправлениям FBO
        sales_fbo = pd.DataFrame()
        loaded_posting_count = 0
        total_posting_count = len(postings_fbo['posting_number'])
        for posting_number in postings_fbo['posting_number']:
            params_fbo = json.dumps({
                        "posting_number": posting_number,
                        "translit": True,
                        "with": {
                        "analytics_data": True,
                        "financial_data": True
                        }
                    })
            result = requests.post(f"{ozon_seller_api_url}/v2/posting/fbo/get",headers=headers, data=params_fbo).json()
            # Получаем дату заказа (для логов)
            orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['created_at']))
            # Достаем из результата данные о товаре
            products_fbo = pd.DataFrame(result['result']['products'])
            products_fbo = products_fbo.assign(
                Номер_заказа=result['result']['order_number'],
                Номер_отправления=result['result']['posting_number'],
                Склад = result['result']['analytics_data']['warehouse_name'],
                Кластер_отправления=result['result']['financial_data']['cluster_from'],
                Кластер_доставки=result['result']['financial_data']['cluster_to'],
                Сумма_отправления=postings_fbo.loc[postings_fbo['posting_number'] == posting_number, 'amount'].values[0],
                Статус=result['result']['status']
            )
            # Объединяем данные с предыдущим заказом
            sales_fbo = pd.concat([sales_fbo, products_fbo])
            # Сколько уже загружено заказов
            loaded_posting_count += 1
            # Сколько осталось выгрузить заказов в процентах
            loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
            logger.info(str(loaded_percent) + ' % ' + "Upload FBO sales from "+ orderDate.replace("['",'').replace("']",''))
            # Делаем паузу 3 с., чтоб ы часто не вызывать метод апи
            # time.sleep(3)

        # Переименовываем некоторые колонки для удобства и убираем лишние
        sales_fbo.rename(columns={'offer_id': 'Артикул',
                                  'name': 'Название товара',
                                  'quantity': 'Количество',
                                  'currency_code': 'Валюта отправления'},
                        inplace=True)
        # Указываем, что схема доставки FBO
        sales_fbo['Схема доставки'] = 'FBO'
        sales_fbo = sales_fbo.loc[:, ~sales_fbo.columns.isin(['price', 'digital_codes'])]
    # Если продаж не было, сохраняем пустой df с продажами
    else:
        colnames_sales_fbo = ['sku', 'Название товара', 'Количество', 'Артикул',
       'Валюта отправления', 'Номер_заказа', 'Номер_отправления', 'Склад',
       'Кластер_отправления', 'Кластер_доставки', 'Сумма_отправления', 'Схема доставки', 'Статус']
        sales_fbo = pd.DataFrame(columns=colnames_sales_fbo)
    sales_fbo.to_csv(f"{uploaddir_today}/{str(date.today())}_Продажи_fbo.csv", sep=';', index=False)


# Продажи FBS
def getSalesFBS(headers):
    # Считываем файл со списком транзакций
    # BUG: при чтении excel все словари в колонках переводятся в строки. Приходится их переводить обратно при помощи eval()
    df_transaction_list = pd.read_csv(f"{uploaddir_today}/{str(date.today())}_Список_транзакций.csv", sep=';')
    df_transaction_list['posting'] = df_transaction_list['posting'].apply(literal_eval)
    # Достаем информацию об отправлениях из списка транзакций
    postings = pd.json_normalize(df_transaction_list['posting'])
    # Добавляем тип (заказ\возврат), стоимость и количество товаров в отправлении
    postings = pd.concat([postings, df_transaction_list[['type', 'operation_date', 'operation_type', 'operation_type_name', 'amount', 'accruals_for_sale', 'items_amount']]], axis=1)

    # Делаем выборку только по FBO и считаем итоговую цену для каждого отправления
    postings_fbs = (
        postings
        # Продажей считаем тип начисления - Доставка покупателю
        .loc[(postings['delivery_schema'] == 'FBS')& (postings['operation_type_name'].isin(['Доставка покупателю'])), :]
        .groupby(['posting_number', 'type', 'order_date'])
        .agg(amount=('amount', 'sum'))
        .reset_index()
    )
    # Делаем фильтр по типу отправления, нужны только заказы (orders) без возвратов
    postings_fbs = (
        postings_fbs
        .loc[postings_fbs['type'] == 'orders', :]
        .drop_duplicates(subset=['posting_number'])
        .sort_values(['order_date'])
    )

    # Если есть продажи по FBS
    if postings_fbs.shape[0] > 0:
        # Выгружаем данные по отправлениям FBO
        sales_fbs = pd.DataFrame()
        loaded_posting_count = 0
        total_posting_count = postings_fbs['posting_number'].shape[0]
        for posting_number in postings_fbs['posting_number']:
            params_fbo = json.dumps({
                        "posting_number": posting_number,
                        "translit": True,
                        "with": {
                        "analytics_data": True,
                        "financial_data": True
                        }
                    })
            result = requests.post(f"{ozon_seller_api_url}/v3/posting/fbs/get",headers=headers, data=params_fbo).json()
            # Получаем дату заказа (для логов)
            orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['in_process_at']))
            # Достаем из результата данные о товаре
            products_fbs = pd.DataFrame(result['result']['products'])
            products_fbs = products_fbs.assign(
                Номер_заказа=result['result']['order_number'],
                Номер_отправления=result['result']['posting_number'],
                Склад = result['result']['analytics_data']['warehouse'],
                Кластер_отправления=result['result']['financial_data']['cluster_from'],
                Кластер_доставки=result['result']['financial_data']['cluster_to'],
                Сумма_отправления=postings_fbs.loc[postings_fbs['posting_number'] == posting_number, 'amount'].values[0],
                Статус=result['result']['status']
            )
            # Объединяем данные с предыдущим заказом
            sales_fbs = pd.concat([sales_fbs, products_fbs])
            # Сколько уже загружено заказов
            loaded_posting_count += 1
            # Сколько осталось выгрузить заказов в процентах
            loaded_percent = round(loaded_posting_count / total_posting_count * 100, 2)
            logger.info(str(loaded_percent) + ' % ' + "Upload FBS sales from "+ orderDate.replace("['",'').replace("']",''))
            # Делаем паузу 3 с., чтоб ы часто не вызывать метод апи
            # time.sleep(3)

        # Переименовываем некоторые колонки для удобства и убираем лишние
        sales_fbs.rename(columns={'offer_id': 'Артикул',
                                  'name': 'Название товара',
                                  'quantity': 'Количество',
                                  'currency_code': 'Валюта отправления'},
                        inplace=True)
        # Указываем, что схема доставки FBS
        sales_fbs['Схема доставки'] = 'FBS'
        sales_fbs = sales_fbs.loc[:, ~sales_fbs.columns.isin(['price', 'digital_codes', 'mandatory_mark', 'dimensions'])]
    # Если продаж не было, сохраняем пустой df с продажами
    else:
        colnames_sales_fbs = ['Артикул', 'Название товара', 'sku', 'Количество', 'Валюта отправления',
       'Номер_заказа', 'Номер_отправления', 'Склад', 'Кластер_отправления',
       'Кластер_доставки', 'Сумма_отправления', 'Статус', 'Схема доставки']
        sales_fbs = pd.DataFrame(columns=colnames_sales_fbs)
    sales_fbs.to_csv(f"{uploaddir_today}/{str(date.today())}_Продажи_fbs.csv", sep=';', index=False)


# Функция выгрузки товаров в заявках на поставку
def get_supply_orders(
        headers,
        states = [
            'ORDER_STATE_DATA_FILLING',
            'ORDER_STATE_IN_TRANSIT',
            'ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE',
            'ORDER_STATE_ACCEPTANCE_AT_STORAGE_WAREHOUSE',
            'ORDER_STATE_READY_TO_SUPPLY'
        ],
        to_save = True
    ):
    # Начальные значения для цикла
    last_supply_order_id = 0 # Номер заявки на поставку из последнего запроса
    supply_order_list = [1] # Список заявок на поставку из апи
    df_supply_orders = pd.DataFrame() # df со списком поставок
    logger.info('Getting supply order list')
    # Пока есть поставки, забираем список поставок
    while len(supply_order_list) > 0:
        params = json.dumps({
            "filter": {
                "states": states
            },
            "paging": {
                "from_supply_order_id": last_supply_order_id,
                "limit": 100
            }
        })
        resp_data = requests.post(f"{ozon_seller_api_url}/v2/supply-order/list", headers=headers,data=params).json()
        # В каждом проходе цикла забираем заявки и объединяем их в один df
        tmp_df = pd.DataFrame(resp_data)
        df_supply_orders = pd.concat([df_supply_orders, tmp_df])
        # Сбрасываем индекс для избежания дубликатов в нем
        df_supply_orders = df_supply_orders.reset_index(drop=True)
        supply_order_list = resp_data['supply_order_id']
        if len(supply_order_list) > 0:
            last_supply_order_id = supply_order_list[-1]


    # Если есть хоть одна заявка, начинаем забирать по ней товары
    if df_supply_orders.shape[0] > 0:
        logger.info('Found supply orders, getting supply orders info')
        # Получаем информацию о заявке на поставку
        df_supply_orders['supply_order_id'] = df_supply_orders['supply_order_id'].astype(int)
        df_supply_orders_info = pd.DataFrame()
        # Разбиваем список заявок на шаги по 50 шт. в каждом шаге
        step = 50
        df_supply_orders['chunks'] = df_supply_orders.index.map(lambda x: int(x/step) + 1)
        # supply_order_list_info = [str(element) for element in supply_order_list_info]
        for chunk in df_supply_orders['chunks'].unique():
            # Выбираем заявки по 50 шт. из общего списка
            supply_order_list_info = df_supply_orders.loc[df_supply_orders['chunks'] == chunk, 'supply_order_id'].to_list()
            params_supply_orders_info = json.dumps({
                "order_ids": supply_order_list_info
            })
            resp_data_supply_orders_info = requests.post(f"{ozon_seller_api_url}/v2/supply-order/get", headers=headers, data=params_supply_orders_info).json()
            # Получаем информацию о поставках и складах
            tmp_df = pd.DataFrame(resp_data_supply_orders_info['orders'])
            tmp_df_warehouses = pd.DataFrame(resp_data_supply_orders_info['warehouses'])
            tmp_df_warehouses = tmp_df_warehouses.rename(columns={'name': 'warehouse_name'})
            # Достаем информацию о поставках
            supplies_info = tmp_df['supplies'].explode().to_frame().reset_index(drop=True)
            supplies_info = pd.json_normalize(supplies_info['supplies'])
            # supplies_info = supplies_info.rename(columns={'storage_warehouse_id': 'warehouse_id'})
            # Получаем информацию о складе хранения
            # supplies_info = supplies_info.merge(tmp_df_warehouses[['warehouse_id', 'warehouse_name']],
            #                                     left_on='storage_warehouse_id',
            #                                     right_on='warehouse_id'
            #                                     how='left')
            # Объединяем с информацией о поставке
            df_supplies_result = pd.concat([
                tmp_df[['supply_order_id', 'supply_order_number', 'state', 'dropoff_warehouse_id']],
                supplies_info
            ], axis=1).reset_index(drop=True)

            # Получаем информацию о складе поставки
            # df_supplies_result = df_supplies_result.merge(
            #     tmp_df_warehouses[['warehouse_id', 'warehouse_name']],
            #     left_on='storage_warehouse_id',
            #     right_on='warehouse_id',
            #     how='left'
            # ).drop(columns='warehouse_id')
            # Объединяем с информацией о поставках из предыдущего прохода цикла
            df_supply_orders_info = pd.concat([df_supply_orders_info, df_supplies_result], axis=0).reset_index(drop=True)

        # Добавляем информацию о кластерах
        # df_clusters = pd.read_csv('clasters_warehouse.csv', sep=';')
        # df_supply_orders_info = df_supply_orders_info.merge(
        #     df_clusters[['warehouse_name', 'cluster']],
        #     how='left',
        #     on='warehouse_name'
        # )
        df_clusters = pd.read_csv(f'{marketplace_dir_name}/scripts/clusters_and_warehouses.csv', sep=';')
        df_supply_orders_info = df_supply_orders_info.merge(
            df_clusters[['warehouse_id', 'warehouse_name', 'cluster_name']],
            how='left',
            left_on='storage_warehouse_id',
            right_on='warehouse_id'
        )
        # Если есть кластеры, которых нет в справочнике, выводим предупреждение
        missing_clusters = df_supply_orders_info.loc[df_supply_orders_info['cluster_name'].isna(), :]
        if missing_clusters.shape[0] > 0:
            warehouse_names_with_no_cluster = missing_clusters['warehouse_name'].to_list()
            logger.warning(f"No cluster found for warehouses {warehouse_names_with_no_cluster}")
        # Заполняем пропущенные кластеры значением "Неизвестный кластер"
        df_supply_orders_info['cluster_name'] = df_supply_orders_info['cluster_name'].fillna('Неизвестный кластер')

        # Получаем информацию о товарах в конкретной поставке
        # Разбиваем список заявок на шаги по 1000 шт. в каждом шаге
        logger.info('Getting supply order items')
        step = 1000
        df_supply_orders_info['chunks'] = df_supply_orders_info.index.map(lambda x: int(x/step) + 1)
        # df, куда будем складывать результат
        df_supply_orders_items = pd.DataFrame()
        # Сколько всего заявок
        supply_orders_count = df_supply_orders_info.shape[0]
        for i in range(df_supply_orders_info.shape[0]):
            # Начальные значение для цикла
            last_id = ''
            has_next = True
            # Сколько осталось выгрузить заявок
            print(f"Выгрузка товаров по {i} из {supply_orders_count} заявок")
            while has_next:
                # Формируем лист с bundle_id
                bundle_id = df_supply_orders_info.iloc[i, df_supply_orders_info.columns.get_loc('bundle_id')]
                params_supply_orders_items = json.dumps({
                                        "bundle_ids": [bundle_id],
                                        "is_asc": True,
                                        "last_id": last_id,
                                        "limit": 100
                                        })
                resp_data_supply_orders_items = requests.post(f"{ozon_seller_api_url}/v1/supply-order/bundle", headers=headers, data=params_supply_orders_items).json()
                # Получаем товары из ответа
                df_items = pd.DataFrame(resp_data_supply_orders_items['items'])
                # Добавляем данные по поставке
                df_items = df_items.assign(
                    supply_order_id = df_supply_orders_info['supply_order_id'][i],
                    supply_order_number = df_supply_orders_info['supply_order_number'][i],
                    supply_id = df_supply_orders_info['supply_id'][i],
                    bundle_id = df_supply_orders_info['bundle_id'][i],
                    warehouse_name = df_supply_orders_info['warehouse_name'][i],
                    cluster=df_supply_orders_info['cluster_name'][i],
                )
                # Объединяем данные с предыдущей итерацией цикла
                df_supply_orders_items = pd.concat([df_supply_orders_items, df_items], axis=0).reset_index(drop=True)
                # Перезаписываем переменные для следующей итерации циклов
                last_id = resp_data_supply_orders_items['last_id']
                has_next = resp_data_supply_orders_items['has_next']
                # Делаем паузу в запросах, т.к. лимит 3 запроса в секунду
                time.sleep(0.4)

        # Добавляем названия кластеров к складам
        # df_clusters = pd.read_csv('clasters_warehouse.csv', sep=',')
        # df_supply_orders_items = df_supply_orders_items.merge(df_clusters[['warehouse_name', 'cluster']],
        #                                                       how='left',
        #                                                       on='warehouse_name')

        # Переименовываем некоторые столбцы для удобства
        df_supply_orders_items = df_supply_orders_items.rename(columns={
            'offer_id': 'Артикул',
            'name': 'Наименование товара',
            'barcode': 'Штрихкод',
            'product_id': 'Ozon Product ID',
            'warehouse_name': 'Склад',
            'cluster': 'Кластер'
        })
        # Выбираем нужные колонки
        df_supply_orders_items = df_supply_orders_items[[
            'sku', 'Артикул',
            'Наименование товара', 'Штрихкод', 'Ozon Product ID',
            'quantity', 'Склад', 'Кластер',
            'supply_order_id', 'supply_order_number', 'supply_id'
        ]]
    # Если нет заявок на поставку, то создаем пустой df
    else:
        df_supply_orders_items = pd.DataFrame(columns=[
            'sku', 'Артикул',
            'Наименование товара', 'Штрихкод', 'Ozon Product ID',
            'quantity', 'Склад', 'Кластер',
            'supply_order_id', 'supply_order_number', 'supply_id'
        ])
    # Если нужно сохранить результат, сохраняем его
    if to_save:
        df_supply_orders_items.to_csv(f"{uploaddir_today}/{str(date.today())}_Поставки.csv", sep=';', index=False)
    # Если нет, возвращаем результат
    else:
        return df_supply_orders_items


# %% Вызов всех функций
if __name__ == '__main__':
    # создаем отдельную папку для текущей выгрузки
    uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
    uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"
    if not os.path.exists(uploaddir):
        os.makedirs(uploaddir, exist_ok=True)
    if not os.path.exists(uploaddir_today):
        # shutil. rmtree(uploaddir_today)
        new_dir = os.makedirs(uploaddir_today, exist_ok=True)
    # days_to_process = read_params()
    # days_to_process = 1
    # date_start, date_end = generateDates()
    date_start, date_end = generate_dates_new()
    # date_end = '2025-07-12T23:59:59.000Z'
    # date_start = '2025-06-13T00:00:00.000Z'
    logger.info(f"Upload Files from Ozon for client {client_name} for dates {date_start} - {date_end}")
    # Обновление кластеров Озон
    update_cluster_list(headers)
    # Сохранение периода выгрузки данных в csv
    save_dates_to_csv(date_start, date_end)
    # Заказы FBO
    getOrders(headers, date_start, date_end, delivery_schema="fbo")
    # Заказы FBS
    getOrders(headers, date_start, date_end, delivery_schema="fbs")
    # Список товаров
    get_ozon_product(headers)
    # Кластер доставки заказов FBO
    getFinalOrders_fbo(headers)
    # Кластер доставки заказов FBS
    getFinalOrders_fbs(headers)
    # Список транзакций
    getTransactionReport(headers, date_start, date_end)
    # Продажи FBO
    getSalesFBO(headers)
    # Продажи FBS
    getSalesFBS(headers)
    # Остатки FBS
    getStockReminders_fbs(headers)
    # Остатки FBO
    # getStockRemainders_fbo(headers)
    getStockReminders_fbo_v2(headers)
    # Поставки FBO
    get_supply_orders(headers)
    logger.info(f"DONE UPLOADING FILES FOR CLIENT {client_name}")
    print(f"\033[44m\033[37mDONE UPLOADING FILES FOR CLIENT {client_name}")
