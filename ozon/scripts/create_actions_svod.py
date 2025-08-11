
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
from openpyxl import Workbook
import re
from loguru import logger
import getopt
import sys
pd.options.mode.chained_assignment = None
# Функция выгрузки каталога
from ozon.scripts.uploadDataFromOzon import get_ozon_product
# Функция форматирования excel с планом акций, отдельный скрипт
from ozon.scripts.format_svod_actions import format_excel_actions
# Файл с некоторыми константами
from ozon.scripts.constants import (
    client_name,
    headers,
    marketplace_dir_name,
    catalog_action_columns,
    svod_actions_cols,
    net_cost_koef
)
# создаем отдельную папку для текущей выгрузки
# uploaddir = f"Clients/{client_name}/UploadFiles"
# if not os.path.exists(uploaddir):
#     os.makedirs(uploaddir)
# uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"
# if os.path.exists(uploaddir_today):
#     shutil. rmtree(uploaddir_today)
# new_dir = os.mkdir(f"{uploaddir}/UploadFiles_"+str(date.today()))


# Функция чтения дат формирования отчета
def read_dates_file(date_report_created):
    report_dates = pd.read_csv(f"Clients/{client_name}/UploadFiles/UploadFiles_{date_report_created}/{date_report_created}_dates_from_to.csv", sep=';')
    for col in report_dates:
        report_dates[col] = pd.to_datetime(report_dates[col])
    return report_dates


# Функция выгрузки списка акций АПИ
def get_action_list(headers, filter_dates=False):
    # Получение списка акций
    result_action_list = requests.get("https://api-seller.ozon.ru/v1/actions",headers=headers).json()
    # print(result_action_list)
    df_action_list = pd.DataFrame(result_action_list['result'])
    # Расчет доп. колонок с датами
    for col in ['date_start', 'date_end']:
        df_action_list[col] = pd.to_datetime(df_action_list[col])
        df_action_list[col + '_excel'] = df_action_list[col].dt.strftime("%d.%m.%Y %H:%M:%S")
    # Фильтрация по датам окончания акции, если стоит флаг
    if filter_dates:
        df_action_list = df_action_list.loc[df_action_list['date_end'] >= '2024-12-01', :]
        df_action_list = df_action_list.reset_index(drop=True)
    # Добавляем колонку с номером акции
    df_action_list['action_number'] = np.arange(df_action_list.shape[0]) + 1

    return df_action_list


# Функция получения товаров, которые можно добавить в акции
def get_available_products_for_actions(df_action_list, headers):

    # df, куда будем складывать результат
    df_products_candidates = pd.DataFrame()

    if len(df_action_list) > 0:
        print("Выгрузка товаров, которые могут участвовать в акции")
        for i in range(len(df_action_list)):
            # Начальные значения для цикла while
            uploaded_products_count = 0  # Сколько выгружено товаров по данной акции
            last_id = ''  # Значение, для перехода на следующую страницу (пагинация)
            total_products_count = 1  # Сколько всего товаров по данной акции (будет получено из запроса далее)
            tmp_df = pd.DataFrame({'A': [1]})
            # Пока есть товары по данной акции, обращаемся к методу апи для выгрузки
            # while uploaded_products_count < total_products_count:
            while not tmp_df.empty:
                params = json.dumps({
                    "action_id": df_action_list['id'][i].astype(str),
                    "limit": 1000,
                    'last_id': last_id,
                })
                # Делаем запрос к АПИ
                result_products_candidates = requests.post('https://api-seller.ozon.ru/v1/actions/candidates', headers=headers, data=params).json()
                # Создаем df с товарами в акции
                tmp_df = pd.DataFrame(result_products_candidates['result']['products'])
                # Добавляем информацию об акции в df
                tmp_df = tmp_df.assign(
                    action_id = df_action_list['id'][i],
                    action_title = df_action_list['title'][i],
                    action_start = df_action_list['date_start'][i],
                    action_end = df_action_list['date_end'][i],
                    potential_products_count = df_action_list['potential_products_count'][i],
                    participating_products_count = df_action_list['participating_products_count'][i],
                    )
                # Объединяем с предыдущим проходом цикла
                df_products_candidates = pd.concat([df_products_candidates, tmp_df])

                # Получаем количество всех товаров по акции
                total_products_count = result_products_candidates['result']['total']
                # Получаем количество выгруженных товаров по акции
                products_amount = len(result_products_candidates['result']['products'])
                # Увеличиваем число выгруженных товаров по акции
                uploaded_products_count += products_amount

                # Получаем id последнего элемента на странице, которое подставим в следующий запрос
                last_id = result_products_candidates['result']['last_id']
                print(
                    f"Акция:{df_action_list['title'][i]}\n",
                    f"Всего товаров: {total_products_count}\n"
                    f"Выгружено {uploaded_products_count } товаров\n"
                )

        # Переименовываем колонку с ID товара в системе Озон
        df_products_candidates = df_products_candidates.rename(columns={
            "id": "Ozon Product ID"
        })
        df_products_candidates['Скидка_по_акции'] = df_products_candidates['max_action_price'] / df_products_candidates['price'] - 1.0

    return df_products_candidates


# Функция получения товаров, которые участвуют в акциях
def get_products_in_actions(df_action_list, headers):
    # Получение товаров, уже участвуют в акции
    df_products_in_actions = pd.DataFrame()
    # Если есть данные по акциям
    if len(df_action_list) > 0:
        print("Выгрузка товаров, которые участвуют в акции")
        # Выгрузка данных по каждой из акции
        for i in range(len(df_action_list)):
            # Начальные значения для цикла while
            uploaded_products_count = 0  # Сколько выгружено товаров по данной акции
            last_id = ''  # Значение, для перехода на следующую страницу (пагинация)
            total_products_count = 1  # Сколько всего товаров по данной акции (будет получено из запроса далее)
            tmp_df = pd.DataFrame({'A': [1]})
            # Пока есть товары по данной акции, обращаемся к методу апи для выгрузки
            # while uploaded_products_count < total_products_count:
            while not tmp_df.empty:
                params = json.dumps({
                    "action_id": df_action_list['id'][i].astype(str),
                    "limit": 1000,
                    'last_id': last_id,
                })
                # Делаем запрос к АПИ
                result_df_products_in_actions = requests.post('https://api-seller.ozon.ru/v1/actions/products', headers=headers, data=params).json()
                # Создаем df с товарами в акции
                tmp_df = pd.DataFrame(result_df_products_in_actions['result']['products'])
                # Добавляем информацию об акции в df
                tmp_df = tmp_df.assign(
                    # Идентификатор акции
                    action_id = df_action_list['id'][i],
                    # Название акции
                    action_title = df_action_list['title'][i],
                    # Даты начала и окончания акции
                    action_start = df_action_list['date_start'][i],
                    action_end = df_action_list['date_end'][i],
                    # Кол-во возможных и участвующих продуктов в данной акции
                    potential_products_count = df_action_list['potential_products_count'][i],
                    participating_products_count = df_action_list['participating_products_count'][i],
                )
                # Объединяем с предыдущим проходом цикла
                df_products_in_actions = pd.concat([df_products_in_actions, tmp_df])
                # Получаем количество всех товаров по акции
                total_products_count = result_df_products_in_actions['result']['total']
                # if i == 6:
                #     products_amount = 73
                # else:
                # Получаем количество выгруженных товаров по акции
                products_amount = len(result_df_products_in_actions['result']['products'])
                # Увеличиваем число выгруженных товаров по акции
                uploaded_products_count += products_amount
                # Получаем id последнего элемента на странице, которое подставим в следующий запрос
                last_id = result_df_products_in_actions['result']['last_id']
                print(
                    f"\nАкция:{df_action_list['title'][i]}\n",
                    f"Всего товаров: {total_products_count}\n"
                    f"Выгружено {uploaded_products_count } товаров"
                )

        # Если есть товары, участвующие в акциях, считаем доп. колонки
        if df_products_in_actions.shape[0] > 0:
            df_products_in_actions.rename(columns={"id": "Ozon Product ID"}, inplace=True)
            df_products_in_actions['Скидка_по_акции'] = df_products_in_actions['action_price'] / df_products_in_actions['price'] - 1.0
        # Если нет, то создаем пустой df
        else:
            df_products_in_actions = pd.DataFrame(columns=[
                'Ozon Product ID', 'price', 'action_price', 'max_action_price',
                'add_mode', 'stock', 'min_stock', 'action_id', 'action_title',
                'action_start', 'action_end', 'potential_products_count',
                'participating_products_count', 'Скидка_по_акции'
            ])

    return df_products_in_actions


# Различные варианты цен
def get_prices(catalog, headers):
    catalog_ = catalog.copy()
    if 'РРЦ' in catalog_.columns:
        catalog_.drop(columns=['РРЦ'], inplace=True)
    # df_products['Артикул'] = df_products['Артикул'].str.replace("'", "", regex=False)
    # df для РРЦ
    df_products_prices = pd.DataFrame()
    # Диапазоны выгрузок (с шагом 1000 до кол-ва товаров)
    step = 1000
    last_id = ''
    catalog_['chunks'] = catalog_.index.map(lambda x: int(x/step) + 1)
    for chunk in catalog_['chunks'].unique():
        # Выборка 1000 id товаров из df с товарами
        product_list = catalog_.loc[catalog_['chunks'] == chunk, 'Ozon Product ID'].to_list()
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
        resp_data = requests.post("https://api-seller.ozon.ru/v5/product/info/prices", headers=headers, data=params).json()
        # if 'result' in resp_data.keys():
        #     product_id = resp_data['result']['items'][0]['product_id']
        #     product_price = resp_data['result']['items'][0]['price']['marketing_price']
        #     df_products.loc[df_products['Ozon Product ID'] == product_id, 'РРЦ'] = product_price
        # по last_id будет выгружаться следующая страница в следующем проходе цикла
        last_id = resp_data['cursor']
        # В промежуточный df складываем данные текущей страницы
        tmp_df = pd.DataFrame(resp_data['items'])
        # Добавляем строки предыдущей страницы к текущей (по ходу цикла)
        df_products_prices = pd.concat([tmp_df, df_products_prices])
    # Получаем нужные цены ('price' должен быть всегда последним в цикле)
    for col in ['marketing_price','old_price', 'min_price', 'price']: # 'marketing_price исключена 26.08.2024
        df_products_prices[col] = [d.get(col) for d in df_products_prices.price]
        df_products_prices[col] = pd.to_numeric(df_products_prices[col], errors='coerce')
        df_products_prices[col] = df_products_prices[col].astype(float)
    # Объединяем с df товаров
    catalog_ = catalog_.merge(df_products_prices[['product_id', 'old_price', 'price', 'min_price']], # 'marketing_price исключена 26.08.2024
                                    how='left',
                                    left_on='Ozon Product ID',
                                    right_on='product_id')
    # Удаляем ненужные колонки
    catalog_.drop(columns=['product_id'], inplace=True)
    catalog_.rename(columns={"marketing_price": "Цена для покупателя",
                            "old_price": "Цена до скидки",
                            "price": "Цена после скидки",
                            "min_price": "Мин. цена Ozon"},
                    inplace=True)
    return catalog_


# Получение каталога
def get_catalog(date_report_created, headers):
    # Считываем список товаров, который пришел по апи
    catalog = pd.read_csv(f"Clients/{client_name}/UploadFiles/UploadFiles_{date_report_created}/{date_report_created}_Товары.csv", sep=';')
    # Убираем ненужные колонки
    catalog = catalog.loc[:, ~catalog.columns.isin([
        'FBS OZON SKU ID', 'Контент-рейтинг', 'Бренд', 'Размер', 'Цвет',
        'Статус товара', 'Видимость FBO', 'Причины скрытия FBO (при наличии)',
        'Видимость FBS', 'Причины скрытия FBS (при наличии)', 'Дата создания',
        'Категория комиссии', 'Объем товара, л', 'Объемный вес, кг',
        'Доступно к продаже по схеме FBO, шт.',
        'Вывезти и нанести КИЗ (кроме Твери), шт', 'Зарезервировано, шт',
        'Доступно к продаже по схеме FBS, шт.',
        'Доступно к продаже по схеме realFBS, шт.',
        'Зарезервировано на моих складах, шт',
        'Актуальная ссылка на рыночную цену', 'Размер НДС, %'
       ])]
    # Переименовываем колонки с ценами, которые приходят из отчета по товарам из апи
    # чтобы потом не было путаницы с ценами, которые мы получаем из отдельного метода
    catalog = catalog.rename(columns={
        'Текущая цена с учетом скидки, ₽': 'Текущая цена с учетом скидки (из отчета по товарам)',
        'Цена до скидки (перечеркнутая цена), ₽': 'Цена до скидки (перечеркнутая цена) (из отчета по товарам)',
        'Рыночная цена, ₽': 'Рыночная цена (из отчета по товарам)'
        })
    # Переименовываем столбец с артикулом
    catalog = catalog.rename(columns={
        'Артикул': 'Артикул продавца'
    })
    # catalog_ = get_prices(catalog, headers)
    # catalog_['№'] = catalog_.index + 1
    # catalog_['Артикул'] = catalog_['Артикул'].str.replace("'", "", regex=False)
    # catalog_ = catalog_.rename(columns={'Артикул': 'Артикул продавца'})
    return catalog

# Обработка данных каталога
def process_catalog_data(catalog):
    # Создаем копию для избежания изменений в оригинальном df
    catalog_processed = catalog.copy()
    # Убираем ненужные колонки
    catalog_processed = catalog_processed.loc[:, ~catalog_processed.columns.isin([
        'FBS OZON SKU ID', 'Контент-рейтинг', 'Бренд', 'Размер', 'Цвет',
        'Статус товара', 'Видимость FBO', 'Причины скрытия FBO (при наличии)',
        'Видимость FBS', 'Причины скрытия FBS (при наличии)', 'Дата создания',
        'Категория комиссии', 'Объем товара, л', 'Объемный вес, кг',
        'Доступно к продаже по схеме FBO, шт.',
        'Вывезти и нанести КИЗ (кроме Твери), шт', 'Зарезервировано, шт',
        'Доступно к продаже по схеме FBS, шт.',
        'Доступно к продаже по схеме realFBS, шт.',
        'Зарезервировано на моих складах, шт',
        'Актуальная ссылка на рыночную цену', 'Размер НДС, %'
       ])]
    # Удаляем лишние символы из артикула
    catalog_processed['Артикул'] = catalog_processed['Артикул'].str.replace("'", "", regex=False)
    # Переводим баркод в число
    catalog_processed['Barcode'] = pd.to_numeric(catalog_processed['Barcode'], errors='coerce')
    catalog_processed['Barcode'] = catalog_processed['Barcode'].apply(lambda x: format(x, 'f') if pd.notnull(x) else x)
    catalog_processed['Barcode'] = catalog_processed['Barcode'].apply(lambda x: str(x).split('.')[0] if pd.notnull(x) else x).astype('Int64')
    # Переименовываем колонки с ценами, которые приходят из отчета по товарам из апи
    # чтобы потом не было путаницы с ценами, которые мы получаем из отдельного метода
    catalog_processed = catalog_processed.rename(columns={
        'Текущая цена с учетом скидки, ₽': 'Текущая цена с учетом скидки (из отчета по товарам)',
        'Цена до скидки (перечеркнутая цена), ₽': 'Цена до скидки (перечеркнутая цена) (из отчета по товарам)',
        'Рыночная цена, ₽': 'Рыночная цена (из отчета по товарам)',
        'Цена Premium, ₽': 'Цена Premium (из отчета по товарам)',
        })
    # Переименовываем колонки с нужными ценами
    catalog_processed = catalog_processed.rename(columns={
        # 'Цена до скидки (перечеркнутая цена) (из отчета по товарам)': 'Цена до скидки',
        # 'Текущая цена с учетом скидки (из отчета по товарам)': 'Цена после скидки',
        # 'Минимальная цена после применения всех скидок': 'Мин. цена Ozon',
        'Цена до учета скидок (зачеркнутая)': 'Цена до скидки',
        'Цена с учетом скидок (на карточке товара)': 'Цена после скидки',
        'Минимальная цена после применения всех скидок': 'Мин. цена Ozon',
    })
    # Переименовываем колонки с характеристиками товара
    catalog_processed = catalog_processed.rename(columns={
        'Артикул': 'Артикул продавца',
        'Название товара': 'Наименование товара',
        'SKU': 'Ozon SKU ID',
        'Barcode': 'Штрихкод',
    })

    return catalog_processed


# Получение колонок и обработка справочной таблицы
def get_catalog_reference_columns(catalog_processed):
    # Загрузка справочной таблицы
    catalog_reference = pd.read_excel(f"Clients/{client_name}/catalog/Справочная_таблица_{client_name}_Ozon.xlsx")
    catalog_reference.rename(columns={"Основной артикул": "Основной Артикул"}, inplace=True)
    # Переименовываем колонки для удобства
    catalog_reference = catalog_reference.rename(columns={
        'Артикул': 'Артикул продавца',
        'Маржинальность, %': 'Желаемая маржинальность, %'
        })
    # Если какой-то из нужных колонок нет, добавляем её, чтобы не было ошибок
    for col in catalog_action_columns:
        if col not in catalog_reference.columns:
            catalog_reference[col] = np.nan
    # Принудительно переводим некоторые колонки в числовой тип, чтобы не было ошибок
    for col in ['Себестоимость', 'Минимальная цена расчетная, руб.', 'РРЦ', 'Желаемая маржинальность, %', 'Нижняя граница цены после скидки, руб', 'Верхняя граница цены после скидки, руб']:
        if col in catalog_reference.columns:
            catalog_reference[col] = catalog_reference[col].apply(lambda x: pd.to_numeric(x, errors='coerce'))
    # Переводим артикул продавца в строку
    catalog_reference['Артикул продавца'] = catalog_reference['Артикул продавца'].astype(str)
    # Объединение со справочной таблицей
    catalog_with_reference = catalog_processed.merge(catalog_reference[catalog_action_columns],
                            how='left',
                            on='Артикул продавца')
    # catalog_ = catalog_.rename(columns={'Артикул продавца': 'Артикул'})
    # Удаляем колонку из справочной таблицы, по которой делали мердж,
    # т.к. она уже есть в списке товаров из апи
    # catalog_ = catalog_.drop(columns=['Артикул продавца'])
    return catalog_with_reference


# Получение заказов, остатков и продаж из файла с метриками
def add_columns_from_metrics(catalog_with_reference, date_report_created):
    # Чтение файла с метриками
    metrics = pd.read_excel(f"Clients/{client_name}/Metrics/{date_report_created}_МетрикиИтоги.xlsx", sheet_name='summary')
    # Создаем копию для избежания изменений в оригинальном df
    catalog_with_metrics = catalog_with_reference.copy()
    # Мерджим список товаров с метриками
    catalog_with_metrics = catalog_with_metrics.merge(metrics[['Ozon Product ID', 'Заказы', 'Продажи', 'Остатки', 'Остатки_fbs', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ']],
                              how='left',
                              on='Ozon Product ID')
    return catalog_with_metrics


# Получение остатков за предыдущий день
def add_reminders_prev_day(catalog, date_report_created):
    catalog_ = catalog.copy()
    # Вычисление даты предыдущего дня
    date_report_created_ = datetime.strptime(date_report_created, "%Y-%m-%d")
    date_prev_day = datetime.strftime(date_report_created_ - timedelta(days=1), '%Y-%m-%d')
    # Путь до файла с метриками за предыдущий день
    path_metrics = f"Clients/{client_name}/Metrics/{date_prev_day}_МетрикиИтоги.xlsx"
    # Если файл есть
    if os.path.exists(path_metrics):
        # Считываем метрики за пред. день
        metrics_prev_day = pd.read_excel(path_metrics, sheet_name='summary')
        # Переименовываем колонку с остатками для избежания дубликатов
        metrics_prev_day.rename(columns={"Остатки": "Остатки_пред_день",
                                         "Остатки_fbs": "Остатки_fbs_пред_день"
                                         },
                                inplace=True)
        # Объединяем с каталогом
        catalog_ = catalog_.merge(metrics_prev_day[['Ozon Product ID', 'Остатки_пред_день', 'Остатки_fbs_пред_день']],
                              how='left',
                              on='Ozon Product ID')
    # Если файла нет, остатки за предыдущий день вычислить нельзя
    else:
        catalog_['Остатки_пред_день'] = np.nan
        catalog_['Остатки_fbs_пред_день'] = np.nan
    return catalog_


# Расчет доп. колонок в каталоге
def calc_catalog_discount_columns(catalog_with_metrics):
    # Расчет столбцов со скидкой
    # catalog['Скидка'] = catalog['Цена после скидки'] / catalog['Цена до скидки'] - 1
    # catalog['Скидка до мин. цены Ozon'] = catalog['Мин. цена Ozon'] / catalog['Цена до скидки'] - 1
    # catalog['Max скидка от цены до скидки, %'] = catalog['Минимальная цена расчетная, руб.'] / catalog['Цена до скидки'] - 1
    # Переименование некоторых колонок для соответствия шаблону
    # catalog = catalog.drop(['Артикул', 'Наименование товара'], axis=1)

    # Создаем копию для избежания изменений в оригинальном df
    catalog_with_discount_columns = catalog_with_metrics.copy()
    catalog_with_discount_columns = catalog_with_discount_columns.rename(columns={
        'Barcode': 'Штрихкод',
        'Название товара': 'Наименование товара',
        'SKU': 'Ozon SKU ID',
        'Минимальная цена расчетная, руб.': 'Min цена маржинальная, руб',
        })

    return catalog_with_discount_columns


# Расчет колонок по акциям
def calc_action_columns(
        catalog_with_discount_columns,
        df_action_list,
        df_products_candidates,
        df_products_in_actions
    ):
    # Создаем копию для избежания изменений в оригинальном df
    svod_actions = catalog_with_discount_columns.copy()
    # Список, в который будем помещать колонки по акциям
    action_cols = []
    for action in df_action_list['id']:
        # Выбираем номер акции для вставки в названия столбцов
        action_name = df_action_list.loc[df_action_list['id'] == action, 'title'].values[0]
        # Выбираем конкретную акцию
        tmp_df_available = df_products_candidates.loc[df_products_candidates['action_id'] == action, :]
        tmp_df_in_action = df_products_in_actions.loc[df_products_in_actions['action_id'] == action, :]
        # Переименовываем столбец со скидкой и ценой по акции, добавляя номер акции
        # Обращаю внимание, что в товарах-кандитатах и участниках цена по акции названа разными именами
        tmp_df_available.rename(columns={"Скидка_по_акции": f"Скидка_по_акции_{action_name}",
                                        "max_action_price": f"Цена_по_акции_{action_name}"},
                                inplace=True)
        tmp_df_in_action.rename(columns={"Скидка_по_акции": f"Скидка_по_акции_{action_name}",
                                        "action_price": f"Цена_по_акции_{action_name}"},
                                inplace=True)
        # Мерджим с товарами, которые могут участвовать в данной акции и которые уже участвуют
        svod_actions = svod_actions.merge(tmp_df_in_action[['Ozon Product ID', f"Скидка_по_акции_{action_name}", f"Цена_по_акции_{action_name}"]],
                                how='left',
                                on='Ozon Product ID',
                                indicator=True)
        svod_actions.rename(columns={'_merge': f'Участвует_в_акции_{action_name}'}, inplace=True)

        svod_actions = svod_actions.merge(tmp_df_available[['Ozon Product ID', f"Скидка_по_акции_{action_name}", f"Цена_по_акции_{action_name}"]],
                                how='left',
                                on='Ozon Product ID',
                                indicator=True)
        svod_actions.rename(columns={'_merge': f'Возможность участия в акции_{action_name}'}, inplace=True)
        # Условия, по которым считаем итоговые колонки
        conditions = [svod_actions[f'Участвует_в_акции_{action_name}'] == 'both',
                      svod_actions[f'Возможность участия в акции_{action_name}'] == 'both']

        choices_in_action = ['Да', 'Нет']

        choices_discount = [svod_actions[f'Скидка_по_акции_{action_name}_x'],
                            svod_actions[f'Скидка_по_акции_{action_name}_y']]
        choices_price = [svod_actions[f'Цена_по_акции_{action_name}_x'],
                        svod_actions[f'Цена_по_акции_{action_name}_y']]
        # Расчет финальных колонок для конкретной акции
        svod_actions[f'Участие в акции {action_name}'] = np.select(conditions, choices_in_action, default=np.nan)
        svod_actions[f'Скидка по акции {action_name}'] = np.select(conditions, choices_discount, default=np.nan)
        svod_actions[f'Цена по акции {action_name}'] = np.select(conditions, choices_price, default=np.nan)
        svod_actions[f'Разница до мин. цены по акции {action_name}'] = svod_actions[f'Цена по акции {action_name}'] - svod_actions['Min цена маржинальная, руб']
        # Замена строковых nan на нормальные nan
        svod_actions[f'Участие в акции {action_name}'] = svod_actions[f'Участие в акции {action_name}'].replace('nan', '')
        # Маржинальность по акциям
        svod_actions[f'Расчетная маржа, руб по акции {action_name}'] = svod_actions[f'Цена по акции {action_name}'] - (net_cost_koef * svod_actions[f'Цена по акции {action_name}']) - (svod_actions['Себестоимость'])
        svod_actions[f'Расчетная маржа, % по акции {action_name}'] = svod_actions[f'Расчетная маржа, руб по акции {action_name}'] / svod_actions[f'Цена по акции {action_name}']
        # Для клиента KU_And_KU применяем доп. логику расчета участия в акциях
        if client_name in ['KU_And_KU', 'Soyuz']:
            # Участие в акции по мин. и макс. границам
            svod_actions[f'Нужно ли добавить товар в акцию {action_name}'] = np.where((svod_actions[f'Участие в акции {action_name}'] == 'Нет') & (svod_actions[f'Цена по акции {action_name}'].between(svod_actions['Нижняя граница цены после скидки, руб'], svod_actions['Верхняя граница цены после скидки, руб'], inclusive='both')),
                                                                                         'Да',
                                                                                         'Нет'
                                                                                         )
            svod_actions[f'Нужно ли убрать товар из акции {action_name}'] = np.where((svod_actions[f'Участие в акции {action_name}'] == 'Да') & (svod_actions[f'Цена по акции {action_name}'].between(svod_actions['Нижняя граница цены после скидки, руб'], svod_actions['Верхняя граница цены после скидки, руб'], inclusive='both')),
                                                                                         'Нет',
                                                                                         'Да'
                                                                                         )
            # У товаров, отсутствующих в акционном списке, данные колонки делаем пустыми
            svod_actions.loc[(svod_actions[f'Участие в акции {action_name}'].isna() ) | (svod_actions[f'Участие в акции {action_name}'].isin(['', 'Да']) ), f'Нужно ли добавить товар в акцию {action_name}'] = np.nan
            svod_actions.loc[(svod_actions[f'Участие в акции {action_name}'].isna() ) | (svod_actions[f'Участие в акции {action_name}'].isin(['', 'Нет']) ), f'Нужно ли убрать товар из акции {action_name}'] = np.nan
            action_cols.extend([f'Нужно ли добавить товар в акцию {action_name}', f'Нужно ли убрать товар из акции {action_name}'])
        # Удаление ненужных колонок
        svod_actions.drop(columns=[f'Участвует_в_акции_{action_name}', f'Возможность участия в акции_{action_name}',
                                    f'Скидка_по_акции_{action_name}_x', f'Скидка_по_акции_{action_name}_y',
                                    f'Цена_по_акции_{action_name}_x', f'Цена_по_акции_{action_name}_y'],
                            inplace=True)
        # Добавляем колонки с именем акции в лист, чтобы потом включить их в итоговую выборку нужных колонок
        action_cols.extend([f'Участие в акции {action_name}',
                        f'Скидка по акции {action_name}',
                        f'Цена по акции {action_name}',
                        f'Разница до мин. цены по акции {action_name}',
                        f'Расчетная маржа, руб по акции {action_name}',
                        f'Расчетная маржа, % по акции {action_name}'])

    return svod_actions, action_cols

# Создание свода по шаблону
def create_svod_for_excel(svod_actions, action_cols, date_report_created, date_start, date_end):
    # Создаем копию для избежания изменений в оригинальном df
    svod_actions_excel = svod_actions.copy()

    # Вычисление даты предыдущего дня
    date_report_created_ = datetime.strptime(date_report_created, "%Y-%m-%d")
    # date_prev_day = date_report_created_ - timedelta(days=1)

    # Расчет некоторых доп. колонок
    # svod_actions_excel['Разница'] = svod_actions_excel['Остатки'] - svod_actions_excel['Остатки_пред_день']
    # svod_actions_excel['Разница fbs'] = svod_actions_excel['Остатки_fbs'] - svod_actions_excel['Остатки_fbs_пред_день']
    svod_actions_excel['Всего остаток'] = svod_actions_excel['Остатки'] + svod_actions_excel['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ']
    # svod_actions_excel['Всего остаток fbs'] = svod_actions_excel['Остатки_fbs'] + svod_actions_excel['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ']

    # Нужные колонки для свода
    svod_cols = svod_actions_cols
    svod_cols = svod_cols + action_cols
    # Если какой-то колонки нет, ее пока оставляем пустой
    for col in svod_cols:
        if not col in svod_actions_excel.columns:
            svod_actions_excel[col] = np.nan
    svod_actions_excel = svod_actions_excel[svod_cols]

    # Переименование некоторых колонок для соответствия шаблону
    svod_actions_excel = svod_actions_excel.rename(columns={
        "Заказы": f"ЗАКАЗЫ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
        "Продажи": f"ПРОДАЖИ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
        "Остатки": f"Ост {date_report_created_.strftime('%d.%m')}",
        "Остатки_fbs": f"Ост fbs {date_report_created_.strftime('%d.%m')}",
        "ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ": "Ожидаемое поступление"
        }
    )

    return svod_actions_excel

# Запись в excel
def save_excel(catalog_excel, df_action_list, date_report_created):
    # Переименовываем некоторые колонки для вставки на лист списка акций
    df_action_list_excel = df_action_list.rename(columns={
        "action_number": "Номер акции",
        "title": "Название",
        "date_start_excel": "Дата начала акции",
        "date_end_excel": "Дата окончания акции"
    })
    # Имя файла для сохранения
    file_name_actions_excel = (
        f"Clients/{client_name}/Actions/"
        f"{date_report_created}_Таблица_по_акциям_{client_name}_Ozon.xlsx"
    )
    with pd.ExcelWriter(file_name_actions_excel) as w:
        catalog_excel.to_excel(w, sheet_name='Акции', index=False, na_rep='')
        df_action_list_excel[['Номер акции', 'Название', "Дата начала акции", "Дата окончания акции"]] \
            .to_excel(w, sheet_name='Названия акций', index=False, na_rep='')


# %% Вызов всех функций
if __name__ == '__main__':
    # Дата, за которую были выгружены данные
    # date_report_created = '2025-04-14'
    date_report_created = str(date.today())
    logger.info(f"Calculating svod actions for client {client_name} for date {date_report_created}")
    # Получение каталога
    # getOzonProduct(headers)
    catalog = get_ozon_product(headers, to_save=False)
    # Обработка данных каталога
    catalog_processed = process_catalog_data(catalog)
    # Получение столбцов из справочной таблицы
    catalog_with_reference = get_catalog_reference_columns(catalog_processed)
    # catalog_with_reference = catalog.copy()
    # Получение столбцов Заказов, Продаж и Остатков из файла с метриками
    catalog_with_metrics = add_columns_from_metrics(catalog_with_reference, date_report_created)
    # Получение остатков за предыдущий день
    # catalog_with_reminders_prev_day = add_reminders_prev_day(catalog_with_metrics, date_report_created)
    # Расчет доп. столбцов со скидками
    catalog_with_discount_columns = calc_catalog_discount_columns(catalog_with_metrics)
    # catalog_with_discount_columns = catalog_processed.copy()
    # Получение списка акций
    df_action_list = get_action_list(headers, filter_dates=False)
    # Получение товаров, доступных для акций и участвующих в акциях
    df_products_candidates = get_available_products_for_actions(df_action_list, headers)
    df_products_in_actions = get_products_in_actions(df_action_list, headers)
    # Расчет столбцов для отдельных акций
    svod_actions, action_cols = calc_action_columns(
        catalog_with_discount_columns,
        df_action_list,
        df_products_candidates,
        df_products_in_actions,
    )
    # Чтение дат, за которые была сделана выгрузка
    report_dates = read_dates_file(date_report_created)
    # Создание df для записи в excel
    svod_actions_excel= create_svod_for_excel(
        svod_actions, action_cols,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0]
    )
    # Сохранение в excel
    save_excel(
        svod_actions_excel,
        df_action_list,
        date_report_created
    )
    # Форматирование файла excel
    format_excel_actions(
        client_name,
        svod_actions_excel,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0]
    )
