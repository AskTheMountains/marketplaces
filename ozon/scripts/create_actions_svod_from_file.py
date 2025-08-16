
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
# Функция выгрузки каталога WB
from wb.scripts.uploadDataFromWB import get_wb_product, get_prices_WB
# Функция форматирования excel с планом акций, отдельный скрипт
from ozon.scripts.format_svod_actions import format_excel_actions
# Файл с некоторыми константами
from ozon.scripts.constants import (
    headers,
    headers_wb,
    client_name,
    marketplace_dir_name,
    catalog_action_columns,
    svod_actions_cols,
    net_cost_koef
)
# Некоторые дополнительные функции
from generic_functions import move_columns, add_element_to_list

# Директория файлов с данными по акциям (обязательно / в конце)
path_download_actions = (
    'd:/artem/downloads/'
)

# Имя файла с данными по акциям (с расширением)
actions_file_name = 'products-1977747(1).xlsx'


# Функция чтения файла с акциями "Бустинг"
def read_action_file(path_download_actions, actions_file_name):
    # Добавляем название файла к пути
    filepath_download_actions = f"{path_download_actions}/{actions_file_name}"
    # Считываем файл по акциям в бустинге
    df_actions_from_file = pd.read_excel(
        filepath_download_actions,
        sheet_name='Товары и цены',
        skiprows=1
        )
    # Убираем первую строку
    df_actions_from_file = df_actions_from_file.iloc[1:]
    # Удаляем лишние колонки
    df_actions_from_file = df_actions_from_file.loc[
        :,
        ~df_actions_from_file.columns.str.contains('Unnamed')
    ]
    # Делаем reset_index после loc
    df_actions_from_file = df_actions_from_file.reset_index(drop=True)

    return df_actions_from_file


# Функция обработки данных файла по акциям
def process_action_file(df_actions_from_file):

    # Создаем копию для избежания изменений в оригинальном df
    df_actions_from_file_processed = df_actions_from_file.copy()

    # Удаляем валюту из колонок
    df_actions_from_file_processed.columns = df_actions_from_file_processed.columns.str.replace(', RUB', '')

    # Формируем словарь с названиями акций в файле из ЛК
    action_column_names = {
       'Бустинг х2 (Из ЛК)': [
           'Цена для получения преимуществ акции Бустинг х2',
        ],
       'Бустинг х3 (Из ЛК)': [
           'Цена для получения преимуществ акции Бустинг х3',
        ],
       'Бустинг х4 (Из ЛК)': [
           'Цена для получения преимуществ акции Бустинг х4',
        ],
       'Максимальный бустинг (Из ЛК)': [
           'Цена для максимального акционного бустинга',
        ],
    }
    # Проверяем, что хотя бы одна акция есть

    # Собираем названия акций, по которым нашлись данные
    action_list_from_file = [
        key
        for key, colnames in action_column_names.items()
        if any(col in df_actions_from_file_processed.columns for col in colnames)
    ]
    if action_list_from_file:
        print(
            f"В файле по акциям найдены колонки по следующим акциям: \n"
            f"{action_list_from_file}"
            )
    else:
        raise ValueError('В файле по акциям не найдены колонки с инфой по акциям')

    # # Соберём все потенциальные имена колонок в один список
    # all_possible_columns = [col for names in action_column_names.values() for col in names]

    # # Проверяем пересечение
    # exists = any(col in df_actions_from_file_processed.columns for col in all_possible_columns)

    # # Выводим найденные колонки
    # matched_columns = list(set(df_actions_from_file_processed.columns) & set(all_possible_columns))

    # if matched_columns:
    #     print(f"Found actions in action file: {matched_columns}")
    # else:
    #     raise ValueError('В файле по акциям не найдены колонки с инфой по акциям')

    # Переименовываем колонки с ценами по акциям
    # Создаём словарь отображения: старое_имя -> новое_имя
    rename_dict = {}
    for new_col, old_names in action_column_names.items():
        for old_name in old_names:
            if old_name in df_actions_from_file_processed.columns:
                rename_dict[old_name] = f"Цена по акции {new_col}"

    # Переименовываем колонку с Ozon Product ID
    df_actions_from_file_processed = df_actions_from_file_processed.rename(columns={
        'OzonID': 'Ozon Product ID'
    })
    # Переименовываем колонки с ценами по акции
    df_actions_from_file_processed = df_actions_from_file_processed.rename(columns=rename_dict)
    # Переводим колонки с ценами по акциям в float
    for key, col in rename_dict.items():
        df_actions_from_file_processed[col] = pd.to_numeric(df_actions_from_file_processed[col])

    # Удаляем лишние символы из колонок с некоторыми ценами
    symbols_to_remove = "*"
    columns_to_clean = ['Минимальная цена']
    for col in columns_to_clean:
        for symbol in symbols_to_remove:
            df_actions_from_file_processed[col] = df_actions_from_file_processed[col].str.replace(symbol, '', regex=False)

    # Переводим колонки с ценами в float
    price_columns = [
        'Цена до скидки',
        'Ваша цена',
        'Текущая цена',
        'Минимальная цена',
        'Итоговая цена по акции',
    ]
    for col in price_columns:
        if col in df_actions_from_file_processed.columns:
            df_actions_from_file_processed[col] = pd.to_numeric(df_actions_from_file_processed[col])

    # Считаем скидку по каждой акции
    for col in action_list_from_file:
        df_actions_from_file_processed[f"Скидка по акции {col}"] = (
            (
                df_actions_from_file_processed[f"Цена по акции {col}"]
                - df_actions_from_file_processed['Ваша цена']
             )
             / df_actions_from_file_processed['Ваша цена']
        )
    # Определяем участие в каждой акции
    for col in action_list_from_file:
        df_actions_from_file_processed[f"Участие в акции {col}"] = np.where(
            df_actions_from_file_processed['Итоговая цена по акции'] <= df_actions_from_file_processed[f"Цена по акции {col}"],
            'Да',
            'Нет'
        )

    return action_list_from_file, df_actions_from_file_processed



# Функция чтения дат формирования отчета
def read_dates_file(date_report_created):
    report_dates = pd.read_csv(f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles/UploadFiles_{date_report_created}/{date_report_created}_dates_from_to.csv", sep=';')
    for col in report_dates:
        report_dates[col] = pd.to_datetime(report_dates[col])
    return report_dates


# Функция получения списка акций из АПИ
def get_action_list(headers, filter_dates=False):
    # Получение списка акций
    result_action_list = requests.get("https://api-seller.ozon.ru/v1/actions",headers=headers).json()
    # print(result_action_list)
    df_action_list = pd.DataFrame(result_action_list['result'])
    # Расчет доп. колонок с датами
    for col in ['date_start', 'date_end']:
        df_action_list[col] = pd.to_datetime(df_action_list[col])
        df_action_list[col + '_excel'] = df_action_list[col].dt.strftime("%d.%m.%Y %H:%M:%S")
    if filter_dates:
        df_action_list = df_action_list.loc[df_action_list['date_end'] >= '2024-12-01', :]
        df_action_list = df_action_list.reset_index(drop=True)
    # Убираем бустинги из списка акций
    actions_to_exclude = [
        # 'Бустинг х2',
        # 'Бустинг х3',
        # 'Бустинг х4',
        # 'Максимальный бустинг!',
        # 'Эластичный бустинг',
        '',
    ]
    # Формируем строку с фильтром
    actions_to_exclude_filter = '|'.join(actions_to_exclude)
    # Убираем лишние акции
    action_list_filtered = df_action_list.loc[
        # ~df_action_list.title.str.contains(actions_to_exclude_filter),
        ~df_action_list.title.isin(actions_to_exclude),
        :
    ]
    # Делаем reset_index после loc
    action_list_filtered = action_list_filtered.reset_index(drop=True)
    # Добавляем префикс к имени акции, отмечая, что данная акция - из АПИ
    action_list_filtered['title'] = action_list_filtered['title'].apply(lambda x: f"{x} (Из API)")
    # Добавляем номер акции
    action_list_filtered['action_number'] = np.arange(action_list_filtered.shape[0]) + 1

    return action_list_filtered


# Функция получения товаров, доступных для участия в акции из АПИ
def get_available_products_for_actions(df_action_list, headers):
# Получение товаров, которые могут участвовать в акции
    products_available_for_actions = pd.DataFrame()
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
                result_products_available_for_actions = requests.post('https://api-seller.ozon.ru/v1/actions/candidates', headers=headers, data=params).json()
                # Создаем df с товарами в акции
                tmp_df = pd.DataFrame(result_products_available_for_actions['result']['products'])
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
                products_available_for_actions = pd.concat([products_available_for_actions, tmp_df])
                # Получаем количество всех товаров по акции
                total_products_count = result_products_available_for_actions['result']['total']
                # Получаем количество выгруженных товаров по акции
                products_amount = len(result_products_available_for_actions['result']['products'])
                # Увеличиваем число выгруженных товаров по акции
                uploaded_products_count += products_amount
                # Получаем id последнего элемента на странице, которое подставим в следующий запрос
                last_id = result_products_available_for_actions['result']['last_id']
                print(
                    f"Акция:{df_action_list['title'][i]}\n",
                    f"Всего товаров: {total_products_count}\n"
                    f"Выгружено {uploaded_products_count } товаров\n"
                )

        products_available_for_actions.rename(columns={"id": "Ozon Product ID"}, inplace=True)
        products_available_for_actions['Скидка_по_акции'] = products_available_for_actions['max_action_price'] / products_available_for_actions['price'] - 1.0
    return products_available_for_actions


# Функция получения товаров, участвующих в акции из АПИ
def get_products_in_actions(df_action_list, headers):
    # Получение товаров, уже участвуют в акции
    products_in_actions = pd.DataFrame()
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
                result_products_in_actions = requests.post('https://api-seller.ozon.ru/v1/actions/products', headers=headers, data=params).json()
                # Создаем df с товарами в акции
                tmp_df = pd.DataFrame(result_products_in_actions['result']['products'])
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
                products_in_actions = pd.concat([products_in_actions, tmp_df])
                # Получаем количество всех товаров по акции
                total_products_count = result_products_in_actions['result']['total']
                # if i == 6:
                #     products_amount = 73
                # else:
                # Получаем количество выгруженных товаров по акции
                products_amount = len(result_products_in_actions['result']['products'])
                # Увеличиваем число выгруженных товаров по акции
                uploaded_products_count += products_amount
                # Получаем id последнего элемента на странице, которое подставим в следующий запрос
                last_id = result_products_in_actions['result']['last_id']
                print(
                    f"\nАкция:{df_action_list['title'][i]}\n",
                    f"Всего товаров: {total_products_count}\n"
                    f"Выгружено {uploaded_products_count } товаров"
                )

        # Если есть товары, участвующие в акциях, считаем доп. колонки
        if products_in_actions.shape[0] > 0:
            products_in_actions.rename(columns={"id": "Ozon Product ID"}, inplace=True)
            products_in_actions['Скидка_по_акции'] = products_in_actions['action_price'] / products_in_actions['price'] - 1.0
        # Если нет, то создаем пустой df
        else:
            products_in_actions = pd.DataFrame(columns=[
                'Ozon Product ID', 'price', 'action_price', 'max_action_price',
                'add_mode', 'stock', 'min_stock', 'action_id', 'action_title',
                'action_start', 'action_end', 'potential_products_count',
                'participating_products_count', 'Скидка_по_акции'
            ])

    return products_in_actions


# Функция объединения списка акций из АПИ и из файла
def add_actions_from_file(df_action_list_api, action_list_from_file):
    # Создаем датафрейм из списка акций из файла
    df_action_list_from_file = pd.DataFrame({
        'title': action_list_from_file
    })
    # Добавляем названия акций из файла в общий датафрейм акций
    df_action_list = pd.concat([
        df_action_list_api,
        df_action_list_from_file
    ], ignore_index=True)
    # Заново нумеруем акции
    df_action_list['action_number'] = np.arange(df_action_list.shape[0]) + 1
    # В колонке id акций из файла ставим номер акции из созданного столбца с id
    df_action_list['id'] = np.where(
        df_action_list['id'].isna(),
        df_action_list['action_number'],
        df_action_list['id']
    )

    return df_action_list


# Функция перевода данных с товарами в акциях в нужный формат
def reformat_actions_file(action_list_from_file, df_action_list_all, df_actions_from_file_processed):

    # df, в который будем помещать результат
    df_actions_from_file_reformatted = pd.DataFrame()
    # Выбираем из df по акциям только те акции, которые пришли из файла
    df_action_list_from_file = df_action_list_all.loc[
        df_action_list_all['title'].isin(action_list_from_file),
        :
    ]
    # Цикл по каждой акции
    for idx, row in df_action_list_from_file.iterrows():
        # Получаем имя и id акции
        action_title = row['title']
        action_id = row['id']
        tmp_df_action_from_file = (
            df_actions_from_file_processed
            # Делаем выборку нужных колонок из файла по акциям
            .loc[
                :,
                [
                    'Ozon Product ID',
                    f'Цена по акции {action_title}',
                    f'Скидка по акции {action_title}',
                    f'Участие в акции {action_title}',
                ]
            ]
            # Переименовываем колонки с акциями
            .rename(columns={
                # f'Цена по акции {action_title}': 'max_action_price',
                f'Скидка по акции {action_title}': 'Скидка_по_акции',
                f'Участие в акции {action_title}': 'Участие_в_акции',
            })

        )
        # Добавляем имя акции
        tmp_df_action_from_file['action_title'] = action_title
        # Добавляем id акции
        tmp_df_action_from_file['action_id'] = action_id
        # Добавляем колонки с ценами для товаров-кандидатов и участников
        tmp_df_action_from_file['action_price'] = tmp_df_action_from_file[f'Цена по акции {action_title}']
        tmp_df_action_from_file['max_action_price'] = tmp_df_action_from_file[f'Цена по акции {action_title}']
        # Убираем лишние колонки
        tmp_df_action_from_file = tmp_df_action_from_file.loc[
            :,
            ~tmp_df_action_from_file.columns.isin([
                                    f'Цена по акции {action_title}',
                                    f'Скидка по акции {action_title}',
                                    f'Участие в акции {action_title}',
            ])
        ]
        # Объединяем с предыдущим проходом цикла
        df_actions_from_file_reformatted = pd.concat([
                df_actions_from_file_reformatted,
                tmp_df_action_from_file
            ], ignore_index=True
        )

    return df_actions_from_file_reformatted


# Функция объединения товаров в по акциям из АПИ и из файла
def union_products_actions_from_api_file(
    df_actions_from_file_reformatted,
    df_products_candidates_api,
    df_products_in_actions_api,
):
    # Делаем выборку из файла по акциям, разделяя товары-кандидаты и участники в акциях
    df_products_candidates_from_file = df_actions_from_file_reformatted.loc[
        df_actions_from_file_reformatted['Участие_в_акции'] == 'Нет',
        :,
    ]
    df_products_in_actions_from_file = df_actions_from_file_reformatted.loc[
        df_actions_from_file_reformatted['Участие_в_акции'] == 'Да',
        :,
    ]
    # Объединяем товары-кандидаты в акции
    df_products_candidates_all = pd.concat([
            df_products_candidates_api,
            df_products_candidates_from_file
        ], ignore_index=True
    )
    # Объединяем товары-участники в акциях
    df_products_in_actions_all = pd.concat([
            df_products_in_actions_api,
            df_products_in_actions_from_file
        ], ignore_index=True
    )

    return df_products_candidates_all, df_products_in_actions_all


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
        'Цена с учетом акций продавца': 'Цена по акции',
        'Цена с учетом всех акций': 'Цена с баллами Озон',
    })
    # Переименовываем колонки с характеристиками товара
    catalog_processed = catalog_processed.rename(columns={
        'Артикул': 'Артикул продавца',
        'Название товара': 'Наименование товара',
        'SKU': 'Ozon SKU ID',
        'Barcode': 'Штрихкод',
    })

    return catalog_processed


# Выгрузка списка товаров WB
def get_catalog_wb(headers_wb):
    # Выгружаем список товаров
    catalog_wb = get_wb_product(headers_wb, to_save=False)
    # Выгружаем цены товаров
    # catalog_wb_with_prices = get_prices_WB(headers_wb, catalog_wb, to_save=False)

    return catalog_wb

# Обработка списка товаров WB
def process_catalog_data_wb(catalog_wb):
    # Создаем копию для избежания изменений в оригинальном df
    catalog_wb_processed = catalog_wb.copy()
    # Переименовываем некоторые колонки
    catalog_wb_processed = catalog_wb_processed.rename(columns={
        'Артикул продавца': 'Артикул WB',
        'price': 'Цена WB',
        'discount_price': 'Цена со скидкой WB'

    })

    # Создаем столбец Артикул + Размер
    catalog_wb_processed['Артикул_Размер_WB'] = (
        catalog_wb_processed[['Артикул WB', 'Размер']]
        .apply(
            lambda row: '_'.join(row.values.astype(str)), axis=1
        )
    )
    # Создаем справочник артикулов Ozon-WB
    if client_name in ['SENS_IP']:
        # Словарь замены артикулов по-простому
        simple_replace = {
            '006/1/Темно_Синий': '006/1_темно_синий',
            '024/': '024_синий',
            '070Ж/синий': '070Ж/_синий',
            '073/_взрослый': '073/взрослый',
            '073/_детский': '073/детский',
            'ЖДЛ02/синий': 'ЖДЛ02синий',
            'ЖДЛ03/синий_синий_бант': 'ЖДЛ03синий_синий_бант',
            'ЖДЛ03/синий_шотландка': 'ЖДЛ/03синий_шотландка',
            'серый_меланж': 'сер меланж',
            'черный.': 'черный',

        }
        # Словарь замены артикулов через регулярку
        regex_replace = {
            r'^006/1/Темно_Синий(_\d{1,3}/\d+)$': r'006/1_темно_синий\1',
            r'^(314)/(.+)[._]?_([0-9]+)/.*': r'\1_\2_\3',
            # r'^ЖДЛ(\d{2})/(.*)': r'ЖДЛ/\1\2',
            # r'^ЖДЛ03/синий_шотландка_(\d{1,3}/\d+)': r'ЖДЛ/03синий_шотландка_\1',
            # r'^ЖДЛ02/синий(_\d{1,3}/\d+)$': r'ЖДЛ02синий\1',
            # r'^ЖДЛ03/синий_синий_бант_(\d{1,3}/\d+)$':r'ЖДЛ03синий_синий_бант_\1',

        }
    elif client_name in ['SENS']:
        # Словарь замены артикулов по-простому
        simple_replace = {
            'Ж1/т.синий_30/122': 'Ж1/т.синий 30/122-128',
            'Ж1/т.синий_32/128': 'Ж1/т.синий 32/128-134',
            'Ж1/т.синий_34/134': 'Ж1/т.синий 34/134-140',
            'Ж1/т.синий_36/140': 'Ж1/т.синий 36/140-146',
            'Ж1/т.синий_38/146': 'Ж1/т.синий 38/146-152',
            'Ж1/т.синий_40/152': 'Ж1/т.синий 40/152-158',
            'Ж1/т.синий_42/158': 'Ж1/т.синий 42/158-164',
            'Ж1/т.синий_44/164': 'Ж1/т.синий 44/164-170',
            'К1/т.синий_30/122': 'К1/т.синий 30/122-128',
            'К1/т.синий_32/128': 'К1/т.синий 32/128-134',
            'К1/т.синий_34/134': 'К1/т.синий 34/134-140',
            'К1/т.синий_36/140': 'К1/т.синий 36/140-146',
            'К1/т.синий_38/146': 'К1/т.синий 38/146-152',
            'К1/т.синий_40/152': 'К1/т.синий 40/152-158',
            'К1/т.синий_42/158': 'К1/т.синий 42/158-164',
            'К1/т.синий_44/164': 'К1/т.синий 44/164-170',
            'П05-110/_60/122-128': 'пиджак вязаный_30',
            'П05-110/_64/128-134': 'пиджак вязаный_32',
            'П05-110/_68/134-140': 'пиджак вязаный_34',
            'П05-110/_72/140-146': 'пиджак вязаный_36',
            'П05-110/_76/146-152': 'пиджак вязаный_38',
            'П05-110/_80/152-158': 'пиджак вязаный_40',
            'П05-110/_84/158-164': 'пиджак вязаный_42',
            'П05-110/_88/164-170': 'пиджак вязаный_44',
            'ПК/белый_30/122-128': 'Поло белый короткий рукав 30/122-128_мал',
            'ПК/белый_32/128-134': 'Поло белый короткий рукав 32/128-134_мал',
            'ПК/белый_34/134-140': 'Поло белый короткий рукав 34/134-140_мал',
            'ПК/белый_36/140-146': 'Поло белый короткий рукав 36/140-146_мал',
            'ПК/белый_38/146-152': 'Поло белый короткий рукав 38/146-152_мал',
            'ПК/белый_40/152-158': 'Поло белый короткий рукав 40/152-158_мал',
            'ПК/белый_42/158-164': 'Поло белый короткий рукав 42/158-164_мал',
            'ПК/белый_44/164-170': 'Поло белый короткий рукав 44/164-170_мал',
            'ПД/голубой_30/122-128': 'Поло голубой длинный рукав 30/122-128_мал',
            'ПД/голубой_32/128-134': 'Поло голубой длинный рукав 32/128-134_мал',
            'ПД/голубой_34/134-140': 'Поло голубой длинный рукав 34/134-140_мал',
            'ПД/голубой_36/140-146': 'Поло голубой длинный рукав 36/140-146_мал',
            'ПД/голубой_38/146-152': 'Поло голубой длинный рукав 38/146-152_мал',
            'ПД/голубой_40/152-158': 'Поло голубой длинный рукав 40/152-158_мал',
            'ПД/голубой_42/158-164': 'Поло голубой длинный рукав 42/158-164_мал',
            'ПД/голубой_44/164-170': 'Поло голубой длинный рукав 44/164-170_мал',
            'ПК/голубой_30/122-128': 'Поло голубой короткий рукав 30/122-128_мал',
            'ПК/голубой_32/128-134': 'Поло голубой короткий рукав 32/128-134_мал',
            'ПК/голубой_34/134-140': 'Поло голубой короткий рукав 34/134-140_мал',
            'ПК/голубой_36/140-146': 'Поло голубой короткий рукав 36/140-146_мал',
            'ПК/голубой_38/146-152': 'Поло голубой короткий рукав 38/146-152_мал',
            'ПК/голубой_40/152-158': 'Поло голубой короткий рукав 40/152-158_мал',
            'ПК/голубой_42/158-164': 'Поло голубой короткий рукав 42/158-164_мал',
            'ПК/голубой_44/164-170': 'Поло голубой короткий рукав 44/164-170_мал',
            'ПД/белый_30/122-128': 'Поло длинный рукав 30/122-128_мал',
            'ПД/белый_32/128-134': 'Поло длинный рукав 32/128-134_мал',
            'ПД/белый_34/134-140': 'Поло длинный рукав 34/134-140_мал',
            'ПД/белый_36/140-146': 'Поло длинный рукав 36/140-146_мал',
            'ПД/белый_38/146-152': 'Поло длинный рукав 38/146-152_мал',
            'ПД/белый_40/152-158': 'Поло длинный рукав 40/152-158_мал',
            'ПД/белый_42/158-164': 'Поло длинный рукав 42/158-164_мал',
            'ПД/белый_44/164-170': 'Поло длинный рукав 44/164-170_мал',
            'ПД/серый_30/122-128': 'Поло серый длинный рукав 30/122-128_мал',
            'ПД/серый_32/128-134': 'Поло серый длинный рукав 32/128-134_мал',
            'ПД/серый_34/134-140': 'Поло серый длинный рукав 34/134-140_мал',
            'ПД/серый_36/140-146': 'Поло серый длинный рукав 36/140-146_мал',
            'ПД/серый_38/146-152': 'Поло серый длинный рукав 38/146-152_мал',
            'ПД/серый_40/152-158': 'Поло серый длинный рукав 40/152-158_мал',
            'ПД/серый_42/158-164': 'Поло серый длинный рукав 42/158-164_мал',
            'ПД/серый_44/164-170': 'Поло серый длинный рукав 44/164-170_мал',
            'ПК/серый_30/122-128': 'Поло серый короткий рукав 30/122-128_мал',
            'ПК/серый_32/128-134': 'Поло серый короткий рукав 32/128-134_мал',
            'ПК/серый_34/134-140': 'Поло серый короткий рукав 34/134-140_мал',
            'ПК/серый_36/140-146': 'Поло серый короткий рукав 36/140-146_мал',
            'ПК/серый_38/146-152': 'Поло серый короткий рукав 38/146-152_мал',
            'ПК/серый_40/152-158': 'Поло серый короткий рукав 40/152-158_мал',
            'ПК/серый_42/158-164': 'Поло серый короткий рукав 42/158-164_мал',
            'ПК/серый_44/164-170': 'Поло серый короткий рукав 44/164-170_мал',
            'ПД/т.синий_30/122-128': 'Поло синий длинный рукав 30/122-128_мал',
            'ПД/т.синий_32/128-134': 'Поло синий длинный рукав 32/128-134_мал',
            'ПД/т.синий_34/134-140': 'Поло синий длинный рукав 34/134-140_мал',
            'ПД/т.синий_36/140-146': 'Поло синий длинный рукав 36/140-146_мал',
            'ПД/т.синий_38/146-152': 'Поло синий длинный рукав 38/146-152_мал',
            'ПД/т.синий_40/152-158': 'Поло синий длинный рукав 40/152-158_мал',
            'ПД/т.синий_42/158-164': 'Поло синий длинный рукав 42/158-164_мал',
            'ПД/т.синий_44/164-170': 'Поло синий длинный рукав 44/164-170_мал',
            'ПК/т.синий_30/122-128': 'Поло синий короткий рукав 30/122-128_мал',
            'ПК/т.синий_32/128-134': 'Поло синий короткий рукав 32/128-134_мал',
            'ПК/т.синий_34/134-140': 'Поло синий короткий рукав 34/134-140_мал',
            'ПК/т.синий_36/140-146': 'Поло синий короткий рукав 36/140-146_мал',
            'ПК/т.синий_38/146-152': 'Поло синий короткий рукав 38/146-152_мал',
            'ПК/т.синий_40/152-158': 'Поло синий короткий рукав 40/152-158_мал',
            'ПК/т.синий_42/158-164': 'Поло синий короткий рукав 42/158-164_мал',
            'ПК/т.синий_44/164-170': 'Поло синий короткий рукав 44/164-170_мал',
        }
        # Словарь замены артикулов через регулярку
        regex_replace = {
            r'\b(euro\d+)_(\d+/\d+)\b': r'\1 \2',
            r'^(В\d+)/блузка_(\d+/\d+-\d+)$': r'\1 \2',
            r'^(ВД\d+)_(\d+)$': r'\1 \2',
            r'^(РДК|РМПП|РМП)/([^_]+)_(\d+/\d+-\d+)$': r'\1/\2 \3',
            r'^СК/т\.синий_(\d+/(\d+)-\d+)$': r'темно-синий \1 \2',
            r'^(СК)/черный_(\d+/\d+-\d+)$': r'\1_черный \2',
        }
    # Создаем столбец, который будет итоговым
    catalog_wb_processed['Артикул_Ozon_WB'] = catalog_wb_processed['Артикул_Размер_WB']

    # Замены по регулярным выражениям:
    catalog_wb_processed['Артикул_Ozon_WB'] = (
        catalog_wb_processed['Артикул_Ozon_WB']
        .replace(regex_replace, regex=True)
    )
    # Простые замены для подстрок во всех строках столбца:
    for repl, replacement in simple_replace.items():
        catalog_wb_processed['Артикул_Ozon_WB'] = (
            catalog_wb_processed['Артикул_Ozon_WB']
            .str.replace(repl, replacement, regex=False)
        )



    return catalog_wb_processed

# Добавление данных по товарам WB
def add_columns_from_wb_catalog(catalog_processed, catalog_wb_processed):
    # Создаем копию для избежания изменений в оригинальном df
    catalog_ozon = catalog_processed.copy()
    catalog_wb_processed_ = catalog_wb_processed.copy()
    # Переименовываем колонки для удобства
    # catalog_ozon = catalog_ozon.rename(columns={
    #     'Артикул продавца': 'Артикул Ozon'
    # })
    # Создаем столбцы для мерджа в обоих датафреймах
    catalog_ozon['Артикул_Ozon_WB'] = catalog_ozon['Артикул продавца']
    catalog_wb_processed_['Артикул_Ozon_WB'] = catalog_wb_processed_['Артикул_Ozon_WB']

    # Столбцы, которые берем из списка товаров WB
    wb_catalog_columns = ['Артикул_Ozon_WB', 'Цена со скидкой WB']
    # Джойним столбцы по размеру
    catalog_ozon_wb = catalog_ozon.merge(
        catalog_wb_processed_[wb_catalog_columns],
        on='Артикул_Ozon_WB',
        how='left',
        indicator=True
    )

    # Удаляем вспомогательный столбец с индикатором мерджа
    catalog_ozon_wb = catalog_ozon_wb.drop(columns='_merge')

    return catalog_ozon_wb

# Добавление данных по товарам WB
def add_data_from_wb(headers_wb, catalog_processed):
    # df, куда будем помещать итоговый результат
    catalog_ozon_wb = catalog_processed.copy()
    # Добавляем данные только по определенным клиентам
    if client_name in ['SENS', 'SENS_IP']:
        # Получаем список товаров WB
        catalog_wb = get_catalog_wb(headers_wb)
        # Обрабатываем список товаров WB
        catalog_wb_processed = process_catalog_data_wb(catalog_wb)
        # Добавляем данные из списка товаров WB в Ozon
        catalog_ozon_wb = add_columns_from_wb_catalog(catalog_processed, catalog_wb_processed)

    return catalog_ozon_wb


# Получение колонок и обработка справочной таблицы
def get_catalog_reference_columns(catalog_ozon_wb):
    # Загрузка справочной таблицы
    catalog_reference = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/catalog/Справочная_таблица_{client_name}_Ozon.xlsx")
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
    catalog_with_reference = catalog_ozon_wb.merge(
        catalog_reference[catalog_action_columns],
        how='left',
        on='Артикул продавца'
    )
    # catalog_ = catalog_.rename(columns={'Артикул продавца': 'Артикул'})
    # Удаляем колонку из справочной таблицы, по которой делали мердж,
    # т.к. она уже есть в списке товаров из апи
    # catalog_ = catalog_.drop(columns=['Артикул продавца'])
    return catalog_with_reference


# Получение заказов, остатков и продаж из файла с метриками
def add_columns_from_metrics(catalog_with_reference, date_report_created):
    # Чтение файла с метриками
    metrics = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/Metrics/{date_report_created}_МетрикиИтоги.xlsx", sheet_name='summary')
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
        'Минимальная цена расчетная, руб.': 'Min цена маржинальная, руб',
        })

    return catalog_with_discount_columns


# Функция добавления цены, по которой мы определяем участие в бустингах
def add_boosting_price_to_catalog(df_actions_from_file_processed, catalog_with_discount_columns):
    # Создаем копию для избежания изменений в оригинальном df
    df_boosting_price = df_actions_from_file_processed.copy()
    # Переименовываем колонку с артикулом для мерджа
    df_boosting_price = df_boosting_price.rename(columns={
        'Артикул': 'Артикул продавца',
        'Итоговая цена по акции': 'Цена для участия в акциях Бустинг'
    })
    # Убираем ненужные колонки
    df_boosting_price = df_boosting_price.loc[
        :,
        ['Артикул продавца', 'Цена для участия в акциях Бустинг']
    ]
    # Мерджим датафрейм с акциями из файла и список товаров
    catalog_with_boosting_prices = (
        catalog_with_discount_columns.merge(
            df_boosting_price,
            how='left',
            on='Артикул продавца'
        )
    )
    # Перемещаем колонку ко всем ценам
    catalog_with_boosting_prices = move_columns(
        catalog_with_boosting_prices,
        ['Цена для участия в акциях Бустинг'],
        'Цена поставщика',
        insert_type='after'
    )

    return catalog_with_boosting_prices

# Расчет колонок по акциям
def calc_action_columns(
        catalog_with_boosting_prices,
        df_action_list_all,
        df_products_candidates_all,
        df_products_in_actions_all
    ):
    # Создаем копию для избежания изменений в оригинальном df
    svod_actions = catalog_with_boosting_prices.copy()
    # Список, в который будем помещать колонки по акциям
    action_cols = []
    for action in df_action_list_all['id']:
        # Выбираем номер акции для вставки в названия столбцов
        action_name = df_action_list_all.loc[df_action_list_all['id'] == action, 'title'].values[0]
        # Выбираем конкретную акцию
        tmp_df_available = df_products_candidates_all.loc[df_products_candidates_all['action_id'] == action, :]
        tmp_df_in_action = df_products_in_actions_all.loc[df_products_in_actions_all['action_id'] == action, :]
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
        svod_actions[f'Участие в акции {action_name}'] = np.select(conditions, choices_in_action, default='nan')
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
    # Добавляем колонку с ценой по акциям в бустинге
    svod_cols = add_element_to_list(
        svod_cols,
        'Цена после скидки',
        ['Цена для участия в акциях Бустинг'],
        after=True
    )
    # Добавляем цену WB
    if client_name in ['SENS', 'SENS_IP']:
        svod_cols = add_element_to_list(
            svod_cols,
            'Цена после скидки',
            ['Цена со скидкой WB'],
            after=True
        )

    svod_cols = svod_cols + action_cols
    # Если какой-то колонки нет, ее пока оставляем пустой
    for col in svod_cols:
        if not col in svod_actions_excel.columns:
            svod_actions_excel[col] = np.nan
    svod_actions_excel = svod_actions_excel[svod_cols]

    # Переименование некоторых колонок для соответствия шаблону
    svod_actions_excel = svod_actions_excel.rename(columns={"Заказы": f"ЗАКАЗЫ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
                                                      "Продажи": f"ПРОДАЖИ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
                                                      "Остатки": f"Ост {date_report_created_.strftime('%d.%m')}",
                                                      "Остатки_fbs": f"Ост fbs {date_report_created_.strftime('%d.%m')}",
                                                      "ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ": "Ожидаемое поступление"
                                                      })

    return svod_actions_excel

# Запись в excel
def save_excel(svod_actions_excel, df_action_list, date_report_created):
    # Переименовываем некоторые колонки для вставки на лист списка акций
    df_action_list_excel = df_action_list.rename(columns={
        "action_number": "Номер акции",
        "title": "Название",
        "date_start_excel": "Дата начала акции",
        "date_end_excel": "Дата окончания акции"
    })
    # Имя файла для сохранения
    file_name_actions_excel = (
        f"{marketplace_dir_name}/Clients/{client_name}/Actions/"
        f"{date_report_created}_Таблица_по_акциям_{client_name}_с_Бустингами_Ozon.xlsx"
    )
    with pd.ExcelWriter(file_name_actions_excel) as w:
        svod_actions_excel.to_excel(w, sheet_name='Акции', index=False, na_rep='')
        df_action_list_excel[['Номер акции', 'Название', "Дата начала акции", "Дата окончания акции"]] \
            .to_excel(w, sheet_name='Названия акций', index=False, na_rep='')


# %% Вызов всех функций
if __name__ == '__main__':
    # Дата, за которую были выгружены данные
    # date_report_created = '2025-04-14'
    date_report_created = str(date.today())
    # Считываем данные по акциям из файла
    df_actions_from_file = read_action_file(path_download_actions, actions_file_name)
    # Обрабатываем файл по акциям
    action_list_from_file, df_actions_from_file_processed = process_action_file(df_actions_from_file)


    logger.info(f"Calculating svod actions for client {client_name} for date {date_report_created}")
    # Получение каталога
    # getOzonProduct(headers)
    # catalog = get_catalog(date_report_created, headers)
    catalog = get_ozon_product(headers, to_save=False)
    # Получение каталога WB
    # catalog_wb = getWBProduct(headers_wb, to_save=False)
    # Обработка данных каталога
    catalog_processed = process_catalog_data(catalog)
    # Добавляем данные из каталога WB
    catalog_ozon_wb = add_data_from_wb(headers_wb, catalog_processed)
    # Получение столбцов из справочной таблицы
    catalog_with_reference = get_catalog_reference_columns(catalog_ozon_wb)
    # catalog_with_reference = catalog.copy()
    # Получение столбцов Заказов, Продаж и Остатков из файла с метриками
    catalog_with_metrics = add_columns_from_metrics(catalog_with_reference, date_report_created)
    # Получение остатков за предыдущий день
    # catalog_with_reminders_prev_day = add_reminders_prev_day(catalog_with_metrics, date_report_created)
    # Расчет доп. столбцов со скидками
    catalog_with_discount_columns = calc_catalog_discount_columns(catalog_with_metrics)
    # catalog_with_discount_columns = catalog_processed.copy()
    # Добавляем цену для определения участия в акциях бустинг
    catalog_with_boosting_prices = add_boosting_price_to_catalog(
        df_actions_from_file_processed,
        catalog_with_discount_columns
    )

    # Получение списка акций
    df_action_list_api = get_action_list(headers, filter_dates=False)
    # Получение товаров, доступных для акций и участвующих в акциях
    df_products_candidates_api = get_available_products_for_actions(df_action_list_api, headers)
    df_products_in_actions_api = get_products_in_actions(df_action_list_api, headers)

    # Объединяем список акций из АПИ и из файла
    df_action_list_all = add_actions_from_file(
        df_action_list_api,
        action_list_from_file
    )

    # Переводим таблицу из файла по акциям под формат АПИ
    df_actions_from_file_reformatted = reformat_actions_file(
        action_list_from_file,
        df_action_list_all,
        df_actions_from_file_processed
    )
    # Объединяем товары в акциях из АПИ и из файла
    df_products_candidates_all, df_products_in_actions_all = union_products_actions_from_api_file(
        df_actions_from_file_reformatted,
        df_products_candidates_api,
        df_products_in_actions_api,
    )
    # Расчет столбцов для отдельных акций
    svod_actions, action_cols = calc_action_columns(
        catalog_with_boosting_prices,
        df_action_list_all,
        df_products_candidates_all,
        df_products_in_actions_all
    )
    # Чтение дат, за которые была сделана выгрузка
    report_dates = read_dates_file(date_report_created)
    # Создание df для записи в excel
    svod_actions_excel = create_svod_for_excel(
        svod_actions, action_cols,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0]
    )
    # Сохранение в excel
    save_excel(
        svod_actions_excel,
        df_action_list_all,
        date_report_created
    )
    # Форматирование файла excel
    format_excel_actions(
        client_name,
        svod_actions_excel,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0],
        boostings_file=True
    )

# %%
