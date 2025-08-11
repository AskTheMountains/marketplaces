
# %% Определение всех функций
import requests
import json
import time
from datetime import date,datetime,timedelta
import pandas as pd
import shutil
import os
import glob
import csv
import numpy as np
from openpyxl import Workbook
import re
from loguru import logger
import getopt
import sys
pd.options.mode.chained_assignment = None

# Функция форматирования excel с планом акций, отдельный скрипт
from wb.scripts.format_actions_svod import format_excel_actions
# Функция выгрузки списка товаров
from wb.scripts.uploadDataFromWB import getWBProduct, get_prices_WB
# Некоторые константы
from wb.scripts.constants import (
    headers,
    client_name,
    marketplace_dir_name,
    catalog_action_columns,
    svod_actions_cols,
    net_cost_koef
)

# Функция чтения дат формирования отчета
def read_dates_file(date_report_created):
    report_dates = pd.read_csv(f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles/UploadFiles_{date_report_created}/{date_report_created}_dates_from_to.csv", sep=';')
    for col in report_dates:
        report_dates[col] = pd.to_datetime(report_dates[col])
    return report_dates

# Функция чтения файлов с акциями
def read_actions_files(date_report_created):
    filenames = {"path": [], "file_name": [], "action_name": []}
    path_actions = f"{marketplace_dir_name}/Clients/{client_name}/Actions/Данные по акциям/{date_report_created}/*"
    # Считывание файла с путем до него
    for file in glob.glob(path_actions):
        filenames['path'].append(file)
    # Считывание только имени файла
    filenames['file_name'] = [os.path.basename(x) for x in glob.glob(path_actions)]
    # Список возможных вариантов наименований файлов с акциями, после которых идет названия акций
    delimeters = ['Акция_',
                  'Товары_для_исключения_из_акции_',
                  'Товары_для_акции_',
                  'Все_товары_подходящие_для_акции_',
                  'Товары_для_возврата_в_акцию_',
                  'Акция_',
                  'Товары для исключения из акции_',
                  'Товары для акции_',
                  'Все товары подходящие для акции_',
                  'Товары для возврата в акцию_']
    # Соединяем в одну строку
    delim = '|'.join(delimeters) + '|.xlsx'
    # Получение названий акций
    # delim = 'Акция_|Товары_для_исключения_из_акции_|Товары_для_акции_|Все_товары_подходящие_для_акции_|.xlsx'
    for i in range(len(filenames['file_name'])):
        res = re.split(delim, filenames['file_name'][i])
        filenames["action_name"].append(res[1])
    filenames_df = pd.DataFrame(filenames)
    filenames_df['action_number'] = filenames_df.index + 1
    return filenames_df


# Функция чтения списка товаров
def read_products_file(date_report_created):
    # Путь к файлу со списком товаром
    filepath_products = (
        f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
        f"/UploadFiles_{date_report_created}/{date_report_created}_Товары.csv"
    )
    # Считываем df с товарами
    df_products = pd.read_csv(filepath_products, sep=';')

    return df_products


# Функция обработки данных каталога
def process_product_list(df_products):
    # Создаем копию для избежания изменений в оригинальном df
    df_products_processed = df_products.copy()
    # Для некоторых клиентов в размере заменяем точку на запятую
    if client_name in ['KU_And_KU', 'Soyuz']:
        df_products_processed['Размер'] =df_products_processed['Размер'].str.replace(",", ".", regex=False)
        df_products_processed['Размер'] = df_products_processed['Размер'].astype(str).str.replace('.0', '')
        df_products_processed['Размер'] = df_products_processed['Размер'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        df_products_processed['Размер'] = df_products_processed['Размер'].fillna(0)
    # Удаляем дубликаты
    df_products_processed = df_products_processed.drop_duplicates(subset=['Артикул продавца', 'Размер'])
    # Считаем скидку
    df_products_processed['discount'] = np.round(1 - (df_products_processed['discount_price']/df_products_processed['price']), 3)

    return df_products_processed


# Функция получения столбцов из справочной таблицы
def get_columns_from_catalog(df_products_processed):
    catalog = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/catalog/Справочная_таблица_{client_name}_WB.xlsx")
    # Для размера отдельно прописываем, что колонку нужно сделать 0
    if 'Размер' not in catalog.columns:
        catalog['Размер'] = 0
    # Если каких-то колонок не хватает, искуственно создаем их, чтобы не было ошибок
    for col in catalog_action_columns:
        if col not in catalog.columns:
            catalog[col] = np.nan
    # Удаляем товары, где не указан размер
    catalog_reference = catalog.dropna(subset=['Размер'], ignore_index=True)
    # Делаем размер строкой
    catalog_reference['Размер'] = catalog_reference['Размер'].astype(str)
    # У некоторых клиентов делаем размер строковым типом
    if client_name in ['KU_And_KU', 'Soyuz']:
        catalog_reference['Размер'] = catalog_reference['Размер'].astype(str).str.replace('.0', '')
        catalog_reference['Размер'] = catalog_reference['Размер'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        catalog_reference['Размер'] = catalog_reference['Размер'].fillna(0)
    df_products_with_catalog = df_products_processed.merge(catalog_reference[catalog_action_columns],
                                                 how='left',
                                                 on=['Артикул продавца', 'Размер'])
    return df_products_with_catalog


# Функция вычисления доп. столбцов по акциям
def calc_additional_columns(df_products_with_catalog):
    # Создаем копию для избежания изменений в оригинальном df
    df_products_ = df_products_with_catalog.copy()
    # Считаем скидку до РРЦ
    df_products_['Скидка до РРЦ'] = (df_products_['price'] - df_products_['РРЦ']) / df_products_['РРЦ']
    return df_products_

# Функция получения столбцов из файла с метриками
def get_columns_from_metrics(date_report_created, df_products):
    # Считываем файл с метриками за указанную дату
    metrics = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/Metrics/{date_report_created}_МетрикиИтоги.xlsx", sheet_name='summary')
    # Переводим столбец с размером в str
    metrics['Размер'] = metrics['Размер'].astype(str)
    # Добавляем столбцы из файла с метриками
    df_products_with_metrics =pd.merge(
        df_products,
        metrics[['Артикул продавца', 'Размер', 'Заказы', 'Продажи', 'Остатки', 'Остатки_fbs']],
        how='left',
        on=['Артикул продавца', 'Размер']
    )

    return df_products_with_metrics

# Получение остатков за предыдущий день
def add_reminders_prev_day(catalog, date_report_created):
    # Создаем копию для избежания изменений в оригинальном df
    catalog_ = catalog.copy()
    # Вычисление даты предыдущего дня
    date_report_created_ = datetime.strptime(date_report_created, "%Y-%m-%d")
    date_prev_day = datetime.strftime(date_report_created_ - timedelta(days=1), '%Y-%m-%d')
    # Путь до файла с метриками за предыдущий день
    path_metrics = f"{marketplace_dir_name}/Clients/{client_name}/Metrics/{date_prev_day}_МетрикиИтоги.xlsx"
    # Если файл есть
    if os.path.exists(path_metrics):
        # Считываем метрики за пред. день
        metrics_prev_day = pd.read_excel(path_metrics, sheet_name='summary')
        # Переименовываем колонку с остатками для избежания дубликатов
        metrics_prev_day.rename(columns={"Остатки": "Остатки_пред_день"},
                                inplace=True)
        # Объединяем с каталогом
        catalog_ = catalog_.merge(metrics_prev_day[['Ozon Product ID', 'Остатки_пред_день', 'Остатки_fbs_пред_день']],
                              how='left',
                              on='Ozon Product ID')
    # Если файла нет, остатки за предыдущий день вычислить нельзя
    else:
        catalog_['Остатки_пред_день'] = np.nan
    return catalog_


# Функция получения столбцов из файлов с акциями
def get_actions_data(filelist_actions, df_products_with_catalog_metrics):
    catalog_for_actions = df_products_with_catalog_metrics.copy()
    catalog_for_actions = catalog_for_actions.rename(columns={
        'barcode': 'Баркод',
        'price': 'Цена до скидки',
        'discount_price': 'Цена после скидки',
        'discount': 'Скидка WB',
        'Минимальная цена расчетная, руб.': 'Минимальная цена маржинальная, руб.'
    })

    # Колонки с названиями акций
    action_cols = []
    for i in range(len(filelist_actions)):
        # action_number = filelist_actions['action_number'][i]
        action_name = filelist_actions['action_name'][i]
        # Считываем данные по конкретной акции
        action_df = pd.read_excel(filelist_actions['path'][i])
        # Переименовываем некоторые колонки
        action_df = action_df.rename(columns={
            'Артикул поставщика': 'Артикул продавца',
            'Последний баркод': 'barcode',
            'Товар уже участвует в акции': f"Участие в акции {action_name}",
            'Загружаемая скидка для участия в акции': f"Скидка по акции {action_name}",
            'Плановая цена для акции': f"Цена по акции {action_name}",
        })
        # Переводим скидку в числовой формат
        for col in [f"Скидка по акции {action_name}"]:
            action_df[col] = action_df[col].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        # Скидка в долях единицы
        action_df[f"Скидка по акции {action_name}"] = action_df[f"Скидка по акции {action_name}"] / 100
        # Выбираем нужные колонки из файла с акциями
        action_df = action_df.loc[:, ['Артикул продавца', f"Участие в акции {action_name}", f"Скидка по акции {action_name}", f"Цена по акции {action_name}"]]
        # Мерджим со списком товаров
        catalog_for_actions = catalog_for_actions.merge(action_df,
                                                        on='Артикул продавца',
                                                        how='left')
        # Переводим колонки с ценами из справочной таблицы в числовой формат
        for col in ['РРЦ', 'Себестоимость', 'Минимальная цена маржинальная, руб.']:
            if col in catalog_for_actions.columns:
                catalog_for_actions[col] = catalog_for_actions[col].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        # Вычисляем дополнительные колонки для конкретной акции
        catalog_for_actions[f"Скидка от РРЦ по акции {action_name}"] = (catalog_for_actions['РРЦ'] - catalog_for_actions[f"Цена по акции {action_name}"]) / catalog_for_actions['РРЦ']
        catalog_for_actions[f"Разница до мин. цены по акции {action_name}"] = catalog_for_actions[f"Цена по акции {action_name}"] - catalog_for_actions["Минимальная цена маржинальная, руб."]
        # Добавляем колонки с именем акции в лист, чтобы потом включить их в итоговую выборку нужных колонок
        action_cols.extend([
            f'Участие в акции {action_name}',
            f'Скидка по акции {action_name}',
            f'Цена по акции {action_name}',
            f'Скидка от РРЦ по акции {action_name}',
            f"Разница до мин. цены по акции {action_name}"
        ])
        if client_name in ['KU_And_KU', 'Soyuz', 'TRIBE', 'Orsk_Combinat']:
            # Маржинальность по акциям
            catalog_for_actions[f'Расчетная маржа, руб по акции {action_name}'] = catalog_for_actions[f'Цена по акции {action_name}'] - (net_cost_koef * catalog_for_actions[f'Цена по акции {action_name}']) - (catalog_for_actions['Себестоимость'])
            catalog_for_actions[f'Расчетная маржа, % по акции {action_name}'] = catalog_for_actions[f'Расчетная маржа, руб по акции {action_name}'] / catalog_for_actions[f'Цена по акции {action_name}']
            action_cols.extend([f'Расчетная маржа, руб по акции {action_name}',
                                f'Расчетная маржа, % по акции {action_name}'])
    catalog_for_actions["№"] = np.arange(catalog_for_actions.shape[0]) + 1
    return catalog_for_actions, action_cols


# Создание свода по шаблону
def create_svod_for_excel(catalog_with_actions, action_cols, date_report_created, date_start, date_end):
    # Создаем копию для избежания изменений в оригинальном df
    df_actions_svod = catalog_with_actions.copy()
    # Переводим дату выгрузки файлов в datetime
    date_report_created_ = datetime.strptime(date_report_created, '%Y-%m-%d')
    # Определяем колонки, которые должны быть в отчете
    svod_columns = svod_actions_cols + action_cols
    # Если какой-то колонки нет в своде по акциям, создаем её
    for col in svod_columns:
        if col not in df_actions_svod.columns:
           df_actions_svod[col] = np.nan

    # Порядок колонок
    df_actions_svod = df_actions_svod[svod_columns]
    # Переименовываем некоторые колонки для соответствия шаблону
    date_for_columns = date_report_created_.strftime('%d.%m')
    date_for_columns_next = (date_report_created_ + timedelta(days=1)).strftime('%d.%m')
    df_actions_svod = df_actions_svod.rename(columns={
        "Заказы": f"ЗАКАЗЫ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
        "Продажи": f"ПРОДАЖИ с {date_start.strftime('%d.%m')} по {date_end.strftime('%d.%m')}",
        # 'Минимальная цена расчетная, руб.': 'Минимальная цена маржинальная, руб.',
        'Скидка WB': f"Скидка WB {date_for_columns}",
        'Остатки': f"Ост. {date_for_columns}",
        'Остатки_fbs': f"Ост. FBS {date_for_columns}"
        # 'Остатки след. день': f"Ост. {date_for_columns_next}"
    })

    return df_actions_svod

# Сохранение файла excel
def save_excel(catalog_excel, action_list_df, date_report_created):
    # Переименовываем колонки в df со списком акций
    action_list_excel = action_list_df.rename(columns={
        "action_number": "Номер акции",
        "action_name": "Название"
    })
    # Задаем имя файла для сохранения
    filepath_actions_svod = (
        f"{marketplace_dir_name}/Clients/{client_name}/Actions/"
        f"{date_report_created}_Таблица_по_акциям_{client_name}_WB.xlsx"
    )
    # Сохраняем файл по акциям
    with pd.ExcelWriter(filepath_actions_svod) as w:
        catalog_excel.to_excel(w, sheet_name='Акции', index=False, na_rep='')
        action_list_excel[['Номер акции', 'Название']] \
            .to_excel(w, sheet_name='Названия акций', index=False, na_rep='')

    # Возвращаем путь до файла с отчетом


# %% Вызов всех функций
# Дата, в которую были посчитаны метрики
# date_report_created = '2025-05-28'
date_report_created = str(date.today())
logger.info(f"Creating actions svod for client {client_name} for date {date_report_created}")
# Получаем даты выгрузки файлов за указанную дату
report_dates = read_dates_file(date_report_created)
# Считываем имена файлов с данными по акции и достаем названия акций
filelist_actions = read_actions_files(date_report_created)
# Получаем список текущих товаров по АПИ
# df_products_list = read_products_file(date_report_created)
df_products_list = getWBProduct(headers, type_products='not_from_recycle', to_save=False)
# Получаем цены на товары по ПИ
df_products = get_prices_WB(headers, df_products_list, to_save=False)
# Обработка списка товаров
df_products_processed = process_product_list(df_products)
# Добавляем столбцы из справочной таблицы
df_products_with_catalog = get_columns_from_catalog(df_products_processed)
# Считаем доп. столбцы, которые рассчитываются на основании данных справочной таблицы
df_products_with_catalog_stats = calc_additional_columns(df_products_with_catalog)
# Добавляем данные из файла с метриками
df_products_with_catalog_metrics = get_columns_from_metrics(date_report_created, df_products_with_catalog)
# catalog_with_reminders_prev_day = add_reminders_prev_day(catalog_metrics, date_report_created)
# Добавляем столбцы с данными по конкретной акции
svod_actions, action_cols = get_actions_data(filelist_actions, df_products_with_catalog_metrics)
# Создаем итоговый отчет по шаблону
svod_excel_actions = create_svod_for_excel(
    svod_actions,
    action_cols,
    date_report_created,
    report_dates['date_start'][0],
    report_dates['date_end'][0],
)
# Сохраняем отчет в excel
save_excel(svod_excel_actions, filelist_actions, date_report_created)
# Форматируем отчет excel
format_excel_actions(client_name, svod_excel_actions, date_report_created)

# %%
