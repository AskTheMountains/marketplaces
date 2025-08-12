
# %% Определение функций
import requests
import json
import time
from datetime import date,datetime,timedelta
import pandas as pd
import shutil
import os
import glob
import csv
import zipfile
from zipfile import ZipFile
import numpy as np
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Border, Side
from openpyxl.styles.numbers import FORMAT_NUMBER_00
import re
from loguru import logger
import getopt
import sys
from pathlib import Path

# Папка, где лежит текущий скрипт
BASE_DIR = Path(__file__).parent.parent


# Файл с настройками и номером клиента
# from options import settings, client_number, headers
# Некоторые константы
from ozon.scripts.constants import (
    headers,
    client_name,
    marketplace_dir_name,
    client_id_performance,
    client_secret_performance,
    ozon_performance_api_url
)
# Функции выгрузки данных по API Seller
from ozon.scripts.uploadDataFromOzon import(
    get_ozon_product,
    getOrders,
    getStockReminders_fbo_v2,
    getStockReminders_fbs
)
# Функции выгрузки данных по API Performane
from ozon.scripts.uploadDataFromOzonPerformance import (
    getAuthorizationToken,
    getCompanyList,
    getCompanyStatistics,
)
# Некоторые вспомогательные функции
from generic_functions import move_columns


# Функция выгрузки отчета о заказах
def get_orders(headers, date_start, date_end):
    # Выгружаем отчет об отправлениях fbo и fbs
    df_orders_fbo = getOrders(headers, date_start, date_end, delivery_schema='fbo', to_save=False)
    df_orders_fbs = getOrders(headers, date_start, date_end, delivery_schema='fbs', to_save=False)
    # Убираем лишние колонки из отчета fbo
    df_orders_fbo = df_orders_fbo.loc[:, ~df_orders_fbo.columns.isin(['Объемный вес товаров, кг'])]

    # Объединяем два отчета в один
    common_columns = ['Артикул', 'Наименование товара', 'OZON id', 'Статус', 'Принят в обработку', 'Сумма отправления']

    # GPT START----
    dfs = []
    for df in [df_orders_fbo, df_orders_fbs]:
        if df is not None and not df.empty:
            # Берем только те common_columns, которые реально есть
            cols = [col for col in common_columns if col in df.columns]
            dfs.append(df.loc[:, cols])

    if dfs:
        df_orders_all = pd.concat(dfs, ignore_index=True)
    else:
        # Если оба датафрейма пустые — создаём пустой датафрейм с нужными колонками
        df_orders_all = pd.DataFrame(columns=common_columns)
    # GPT END----

    # Переводим колонку с датой заказа в datetime
    df_orders_all['datetime_orders'] = pd.to_datetime(df_orders_all['Принят в обработку'])
    # Указываем, какие колонки считаем за заказы в штуках и рублях
    df_orders_all['Заказы шт'] = 1
    df_orders_all['Заказы руб'] = df_orders_all['Сумма отправления']
    # Переименовываем колонку с Ozon ID
    df_orders_all = df_orders_all.rename(columns={
        'OZON id': 'Ozon Product ID'
    })
    # Переводим столбец с артикулом в строку
    df_orders_all['Артикул'] = df_orders_all['Артикул'].astype(str)

    return df_orders_all


# Функция выгрузки остатков потоварно
def get_reminders_by_product(df_products, headers, to_agg=True, agg_col=['Артикул']):
    # Выгружаем остатки FBO и FBS
    df_reminders_fbo = getStockReminders_fbo_v2(headers, df_products, to_save=False)
    df_reminders_fbs = getStockReminders_fbs(headers, to_save=False)
    # Переименовываем колонки для удобства
    df_reminders_fbo = df_reminders_fbo.rename(columns={
        'Доступный к продаже товар': 'Остатки FBO',
    })
    df_reminders_fbs = df_reminders_fbs.rename(columns={
        'Доступно на моем складе, шт': 'Остатки FBS',
    })
    # Объединяем остатки FBO и FBS в один df
    df_reminders_all = pd.concat([
        df_reminders_fbo.loc[:, ['Артикул', 'Остатки FBO']],
        df_reminders_fbs.loc[:, ['Артикул', 'Остатки FBS']],
    ])

    # Переводим колонку с артикулом в строку
    df_reminders_all['Артикул'] = df_reminders_all['Артикул'].astype(str)

    # Если стоит флаг агрегации, то считаем сумму остатков
    if to_agg:
        # Считаем остатки FBO + FBS
        df_reminders_total = (
            df_reminders_all
            .groupby(agg_col, as_index=False)
            .sum()
            .assign(**{
                'Остаток': lambda df:df[['Остатки FBO', 'Остатки FBS']].sum(axis=1)
            })
            # .reset_index()
        )
        return df_reminders_total
    # Если флаг не стоит, то возвращаем просто остатки
    else:
        return df_reminders_all


# Функция выгрузки отчета реализации товаров
def get_products_realization_report(headers, year, month):
    params_finance_report = json.dumps({
    "year": year,
    "month": month
    })
    resp_data_finance_report = requests.post("https://api-seller.ozon.ru/v2/finance/realization", headers=headers, data=params_finance_report).json()
    # result_finance_report_title = pd.DataFrame(resp_data_finance_report['result']['header'], index=[0])
    result_finance_report = pd.DataFrame(resp_data_finance_report['result']['rows'])
    # Получаем данные по товару
    df_products_realization = (
        pd
        .json_normalize(result_finance_report['item'])
        .rename(columns={
            'name': 'Наименование товара',
            'offer_id': 'Артикул',
            'barcode': 'Штрихкод',
            'sku': 'SKU'
        })
    )
    # Словарики для переименовывания
    sales_rename_columns = {
        'amount': 'Сумма',
        'bonus': 'Баллы за скидки',
        'commission': 'Комиссия',
        'compensation': 'Доплата за счёт Ozon',
        'price_per_instance': 'Цена за экземпляр',
        'quantity': 'Количество товара',
        'standard_fee': 'Базовое вознаграждение Ozon',
        'bank_coinvestment': 'Механики лояльности партнеров: зелёные цены',
        'stars': 'Механики лояльности партнеров: звезды',
        'pick_up_point_coinvestment': 'Механики лояльности партнеров: АПВЗ',
        'total': 'Итого к начислению',
    }
    return_rename_columns = sales_rename_columns.copy()
    # Получаем данные по продажам
    df_sales_realization = (
        pd
        .json_normalize(result_finance_report['delivery_commission'])
        .rename(columns=sales_rename_columns)
    )
    # Добавляем префикс к колонкам продаж
    df_sales_realization.columns = [f"{col} (Продажи)" for col in df_sales_realization.columns]

    # Получаем данные по возвратам
    df_returns_realization = (
        pd
        .json_normalize(result_finance_report['return_commission'])
        .rename(columns=return_rename_columns)
    )
    # Добавляем префикс к колонкам возвратов
    df_returns_realization.columns = [f"{col} (Возвраты)" for col in df_returns_realization.columns]

    # Объединяем отчет в один df
    df_realization_report = pd.concat(
        [df_products_realization, df_sales_realization, df_returns_realization],
        axis=1
    )
    # Добавляем год и месяц
    df_realization_report['Год'] = year
    df_realization_report['Месяц'] = month
    # Получаем нужные данные из отчета апи
    # result_finance_report = result_finance_report.assign(
    #     # Номенклатура
    #     Наименование_товара=[d.get('name') for d in result_finance_report.item],
    #     Артикул_продавца=[d.get('offer_id') for d in result_finance_report.item],
    #     Штрихкод=[d.get('barcode') for d in result_finance_report.item],
    #     SKU=[d.get('sku') for d in result_finance_report.item],
    #     Количество=[d.get('quantity') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     # Продажи
    #     Количество_продажи=[d.get('quantity') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Цена_продажи=[d.get('price_per_instance') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Сумма_продажи=[d.get('amount') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Баллы_за_скидки_продажи=[d.get('bonus') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Комиссия_продажи=[d.get('comission') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Доплата_за_счет_озон_продажи=[d.get('compensation') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Базовое_вознаграждение_озон_продажи=[d.get('standard_fee') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_звёзды_продажи=[d.get('stars') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_зелёные_цены_продажи=[d.get('bank_coinvestment') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_апвз_продажи=[d.get('pick_up_point_coinvestment') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     Итого_к_начислению_продажи=[d.get('total') if d != None else 0 for d in result_finance_report.delivery_commission],
    #     # Возвраты
    #     Количество_возвраты=[d.get('quantity') if d != None else 0 for d in result_finance_report.return_commission ],
    #     Цена_возвраты=[d.get('price_per_instance') if d != None else 0 for d in result_finance_report.return_commission],
    #     Сумма_возвраты=[d.get('amount') if d != None else 0 for d in result_finance_report.return_commission],
    #     Баллы_за_скидки_возвраты=[d.get('bonus') if d != None else 0 for d in result_finance_report.return_commission],
    #     Комиссия_возвраты=[d.get('comission') if d != None else 0 for d in result_finance_report.return_commission],
    #     Доплата_за_счет_озон_возвраты=[d.get('compensation') if d != None else 0 for d in result_finance_report.return_commission],
    #     Базовое_вознаграждение_озон_возвраты=[d.get('standard_fee') if d != None else 0 for d in result_finance_report.return_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_звёзды_возвраты=[d.get('stars') if d != None else 0 for d in result_finance_report.return_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_зелёные_цены_возвраты=[d.get('bank_coinvestment') if d != None else 0 for d in result_finance_report.return_commission],
    #     Выплаты_по_механикам_лояльности_партнёров_апвз_возвраты=[d.get('pick_up_point_coinvestment') if d != None else 0 for d in result_finance_report.return_commission],
    #     Итого_к_начислению_возвраты=[d.get('total') if d != None else 0 for d in result_finance_report.return_commission],
    #     Год=year,
    #     Месяц=month
    # )
    # for col in ['Количество_продажи', 'Сумма_продажи', 'Количество_возвраты', 'Сумма_возвраты']:
    #     df_finance_report[col] = df_finance_report[col].fillna(0)

    # df_finance_report['Механики партнеров Продажи'] = df_finance_report[['Выплаты_по_механикам_лояльности_партнёров_звёзды_продажи',
    #                                                             'Выплаты_по_механикам_лояльности_партнёров_зелёные_цены_продажи',
    #                                                             'Выплаты_по_механикам_лояльности_партнёров_апвз_продажи']].sum(axis=1)
    # df_finance_report['Механики партнеров Возвраты'] = df_finance_report[['Выплаты_по_механикам_лояльности_партнёров_звёзды_возвраты',
    #                                                             'Выплаты_по_механикам_лояльности_партнёров_зелёные_цены_возвраты',
    #                                                             'Выплаты_по_механикам_лояльности_партнёров_апвз_возвраты']].sum(axis=1)
    # df_finance_report['Продажи минус возвраты шт Озон'] = df_finance_report['Количество_продажи'] - df_finance_report['Количество_возвраты']
    # df_finance_report['Продажи минус возвраты руб Озон'] = df_finance_report['Сумма_продажи'] - df_finance_report['Сумма_возвраты']
    # df_finance_report['Баллы Озон Итог'] = df_finance_report['Баллы_Озон_продажи'] - df_finance_report['Баллы_Озон_возвраты']

    # df_finance_report_by_month = (
    #     df_finance_report
    #     .groupby(['Артикул_продавца', 'SKU', 'Штрихкод', 'Год', 'Месяц'])
    #     .agg(**{
    #     'Продажи шт': ('Количество_продажи', 'sum'),
    #     'Продажи руб': ('Сумма_продажи', 'sum'),
    #     'Баллы Озон Продажи': ('Баллы_Озон_продажи', 'sum'),
    #     'Возвраты шт': ('Количество_возвраты', 'sum'),
    #     'Возвраты руб': ('Сумма_возвраты', 'sum'),
    #     'Баллы Озон Возвраты': ('Баллы_Озон_возвраты', 'sum'),
    #     'Продажи минус возвраты шт Озон': ('Продажи минус возвраты шт Озон', 'sum'),
    #     'Продажи минус возвраты руб Озон': ('Продажи минус возвраты руб Озон', 'sum')
    #     })
    #     .reset_index()
    #     .rename(columns={'Артикул_продавца': 'Артикул продавца'})
    #     # .assign(
    #     #     Размер=df_finance_report_by_month['Артикул продавца'].str.split('_', n=2)
    #     # )
    # )

    # if to_save:
    #     with pd.ExcelWriter(f"{uploaddir_finance_report_today}/finance_report_{client_name}_Ozon.xlsx") as w:
    #         # result_finance_report_title.to_excel(w, sheet_name='Титульный лист отчета')
    #         df_realization_report.to_excel(w, sheet_name='Таблица отчета')

    return df_realization_report

# %%
