
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

# Файл с настройками и номером клиента
from ozon.scripts.constants import (
    headers,
    client_name,
    marketplace_dir_name
)

# создаем отдельную папку для текущей выгрузки
uploaddir_finance_report = f"{marketplace_dir_name}/Clients/{client_name}/Finance_Reports"
if not os.path.exists(uploaddir_finance_report):
    os.makedirs(uploaddir_finance_report)
uploaddir_finance_report_today = f"{uploaddir_finance_report}/FinanceReport_{str(date.today())}"
if os.path.exists(uploaddir_finance_report_today):
    shutil.rmtree(uploaddir_finance_report_today)
new_dir = os.mkdir(f"{uploaddir_finance_report}/FinanceReport_"+str(date.today()))

# Функция выгрузки отчета по реализации товаров
def get_products_realization_report(headers, year, month, to_save=True):
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

    if to_save:
        with pd.ExcelWriter(f"{uploaddir_finance_report_today}/finance_report_{client_name}_Ozon.xlsx") as w:
            # result_finance_report_title.to_excel(w, sheet_name='Титульный лист отчета')
            df_realization_report.to_excel(w, sheet_name='Таблица отчета')

    return df_realization_report


# Функция выгрузки отчета по транзакциям
def getTransactionReport(headers, date_start, date_end):

    date_start = '2025-01-01T00:00:00.000Z'
    date_end = '2025-01-30T23:59:59.000Z'
    # Начальные значения для цикла
    page = 1
    page_count = 2
    # df для выгрузки
    df_transaction_list = pd.DataFrame()
    while page <= page_count:
        # Выгружаем отдельно каждую страницу отчета
        params = json.dumps({
            "filter": {
                "date": {
                    "from": date_start,
                    "to": date_end
                },
                "operation_type": [],
                "posting_number": "",
                "transaction_type": "all"
            },
            "page": page,
            "page_size": 1000
        })

        resp_data_transaction_list = requests.post("https://api-seller.ozon.ru/v3/finance/transaction/list", headers=headers, data=params).json()
        # Сколько нужно выгрузить страниц
        page_count = resp_data_transaction_list['result']['page_count']
        # Увеличиваем страницу 1 для выгрузки следующей страницы
        page = page + 1
        # print(resp_data_transaction_list)
        # Промежуточный df, в который помещаем результаты текущей страницы
        tmp_df = pd.DataFrame(resp_data_transaction_list['result']['operations'])
        # Объединяем с предыдущей страницей
        df_transaction_list = pd.concat([df_transaction_list, tmp_df])

    # Убираем дубликаты из index
    df_transaction_list = df_transaction_list.reset_index(drop=True)
    # Обработка данных фин. отчета
    # Количество товаров в одной операции
    df_transaction_list['items_amount'] = df_transaction_list['items'].apply(lambda x: len(x))
    df_transaction_list['services_amount'] = df_transaction_list['services'].apply(lambda x: len(x))
    # Распаковка операций и товаров из list в колонках
    df_transactions_unpacked = df_transaction_list.explode('items').explode('services').reset_index(drop=True)
    # Операции
    services = pd.json_normalize(df_transactions_unpacked['services'])
    services.rename(columns={"name": "service_name", "price": "service_price"}, inplace=True)
    # Товары
    products = pd.json_normalize(df_transactions_unpacked['items'])
    products.rename(columns={"name": "product_name"}, inplace=True)
    # Отправления
    postings = pd.json_normalize(df_transactions_unpacked['posting'])
    # Объединяем распакованные колонки с исходным файлом
    df_transactions_all = pd.concat([df_transactions_unpacked, postings, products, services], axis=1)

    # Выгрузка данных по отправлениям (кол-во товаров)
    postings = pd.json_normalize(df_transaction_list['posting'])
    # Добавляем тип (заказ\возврат) и стоимость отправления
    postings = pd.concat([postings, df_transaction_list[['type', 'amount']]], axis=1)
    # Получение отправлений FBO и FBS (только продажи)
    # FBO
    postings_fbo = postings.loc[(postings['delivery_schema'] == 'FBO'), :] \
        .groupby(['posting_number', 'type', 'order_date']) \
        .agg(amount=('amount', 'sum')) \
        .reset_index()
    postings_fbo = postings_fbo \
        .loc[postings_fbo['type'] == 'orders', :] \
        .drop_duplicates(subset=['posting_number']) \
        .sort_values(['order_date'])
    # FBS
    postings_fbs = postings.loc[(postings['delivery_schema'] == 'FBS'), :] \
        .groupby(['posting_number', 'type', 'order_date']) \
        .agg(amount=('amount', 'sum')) \
        .reset_index()
    postings_fbs = postings_fbs \
        .loc[postings_fbs['type'] == 'orders', :] \
        .drop_duplicates(subset=['posting_number']) \
        .sort_values(['order_date'])

    # Выгружаем данные по отправлениям FBO
    sales_fbo = pd.DataFrame()
    for posting_number in postings_fbo['posting_number']:
        params_fbo = json.dumps({
                    "posting_number": posting_number,
                    "translit": True,
                    "with": {
                    "analytics_data": True,
                    "financial_data": True
                    }
                })
        result = requests.post("https://api-seller.ozon.ru/v2/posting/fbo/get",headers=headers, data=params_fbo).json()
        # Получаем дату заказа (для логов)
        orderDate = str(re.findall(r'[0-9]*-[0-9]*-[0-9][0-9]',result['result']['created_at']))
        logger.info("Upload orders on "+ orderDate.replace("['",'').replace("']",''))
        # Достаем из результата характеристики товара
        products_fbo = pd.DataFrame(result['result']['products'])
        products_fbo = products_fbo.assign(
            Номер_заказа=result['result']['order_number'],
            Номер_отправления=result['result']['posting_number'],
            Склад = result['result']['analytics_data']['warehouse_name'],
            Кластер_отправления=result['result']['financial_data']['cluster_from'],
            Кластер_доставки=result['result']['financial_data']['cluster_to'],
            Статус=result['result']['status']
        )
        # Объединяем данные с предыдущим заказом
        sales_fbo = pd.concat([sales_fbo, products_fbo])
        # Делаем паузу 3 с., чтобы часто не вызывать метод апи
        time.sleep(3)


    # with pd.ExcelWriter(f"{uploaddir_finance_report_today}/transaction_list_{settings['client_name'][client_number]}_Ozon.xlsx") as w:
    #     df_transaction_list.to_excel(w)

# %% Вызов всех функций
if __name__ == '__main__':
    date_start = '2023-01-01'
    date_end = '2025-08-01'
    date_start_file = datetime.strptime(date_start, '%Y-%m-%d').strftime('%d.%m.%Y')
    date_end_file = datetime.strptime(date_end, '%Y-%m-%d').strftime('%d.%m.%Y')
    date_range = pd.DataFrame({"date": pd.date_range(start=date_start, end=date_end, freq='ME')})
    date_range = date_range.assign(
        year=date_range['date'].dt.year,
        month=date_range['date'].dt.month
    )
    df_finance_report = pd.DataFrame()
    for i in range(date_range.shape[0]):
        # params_finance_report = json.dumps({
        #     "month": int(date_range['month'][i]),
        #     "year": int(date_range['year'][i])
        # })
        year = int(date_range['year'][i])
        month = int(date_range['month'][i])
        df_realization_report = get_products_realization_report(headers, year=year, month=month, to_save=False)
        df_finance_report = pd.concat(
            [df_finance_report, df_realization_report],
            ignore_index=True
        )

# %%
