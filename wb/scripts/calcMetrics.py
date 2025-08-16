from datetime import date,datetime,timedelta
import pandas as pd
import shutil
import os
import csv
import json
import numpy as np
from openpyxl import Workbook
from loguru import logger
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# Файл с константами
from wb.scripts.constants import (
    marketplace_dir_name,
    client_name,
    cargo_type,
    ignore_virtual_warehouses,
)


# date_upload_files = "2025-05-28"

def calcMetrics(date_upload_files=str(date.today())):
    # Директории с загруженными файлами из апи
    uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
    uploaddir = f"{uploaddir}/UploadFiles_{date_upload_files}"
    dates_from_to = pd.read_csv(f"{uploaddir}/{date_upload_files}_dates_from_to.csv", sep=';')
    logger.info(f"Calculation metrics for client {client_name} for dates {dates_from_to['date_start'][0]} - {dates_from_to['date_end'][0]}")
    # Загрузка данных из CSV файлов
    catalog = pd.read_csv(f"{uploaddir}/{date_upload_files}_Товары.csv", sep=';')
    order = pd.read_csv(f"{uploaddir}/{date_upload_files}_Заказы.csv", sep=';')
    sales = pd.read_csv(f"{uploaddir}/{date_upload_files}_Продажи.csv", sep=';')
    reminders = pd.read_csv(f"{uploaddir}/{date_upload_files}_Остатки.csv", sep=';')
    reminders_fbs = pd.read_csv(f"{uploaddir}/{date_upload_files}_Остатки_fbs.csv", sep=';')
    supply_orders = pd.read_csv(f"{uploaddir}/{date_upload_files}_Поставки.csv", sep=';')
    # supply_orders_fbs = pd.read_csv(f"{uploaddir}/{date_upload_files}_Поставки_fbs.csv", sep=';')

    # Обработка данных
    df_list = [catalog, order, sales, reminders, reminders_fbs, supply_orders]
    for i, df in enumerate(df_list):
        tmp_df = df_list[i]
    # Для некоторых клиентов в размере заменяем точку на запятую
        if client_name in ['KU_And_KU', 'Soyuz']:
                if 'Размер' in tmp_df.columns:
                    tmp_df['Размер'] = tmp_df['Размер'].astype(str).str.replace(',', '.', regex=False)
                    # tmp_df['Размер'] = tmp_df['Размер'].astype(str).str.replace('.0', '', regex=False)
                    tmp_df['Размер'] = tmp_df['Размер'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
                    tmp_df['Размер'] = tmp_df['Размер'].fillna(0)
        # Перевод столбца barcode в тип строки, потому что иногда встречаются
        # длинные штрихкоды которые не помещаются в int64
        if 'barcode' in df.columns:
            tmp_df['barcode'] = tmp_df['barcode'].astype(str)
        # Удаляем ненужную колонку
        if 'Unnamed: 0' in df.columns:
            tmp_df.drop(columns='Unnamed: 0', inplace=True)
        # Создаем колонку Артикул+Размер
        tmp_df['Артикул_Размер'] = tmp_df[['Артикул продавца', 'Размер']].apply(lambda row: '_size_'.join(row.values.astype(str)), axis=1)


    # Переименование некоторых колонок каталога
    catalog = catalog.rename(columns={
        'nmID': 'Артикул WB',
        'barcode': 'barcode_list',
        'last_barcode': 'Штрихкод',
        'discount_price': 'РРЦ'
        })
    # Удаляем колонку с баркодом
    # catalog = catalog.drop(columns=['barcode'])
    # Удаляем дубликаты в Артикуле и Размере, если они есть
    catalog = catalog.drop_duplicates(subset=['Артикул_Размер'])
    # Удаляем колонку Артикула и Размера, поскольку будем мерджить по объединенной колонке выше
    catalog = catalog.drop(columns=['Артикул продавца', 'Размер'])
    # Добавляем колонку с номером товара
    catalog['№ товара'] = np.arange(1, catalog.shape[0] + 1)

    # Убираем лишние символы в столбцах размера
    # for df in [catalog, order, sales, reminders, supply_orders]:
    #     if 'Размер' in df.columns:
    #         df['Размер'] = df['Размер'].str.split('-').str[0]

    # Какие строки учитываем в продажах и заказах и остатках
    order['Заказы'] = 1
    order['Заказы_руб'] = np.where(order['Заказы'] == 1,
                                   order['priceWithDisc'],
                                   0)
    sales['Продажи'] = np.where(sales['finishedPrice'] > 0,
                                1,
                                0)
    sales['Продажи_руб'] = sales['finishedPrice']
    reminders['Остатки'] = reminders['quantityFull']
    reminders_fbs['Остатки_fbs'] = reminders_fbs['amount']
    # reminders_fbs['Остатки_fbs'] = reminders_fbs['amount']

    # Выборка нужных колонок
    #order = order.loc[:, ['barcode', 'Заказы' 'warehouseName', 'isCancel', 'totalPrice', 'discountPercent', 'spp', 'finishedPrice', 'priceWithDisc']]
    #sales = sales.loc[:, ['barcode', 'Продажи', 'warehouseName', 'totalPrice', 'discountPercent', 'spp', 'paymentSaleAmount', 'forPay', 'finishedPrice', 'priceWithDisc']]
    order = order.loc[:, ['Артикул_Размер', 'warehouseName', 'Заказы', 'Заказы_руб']]
    sales = sales.loc[:, ['Артикул_Размер', 'warehouseName', 'Продажи', 'Продажи_руб']]
    reminders = reminders.loc[:, ['Артикул_Размер', 'warehouseName', 'Остатки']]
    reminders_fbs = reminders_fbs.loc[:, ['Артикул_Размер', 'Остатки_fbs']]
    orderware = pd.concat([order, sales, reminders, reminders_fbs])
    # Если были поставки, включаем их в расчет
    if supply_orders.shape[0]> 0:
        supply_orders['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = supply_orders['quantity']
        supply_orders = supply_orders.loc[:, ['Артикул_Размер', 'warehouseName', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ']]
        orderware = pd.concat([orderware, supply_orders])
    # Если нет, то товары, которые должны поступить на склад = 0
    else:
        orderware['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0

    # TODO: Убираем виртуальные склады из списка складов
    if ignore_virtual_warehouses:
        logger.warning("Ignoring virtual warehouses")
        orderware = orderware.loc[~orderware['warehouseName'].str.contains('Виртуальный', na=False), :]

    # Считаем суммы по складам
    orderware_by_warehouses = (
        orderware
        # Группировка
        .groupby(['Артикул_Размер', 'warehouseName'])
        # Расчет статистик
        .agg(
            Заказы=('Заказы', 'sum'),
            Заказы_руб=('Заказы_руб', 'sum'),
            Продажи=('Продажи', 'sum'),
            Продажи_руб=('Продажи_руб', 'sum'),
            ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum'),
            Остатки=('Остатки', 'sum'),
            Остатки_fbs=('Остатки_fbs', 'sum')
            )
        # Достаем столбцы из index
        .reset_index()
    )


    # Считаем суммы по артикулам
    orderware_by_sku = (
        orderware
        # Группировка
        .groupby(['Артикул_Размер'])
        # Расчет статистик
        .agg(
            Заказы=('Заказы', 'sum'),
            Заказы_руб=('Заказы_руб', 'sum'),
            Продажи=('Продажи', 'sum'),
            Продажи_руб=('Продажи_руб', 'sum'),
            ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum'),
            Остатки=('Остатки', 'sum'),
            Остатки_fbs=('Остатки_fbs', 'sum')
            )
        # Достаем столбцы из index
        .reset_index()
    )


    # Мердж каталога с заказми и продажами по складам
    claster_report = catalog.merge(orderware_by_warehouses, on=['Артикул_Размер'], how='left')
    # Достаем артикул и размер из объединенной колонки Артикула и размера
    claster_report = claster_report.assign(**{
        'Артикул продавца': claster_report['Артикул_Размер'].str.split('_size_', expand=True)[0],
        'Размер': claster_report['Артикул_Размер'].str.split('_size_', expand=True)[1]
    })
    # Удаляем те товары, которые не были ни на одном из складов
    # TODO: переделать, как появится список складов
    claster_report = claster_report.dropna(subset=['warehouseName'])
    # Заполнение NA на всякий случай
    claster_report = claster_report.fillna(0)
    # Переименовываем некоторые колонки для удобства
    claster_report = claster_report.rename(columns={
        # 'Barcode': 'Штрихкод',
        'warehouseName': 'Склад',
    })
    # Определяем тип груза клиента из настроек
    if cargo_type == 'monopallets':
        wb_warehouses_sheet_name = 'Монопалеты'
    elif cargo_type == 'boxes':
        wb_warehouses_sheet_name = 'Короба'
    # Считываем таблицу группировки складов WB
    wb_warehouses_mapping = pd.read_excel(
        f"{marketplace_dir_name}/scripts/wb_warehouses_mapping.xlsx",
        sheet_name = wb_warehouses_sheet_name
    )
    # На всякий случай удаляем дубликаты
    wb_warehouses_mapping = wb_warehouses_mapping.drop_duplicates(subset='Склад')
    # Убираем колонку с номером
    wb_warehouses_mapping = wb_warehouses_mapping.loc[:, ~wb_warehouses_mapping.columns.isin(['№'])]
    # Создаем df со списком складов, пришедших из АПИ
    api_clusters = pd.DataFrame({"Склад": claster_report['Склад'].unique()})
    # Объединяем список складов АПИ с таблицей группировки складов
    api_clusters = api_clusters.merge(
        wb_warehouses_mapping,
        how='left',
        on='Склад',
        indicator=True
    )
    # Ищем, есть ли незаполненные склады
    missing_clusters = (
        api_clusters
        .loc[api_clusters['Группировка'].isna(), 'Склад']
        .drop_duplicates()
    )
    # Если нашлись склады, которые не указаны в таблице, выводим предупреждение
    if missing_clusters.shape[0] > 0:
        logger.warning(f"No warehouses found for \n {missing_clusters}")

    # # Мерджим таблицу соответствия складов со статистикой по складам
    # claster_report = claster_report.merge(wb_warehouses_mapping,
    #                                       how='left',
    #                                       on='Склад',
    #                                       indicator=True)
    # # Заполняем NA в колонках из таблицы соответствия
    # for col in wb_warehouses_mapping.columns:
    #     if col in claster_report.columns:
    #         claster_report[col] = claster_report[col].fillna('Неизвестный склад')


    claster_report = claster_report.sort_values(by=['Артикул продавца', 'Размер', 'Склад'], ignore_index=True)

    # Выборка нужных колонок
    claster_report = claster_report.loc[:, [
        '№ товара', 'Артикул_Размер', 'Артикул продавца', 'Артикул WB',
        'Наименование товара', 'Предмет', 'Размер', 'Цвет', 'Штрихкод',
        'Склад',
        # 'РРЦ',
        'Заказы', 'Заказы_руб', 'Продажи', 'Продажи_руб', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки', 'Остатки_fbs']
        ]

    summary = catalog.merge(orderware_by_sku, on=['Артикул_Размер'], how='left')
    # Достаем артикул и размер из объединенной колонки Артикула и размера
    summary = summary.assign(**{
        'Артикул продавца': summary['Артикул_Размер'].str.split('_size_', expand=True)[0],
        'Размер': summary['Артикул_Размер'].str.split('_size_', expand=True)[1]
    })

    summary = summary.fillna(0)
    # Выборка нужных колонок
    summary = summary.loc[:, [
        '№ товара', 'Артикул_Размер', 'Артикул продавца', 'Артикул WB',
        'Наименование товара', 'Предмет', 'Размер', 'Цвет', 'Штрихкод',
        # 'РРЦ',
        'Заказы', 'Заказы_руб', 'Продажи', 'Продажи_руб', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки', 'Остатки_fbs']
        ]

    with pd.ExcelWriter(f"{metricsdir}/{date_upload_files}_МетрикиИтоги.xlsx") as w:
        summary.to_excel(w, sheet_name='summary', index=False)
        claster_report.to_excel(w, sheet_name='claster_report', index=False)

    logger.info("Done")

if __name__ == '__main__':
    metricsdir = f"{marketplace_dir_name}/Clients/{client_name}/Metrics"
    if not os.path.exists(metricsdir):
        os.mkdir(metricsdir)
    # calcMetrics(date_upload_files)
    calcMetrics()
