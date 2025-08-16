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
pd.set_option('future.no_silent_downcasting', True)
# Файл с константами
from ozon.scripts.constants import marketplace_dir_name, client_name


metricsdir = f"{marketplace_dir_name}/Clients/{client_name}/Metrics"
if not os.path.exists(metricsdir):
    os.mkdir(metricsdir)

# date_upload_files = '2024-11-13'

def calcMetrics(date_upload_files = str(date.today())):
    # Директории с загруженными файлами из апи
    uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
    uploaddir = f"{uploaddir}/UploadFiles_{date_upload_files}"
    dates_from_to = pd.read_csv(f"{uploaddir}/{date_upload_files}_dates_from_to.csv", sep=';')
    logger.info(f"Calculation metrics for client {client_name} for dates {dates_from_to['date_start'][0]} - {dates_from_to['date_end'][0]}")

    # Загрузка данных из CSV файлов
    catalog = pd.read_csv(f"{uploaddir}/{date_upload_files}_Товары.csv", sep=';')
    order_fbo = pd.read_csv(f"{uploaddir}/{date_upload_files}_Заказы_fbo.csv", encoding='utf-8', sep=';')
    order_fbs = pd.read_csv(f"{uploaddir}/{date_upload_files}_Заказы_fbs.csv", encoding='utf-8', sep=';')
    sales_fbo = pd.read_csv(f"{uploaddir}/{date_upload_files}_Продажи_fbo.csv", encoding='utf-8', sep=';')
    sales_fbs = pd.read_csv(f"{uploaddir}/{date_upload_files}_Продажи_fbs.csv", encoding='utf-8', sep=';')
    warehouses_fbo = pd.read_csv(f"{uploaddir}/{date_upload_files}_Остатки.csv", encoding='utf-8', sep=';')
    warehouses_fbs = pd.read_csv(f"{uploaddir}/{date_upload_files}_Остатки_fbs.csv", encoding='utf-8', sep=';')
    supply_order_items = pd.read_csv(f"{uploaddir}/{date_upload_files}_Поставки.csv", encoding='utf-8', sep=';')

    # Объединение данных заказов FBO и FBS
    order_fbo = order_fbo.loc[:, ~order_fbo.columns.isin(['Объемный вес товаров, кг'])]
    order_fbo['Схема доставки'] = 'FBO'
    order_fbs['Схема доставки'] = 'FBS'
    if not order_fbs.empty:
        order = pd.concat([order_fbo, order_fbs], axis=0)
    else:
        order = order_fbo

    # Обработка данных заказов
    order = order[['Артикул', 'Кластер доставки', 'Статус', 'Схема доставки', 'Сумма отправления']]
    order = order.rename(columns={'Кластер доставки': 'Кластер'})
    # Добавление колонок для подсчета продаж и заказов
    # order['Продажи'] = np.where(order['Статус'] != 'Отменен',1, 0)
    # order['Продажи_только_Доставлен'] = np.where(order['Статус'] == 'Доставлен',1, 0)
    # order['Продажи, руб.'] = np.where(order['Продажи'] == 1, order['Сумма отправления'], 0)
    # order['Отмены, руб.'] = np.where((order['Статус'] == 'Отменён') | (order['Статус'] == 'Отменен'), order['Сумма отправления'],0)
    # order['Продажи итого, руб.'] = order['Продажи, руб.'] - order['Отмены, руб.']
    order['Заказы'] = np.where(order['Статус'].notnull(), 1, 0)
    order['Заказы, руб.'] = np.where(order['Заказы'] == 1, order['Сумма отправления'], 0)
    # order.drop(columns=['Статус'], inplace=True)
    pd.set_option('display.max_columns', None)

    # Объединение данных продаж FBO и FBS
    sales = pd.concat([sales_fbo, sales_fbs], axis=0)
    # Обработка данных продаж
    # sales['Продажи'] = np.where(sales['Статус'] == 'delivered',
    #                             sales['Количество'],
    #                             0)
    sales['Продажи'] = sales['Количество']
    sales = sales.rename(columns={'Кластер_доставки': 'Кластер'})
    sales = sales.loc[:, ['Артикул', 'Кластер', 'Продажи']]

    # Удаление ненужных колонок и фильтрация складских данных
    # warehouses_fbo.drop(columns=['sku', 'Название товара', 'Резерв', 'Название склада'], inplace=True)
    # warehouses = warehouses[warehouses['Доступный к продаже товар'] != 0]
    warehouses_fbo = warehouses_fbo.loc[:, warehouses_fbo.columns.isin([
        'Артикул',
        'Кластер',
        'Доступный к продаже товар'
    ])]
    warehouses_fbo = warehouses_fbo.rename(columns={
        # 'Артикул продавца': 'Артикул',
        'Доступный к продаже товар': 'Остатки'
    })

    # Обработка данных поставок
    # Если есть заявки на поставку, добавляем их в отчет
    if supply_order_items.shape[0] > 0:
        supply_order_items = supply_order_items.rename(columns={'Наименование товара': 'Название товара',
                                                                'quantity': 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'})
        supply_order_items = supply_order_items.loc[:, ['Артикул', 'Кластер', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ']]
    # Если нет, то создаем пустой df, чтобы потом не было ошибок в concat
    else:
        supply_order_items = pd.DataFrame(columns=['Артикул', 'Кластер', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'])

    # Обработка данных остатков на складах продавца
    # Если есть остатки на FBS складах, добавляем их в отчет
    warehouses_fbs = warehouses_fbs.rename(columns={'Доступно на моем складе, шт': 'Остатки_fbs'})
    if warehouses_fbs['Остатки_fbs'].sum() >= 1:
        warehouses_fbs = warehouses_fbs.loc[:, ['Артикул', 'Остатки_fbs']]
    # Если нет, то создаем пустой df, чтобы потом не было ошибок в concat
    else:
        warehouses_fbs = pd.DataFrame(columns=['Артикул', 'Остатки_fbs'])

    # Объединение данных заказов и складских остатков
    #orderware = pd.merge(order, warehouses_fbo, how='outer', on = ['Артикул', 'Кластер'])
    orderware = pd.concat([order, sales, supply_order_items, warehouses_fbo, warehouses_fbs])
    orderware = orderware.fillna(0)
    # orderware = orderware.rename(columns={'Доступный к продаже товар': 'Остатки'})
    orderware = orderware.groupby(['Артикул', 'Кластер']).agg(
        Продажи=('Продажи', 'sum'),
        Заказы=('Заказы', 'sum'),
        Заказы_руб=('Заказы, руб.', 'sum'),
        # Товары_в_пути=('Товары в пути', 'sum'),
        ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum'),
        Остатки=('Остатки', 'sum'),
        # У остатков fbs нет разбивки по кластерам
        Остатки_fbs=('Остатки_fbs', 'sum')
    ).reset_index()
    # Сохранение объединенных данных в файл Excel
    # orderware.to_excel('orderware.xlsx')

    # Обработка данных каталога
    catalog = catalog.rename(columns={'Название товара': 'Наименование товара'})
    catalog['Артикул'] = catalog['Артикул'].str.replace("'", "", regex=False)
    # catalog['Категория'] = catalog['Наименование товара'].str.split(' |-').str[0]
    # catalog['Размер'] = catalog['Артикул'].str.extract(r'_([^_]+)$')
    # catalog['Размер'] = np.where((catalog['Категория'] == 'Бейсболка') |
    #                              (catalog['Категория'] == 'Платч') |
    #                              (catalog['Категория'] == 'Палантин'),
    #                              0,
    #                              catalog['Размер']
    # )
    # catalog['Barcode'] = pd.to_numeric(catalog['Barcode'], errors='coerce')
    # catalog['Barcode'] = catalog['Barcode'].apply(lambda x: format(x, 'f') if pd.notnull(x) else x)
    # catalog['Barcode'] = catalog['Barcode'].apply(lambda x: str(x).split('.')[0] if pd.notnull(x) else x).astype('Int64')
    catalog = catalog[['Артикул', 'Наименование товара', 'Barcode', 'Ozon Product ID', 'SKU']]
    catalog.rename(columns={'Barcode': 'Штрихкод'}, inplace=True)

    # Объединение данных каталога с данными о заказах и остатках
    merged_df = catalog.merge(orderware, on='Артикул', how='left')
    merged_df = merged_df.fillna(0)
    # Сортировка для наглядности
    # merged_df = merged_df.sort_values(by=['Заказы', 'Остатки'], ascending=False)

    # Фильтрация данных по наличию кластеров
    claster_report = merged_df[merged_df['Кластер'] != 0]

    # Создание сводного отчета
    summary = merged_df.groupby(['Артикул', 'SKU', 'Ozon Product ID', 'Наименование товара', 'Штрихкод',]).agg(
        Продажи=('Продажи', 'sum'),
        Заказы=('Заказы', 'sum'),
        Заказы_руб=('Заказы_руб', 'sum'),
        # Товары_в_пути=('Товары_в_пути', 'sum'),
        ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum'),
        Остатки=('Остатки', 'sum'),
        Остатки_fbs=('Остатки_fbs', 'sum')
    ).reset_index()
    # # Если есть заявки на поставку, добавляем их в отчет
    # if supply_order_items.shape[0] > 0:
    #     supply_order_items_total = supply_order_items.groupby('sku').agg(
    #         ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('quantity', 'sum')
    #     ).reset_index()
    #     supply_order_items_total.rename(columns={'sku': 'SKU'}, inplace=True)
    #     summary = summary.merge(supply_order_items_total, how='left', on='SKU')
    #     summary = summary.fillna(0)
    #     summary['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = summary['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'].astype(float)
    # else:
    #     summary['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0
    # Если есть остатки на FBS складах, добавляем их в отчет
    # if warehouses_fbs.shape[0] > 0:
    #     warehouses_fbs.rename(columns={'Доступно на моем складе, шт': 'Остатки_fbs'}, inplace=True)
    #     warehouses_fbs_total = warehouses_fbs.groupby('Артикул').agg(
    #         Остатки_fbs=('Остатки_fbs', 'sum')
    #     ).reset_index()
    #     summary = summary.merge(warehouses_fbs_total, how='left', on='Артикул')
    #     summary = summary.fillna(0)
    # else:
    #     summary['Остатки_fbs'] = 0

    # Сортировка сводного отчета для наглядности
    summary = summary.sort_values(by=['Артикул'])
    claster_report = claster_report.sort_values(by=['Артикул', 'Кластер'])

    # Сохранение итоговых данных в файл Excel с несколькими листами
    writer = pd.ExcelWriter(f"{metricsdir}/{date_upload_files}_МетрикиИтоги.xlsx", engine='xlsxwriter')

    summary.to_excel(writer, sheet_name='summary', index=False)
    claster_report.to_excel(writer, sheet_name='claster_report', index=False)

    # Закрытие записи в файл Excel
    writer.close()
    # os.remove('orderware.xlsx')
    logger.info('Done')


if __name__ == '__main__':
    # calcMetrics(date_upload_files)
    calcMetrics()
