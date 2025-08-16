
# %% определение всех функций
import pandas as pd
import numpy as np
import os
from loguru import logger
from datetime import date,datetime,timedelta
from itertools import product


# Функция форматирования файла excel
import wb.scripts.format_supply_svod as format_supply_svod

# Некоторые константы
from wb.scripts.constants import (
    client_name, marketplace_dir_name, shop_type, cargo_type, is_heavy_cargo,
    catalog_supply_columns, svod_total_columns, svod_clusters_columns
)


# Директории с загруженными файлами из апи
uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"

# Директория с метриками
metricsdir = f"{marketplace_dir_name}/Clients/{client_name}/Metrics"

# Функция чтения дат фомрирования отчета
def read_dates_file(date_report_created):
    report_dates = pd.read_csv(f"{uploaddir}/UploadFiles_{date_report_created}/{date_report_created}_dates_from_to.csv", sep=';')
    for col in report_dates:
        report_dates[col] = pd.to_datetime(report_dates[col])
    return report_dates


# Функция чтения метрик
def read_metrics_file(date_report_created, sheet_name='summary'):
    metrics = pd.read_excel(f"{metricsdir}/{date_report_created}_МетрикиИтоги.xlsx",
                                        sheet_name=sheet_name)
    # Создаем копию для избежания изменений в оригинальном df
    metrics_processed = metrics.copy()
    # Для некоторых клиентов делаем размер строковой переменной
    # if client_name in ['TRIBE']:
    #     metrics_processed['Размер'] = metrics_processed['Размер'].astype(str)#.str.replace('.0', '', regex=False)

    return metrics_processed


# Добавление недостающих кластеров к товарам
def add_clusters(date_report_created):
    # Чтение файла с метриками
    metrics_clusters = read_metrics_file(date_report_created,
                                         sheet_name='claster_report')

    # Чтение файла с метрками (лист со всеми товарами)
    metrics_summary = read_metrics_file(date_report_created,
                                         sheet_name='summary')

    # Получение уникальных комбинаций Артикул-Кластер
    sku_clusters = list(
        product(
            metrics_summary['Артикул_Размер'].unique(),
            metrics_clusters['Склад'].unique()
        )
    )
    sku_clusters = pd.DataFrame(sku_clusters, columns=['Артикул_Размер', 'Склад'])

    # Мердж с комбинациями Артикул-Кластер.
    # Таким образом, получим все товары во всех кластерах
    metrics_merged = sku_clusters.merge(
        metrics_clusters[['Артикул_Размер', 'Склад',
                          'Продажи', 'Заказы', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки']],
        on=['Артикул_Размер', 'Склад'],
        how='left'
    )
    # metrics_merged = metrics_cluster.merge(sku_clusters,
    # 								left_on=['FBO OZON SKU ID', 'Кластер'],
    # 								right_on=['SKU', 'cluster'],
    # 								how='right')
    # metrics_merged.sort_values(by=['SKU', 'cluster'], inplace=True)

    # В пропусках считаем, что в данном кластере по данному товару
    # не было товаров, заказов и продаж
    for col in ['Продажи', 'Заказы', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки']:
        metrics_merged[col] = metrics_merged[col].fillna(0)

    # Мердж для получения остальных колонок
    metrics_merged = metrics_merged.merge(metrics_summary.drop(columns=['Продажи', 'Заказы', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки', 'Остатки_fbs']),
                                       on='Артикул_Размер',
                                       how='left')
    return metrics_merged


# Функция получения списка кластеров
def get_cluster_list(df_cluster_source):
    cluster_list = df_cluster_source['Склад'].unique().tolist()
    return cluster_list


# Функция чтения нужного листа из таблицы соответствия кластеров
def read_clusters_mapping(cargo_type):
    # Определяем тип коробов
    cluster_sheet_name = ''
    if cargo_type == 'monopallets':
        cluster_sheet_name = 'Монопалеты'
    elif cargo_type == 'boxes':
        cluster_sheet_name = 'Короба'
    # Считываем df с группировкой складов
    clusters_mapping_df = pd.read_excel(
        f'{marketplace_dir_name}/scripts/wb_warehouses_mapping.xlsx',
        sheet_name=cluster_sheet_name
    )
    # Удаляем дубликаты на всякий случай
    clusters_mapping_df = clusters_mapping_df.drop_duplicates(subset=['Склад'])

    return clusters_mapping_df

# Функция определения нужной колонки для группировки по складам
def get_cluster_column_from_mapping(is_heavy_cargo, specific_mapping=None):
    # Выбираем нужную колонку с группировкой
    if is_heavy_cargo:
        cluster_col = 'Группировка СГТ'
    else:
        cluster_col = 'Группировка'
    # Если нужна специфическая группировка, колонка для замены кластеров будет другой
    # пока только для ювелирных складов
    if specific_mapping:
        cluster_col = 'Группировка ювелирка'

    return cluster_col

# Объединение нескольких кластеров в один
def replace_clusters(clusters_mapping_df, cluster_col, df_with_clusters):
    # Создаем копию для избежания изменений в оригинальном df
    df_with_clusters_ = df_with_clusters.copy()
    cluster_col_ = cluster_col
    # Мерджим таблицу соответствия складов
    df_with_clusters_ = df_with_clusters_.merge(
        clusters_mapping_df,
        how='left',
        on='Склад'
    )
    # На всякий случай заполняем NA
    for col in clusters_mapping_df.columns:
        if col in df_with_clusters_.columns:
            df_with_clusters_[col] = df_with_clusters_[col].fillna('Неизвестный склад')
    # Удаляем старую колонку со складом, заменяя её на новую
    df_with_clusters_ = (
        df_with_clusters_
        .drop(columns='Склад')
        .rename(columns={cluster_col_: 'Склад'})
    )
    # Считаем суммы после замены кластеров
    df_with_clusters_stats = (
        df_with_clusters_
        .loc[:, ['Артикул_Размер', '№ товара', 'Предмет', 'Наименование товара', 'Цвет', 'Штрихкод', 'Склад', 'Продажи', 'Заказы', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки']]
        .groupby(['Артикул_Размер', '№ товара', 'Склад', 'Предмет', 'Наименование товара', 'Цвет', 'Штрихкод'])
        .agg('sum')
        .reset_index()
        )

    return df_with_clusters_stats

# Расчет потребности и оборачиваемости для товаров по кластерам
# лист 'По Кластерам'
def calc_svod_for_clusters(metrics_df_with_clusters):
    # Считаем сумму снова (по аналогии с calcMetrics.py),
    # поскольку мы добавили каждый товар на каждый кластер
    # metrics_df_with_clusters = metrics_df.groupby(['Артикул', 'Наименование товара', 'Штрихкод', 'Категория', 'Ozon Product ID', 'FBO OZON SKU ID', 'Размер']).agg(
    #     Продажи=('Продажи', 'sum'),
    #     Заказы=('Заказы', 'sum'),
    #     Товары_в_пути=('Товары_в_пути', 'sum'),
    #     Остатки=('Остатки', 'sum')
    # ).reset_index()
    metrics_df_with_clusters_ = metrics_df_with_clusters.copy()
    # Расчет доп. колонок (лист 'По кластерам')
    # metrics_df_with_clusters_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0
    metrics_df_with_clusters_['Оборачиваемость'] = (metrics_df_with_clusters_['Заказы'] + metrics_df_with_clusters_['Продажи']) / 2 / 31
    metrics_df_with_clusters_['Потребность на 40 дней'] = metrics_df_with_clusters_['Оборачиваемость'] * 40
    # metrics_df_with_clusters_['Дефицит/Избыток 40 дней'] = metrics_df_with_clusters_['Остатки'] + metrics_df_with_clusters_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] + metrics_df_with_clusters_['Товары_в_пути'] - metrics_df_with_clusters_['Потребность на 40 дней']
    metrics_df_with_clusters_['Дефицит/Избыток 40 дней'] = metrics_df_with_clusters_['Остатки'] - metrics_df_with_clusters_['Потребность на 40 дней']
    metrics_df_with_clusters_['Потребность на 40 дней (округл.)'] = np.where(metrics_df_with_clusters_['Дефицит/Избыток 40 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_with_clusters_['Дефицит/Избыток 40 дней']))
)
    metrics_df_with_clusters_['Потребность на 60 дней'] = metrics_df_with_clusters_['Оборачиваемость'] * 60
    # metrics_df_with_clusters_['Дефицит/Избыток 60 дней'] = metrics_df_with_clusters_['Остатки'] + metrics_df_with_clusters_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] + metrics_df_with_clusters_['Товары_в_пути'] - metrics_df_with_clusters_['Потребность на 60 дней']
    metrics_df_with_clusters_['Дефицит/Избыток 60 дней'] = metrics_df_with_clusters_['Остатки'] - metrics_df_with_clusters_['Потребность на 60 дней']
    metrics_df_with_clusters_['Потребность на 60 дней (округл.)'] = np.where(metrics_df_with_clusters_['Дефицит/Избыток 60 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_with_clusters_['Дефицит/Избыток 60 дней']))
    )
    metrics_df_with_clusters_['Корректировка с учетом нулевых остатков'] = metrics_df_with_clusters_['Потребность на 60 дней (округл.)']
    # Добавляем колонку с номером
    metrics_df_with_clusters_['№'] = metrics_df_with_clusters_.index + 1
    return metrics_df_with_clusters_

# Расчет "сводной" таблицы по кластерам
# лист 'Сводная по кластерам'
def calc_svod_by_clusters(metrics_df_with_stats):
    # Сумма по каждому кластеру (лист 'Сводная по кластерам')
    metrics_df_by_sku_cluster = pd.pivot_table(
        data=metrics_df_with_stats,
        values='Корректировка с учетом нулевых остатков',
        aggfunc='sum',
        columns=['Склад'],
        index=['Артикул_Размер'],
        fill_value=0,
    )
    # Получаем список складов
    cluster_list = metrics_df_by_sku_cluster.columns.to_list()
    # Добавляем префикс к названию колонок
    metrics_df_by_sku_cluster = (
        metrics_df_by_sku_cluster
        .add_prefix('Потребность ')
        .reset_index()
    )

    return metrics_df_by_sku_cluster, cluster_list

# Расчет "сводной" таблицы по SKU
def calc_svod_by_sku(metrics_df_with_clusters):
    # Считаем сумму снова (по аналогии с calcMetrics.py),
    # поскольку мы добавили каждый товар на каждый кластер
    metrics_df_by_sku = metrics_df_with_clusters.groupby(['Артикул продавца', 'Размер', 'Наименование товара', 'Предмет', 'РРЦ']).agg(
            Продажи=('Продажи', 'sum'),
            Заказы=('Заказы', 'sum'),
            ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum'),
            Остатки=('Остатки', 'sum'),
            # ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ=('ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'sum')
        ).reset_index()
    return metrics_df_by_sku


# Расчет потребности и оборачиваемости для товаров, группированных по SKU
# лист 'Всего'
def calc_svod_for_sku(metrics_df_by_sku):
    # Расчет доп. колонок (лист 'Всего')
    metrics_df_by_sku_ = metrics_df_by_sku.copy()
    metrics_df_by_sku_['Оборачиваемость'] = (metrics_df_by_sku_['Заказы'] + metrics_df_by_sku_['Продажи']) / 2 / 31
    # metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0
    metrics_df_by_sku_['Потребность на 40 дней'] = metrics_df_by_sku_['Оборачиваемость'] * 40
    # metrics_df_by_sku_['Дефицит/Избыток 40 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] + metrics_df_by_sku_['Товары_в_пути'] - metrics_df_by_sku_['Потребность на 40 дней']
    metrics_df_by_sku_['Дефицит/Избыток 40 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['Остатки_fbs'] - metrics_df_by_sku_['Потребность на 40 дней']
    metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['Остатки_fbs'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - metrics_df_by_sku_['Потребность на 40 дней']
    metrics_df_by_sku_['Потребность на 40 дней (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 40 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 40 дней']))
    )
    metrics_df_by_sku_['Потребность на 40 дней с учетом FBS (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS']))
    )
    metrics_df_by_sku_['Потребность на 60 дней'] = metrics_df_by_sku_['Оборачиваемость'] * 60
    # metrics_df_by_sku_['Дефицит/Избыток 60 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] + metrics_df_by_sku_['Товары_в_пути'] - metrics_df_by_sku_['Потребность на 60 дней']
    metrics_df_by_sku_['Дефицит/Избыток 60 дней'] = metrics_df_by_sku_['Остатки'] - metrics_df_by_sku_['Потребность на 60 дней']
    metrics_df_by_sku_['Потребность на 60 дней (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 60 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 60 дней']))
    )
    # Добавляем колонку с номером
    metrics_df_by_sku_['№'] = metrics_df_by_sku_.index + 1
    return metrics_df_by_sku_

# Получение колонок потребности для конкретного кластера
def add_demand_for_cluster_columns(metrics_df_by_cluster,
                                   metrics_df_by_sku_with_stats,
                                   cluster_list):
    # Создаем копию для избежания изменений в оригинальном df
    metrics_df_by_sku_with_stats_ = metrics_df_by_sku_with_stats.copy()
    # Мерджим со сводной по кластерам
    metrics_df_by_sku_with_stats_ = metrics_df_by_sku_with_stats_.merge(metrics_df_by_cluster,
                                                                        how='left',
                                                                        on=['Артикул_Размер'])
    # Заполняем пропуски нулями
    for cluster in cluster_list:
        metrics_df_by_sku_with_stats_[f'Потребность {cluster}'] = metrics_df_by_sku_with_stats_[f'Потребность {cluster}'].fillna(0)
    # Из сводной по кластерам берем нужный кластер и делаем merge по артикулу
    # cluster_list = metrics_df_by_cluster['Склад'].unique().tolist()
    # # Перевод в float64 для избежания ошибок во время merge
    # # metrics_df_by_sku_with_stats['Штрихкод'] = metrics_df_by_sku_with_stats['Штрихкод'].astype(np.float64)
    # for cluster in cluster_list:
    #     tmp_df = metrics_df_by_cluster.loc[metrics_df_by_cluster['Склад'] == cluster, ['Артикул продавца', 'Размер', 'Корректировка_с_учетом_нулевых_остатков']]
    #     tmp_df = tmp_df.rename(columns={'Корректировка_с_учетом_нулевых_остатков': 'Потребность' + ' ' + cluster})
    #     metrics_df_by_sku_with_stats = metrics_df_by_sku_with_stats.merge(tmp_df, how='left', on=['Артикул продавца', 'Размер'])
    return metrics_df_by_sku_with_stats_


# Функция чтения и обработки справочной таблицы
def read_catalog():
    # Чтение справочной таблицы
    catalog = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/catalog/Справочная_таблица_{client_name}_WB.xlsx")
    # Создаем копию, в которой будем проводить обработку
    catalog_processed = catalog.copy()
    # Добавляем колонку с размером, если она отсутствует в справочной таблице
    if 'Размер' not in catalog_processed.columns:
        catalog_processed['Размер'] = 0
    # Создаем колонку Артикул_Размер для мерджа
    catalog_processed['Артикул_Размер'] = catalog_processed[['Артикул продавца', 'Размер']].apply(lambda row: '_size_'.join(row.values.astype(str)), axis=1)
    # Удаляем товары, где не указан размер
    catalog_processed = catalog_processed.dropna(subset=['Размер'])
    # Если каких-то колонок не хватает, искуственно создаем их, чтобы не было ошибок
    for col in catalog_supply_columns:
        if col not in catalog_processed.columns:
            catalog_processed[col] = np.nan
    # У некоторых клиентов делаем размер строковым типом
    if client_name in ['KU_And_KU', 'Soyuz']:
        # catalog_processed['Размер'] = catalog_processed['Размер'].astype(str).str.replace('.0', '')
        catalog_processed['Размер'] = catalog_processed['Размер'].apply(lambda x: pd.to_numeric(x, errors='coerce'))
        catalog_processed['Размер'] = catalog_processed['Размер'].fillna(0)

    return catalog_processed


# Заполнение некоторых столбцов из справочных таблиц, заполненных вручную
def add_columns_from_catalog(metrics_df_by_sku_with_stats, catalog):
    # Создаем копию для избежания изменений в оригинальном df
    metrics_df_by_sku_with_stats_ = metrics_df_by_sku_with_stats.copy()
    catalog_ = catalog.copy()
    # Мерджим со справочной таблицей
    metrics_df_by_sku_with_stats_with_catalog = metrics_df_by_sku_with_stats_.merge(catalog_[catalog_supply_columns],
                                                                   how='left',
                                                                   on=['Артикул_Размер'])
    # metrics_df_by_sku_with_stats.drop(columns=['Артикул продавца'])
    return metrics_df_by_sku_with_stats_with_catalog


# Переименование и вставка колонок для соответствия шаблону
def add_columns_for_excel(df_with_stats, cluster_list, date_report_created,
                          day_start, day_end,
                          svod_type='Всего'):
    # Создаем копию для избежания изменений в оригинальном df
    df_with_stats_ = df_with_stats.copy()
    # Создаем datetime объект из строки даты выгрузки
    date_report_created_ = datetime.strptime(date_report_created, '%Y-%m-%d')
    # Достаем колонки из Артикула+Размера
    df_with_stats_ = df_with_stats_.assign(**{
    'Артикул продавца': df_with_stats_['Артикул_Размер'].str.split('_size_', expand=True)[0],
    'Размер': df_with_stats_['Артикул_Размер'].str.split('_size_', expand=True)[1]
    })

    # Колонки, которые должны быть в итоговом файле на листе "Всего"
    if svod_type == 'Всего':
        svod_columns = svod_total_columns
        cluster_columns = ['Потребность' + ' ' + cluster
                        for cluster in cluster_list]
        svod_columns = svod_columns + cluster_columns

    # Колонки, которые должны быть в итоговом файле на листе "По кластерам"
    else:
        svod_columns = svod_clusters_columns

    # Добавляем недостающие колонки (они либо заполняются вручную,
    # либо добавляются в процессе)
    for col in svod_columns:
        if col not in df_with_stats_.columns.to_list():
            df_with_stats_[col] = np.nan

    # Порядок колонок
    df_with_stats_ = df_with_stats_[svod_columns]

    # Сортировка
    df_with_stats_ = df_with_stats_.sort_values(by=['Артикул продавца', 'Размер'])

    # Переименование некоторых колонок для соответствия шаблону
    df_with_stats_.rename(columns={
        'Наименование товара': 'Наименование',
        'Заказы': f"ЗАКАЗЫ с {day_start.strftime('%d.%m')} по {day_end.strftime('%d.%m')}, шт.",
        'Продажи': f"ПРОДАЖИ с {day_start.strftime('%d.%m')} по {day_end.strftime('%d.%m')}, шт.",
        'Остатки': f"ОСТАТОК {date_report_created_.strftime('%d.%m')}",
        'Остатки_fbs': f"ОСТАТОК FBS {date_report_created_.strftime('%d.%m')}"
        },
        inplace=True
        )

    return df_with_stats_


# Создание отчета поставок для ювелирных кластеров
def create_jewelry_clusters_svod(clusters_mapping_df, metrics_df_all_clusters, cluster_column):
    if shop_type == 'jewelry':
        # Объединяем несколько кластеров в один
        metrics_df_union_clusters = replace_clusters(clusters_mapping_df, cluster_column, metrics_df_all_clusters)
        # metrics_df_union_clusters = metrics_df.copy()
        # Расчитываем доп. колонки с оборачиваемостью и потребностью для листа "По кластерам"
        metrics_df_with_stats = calc_svod_for_clusters(metrics_df_union_clusters)
        # Группировка по кластеру (лист "Сводная по кластерам")
        metrics_df_by_cluster, cluster_list = calc_svod_by_clusters(metrics_df_with_stats)
        # Лист "Всего". Берем из файла с метриками
        metrics_df_by_sku = read_metrics_file(date_report_created)
        # Считаем доп. колонки с оборачиваемостью и потребностью для листа "Всего"
        metrics_df_by_sku_stats = calc_svod_for_sku(metrics_df_by_sku)
        # Добавляем колонки с потребностью по кластерам
        svod = add_demand_for_cluster_columns(metrics_df_by_cluster, metrics_df_by_sku_stats, cluster_list)
        # Добавляем колонки, заполняемые вручную
        catalog = read_catalog()
        svod_with_catalog_by_sku = add_columns_from_catalog(svod, catalog)
        # svod_with_catalog_by_sku = svod.copy()
        svod_with_catalog_for_clusters = add_columns_from_catalog(svod_for_clusters, catalog)
        # svod_with_catalog_for_clusters = svod_for_clusters.copy()
        # Чтение файла с датами формирования выгрузок
        report_dates = read_dates_file(date_report_created)
        # Добавляем недостающие колонки для excel
        svod_excel_jewelry_by_sku = add_columns_for_excel(svod_with_catalog_by_sku, cluster_list,
                                        date_report_created,
                                        report_dates['date_start_file'][0],
                                        report_dates['date_end_file'][0],
                                        svod_type='Всего')
    else:
        svod_excel_jewelry_by_sku = None
    return svod_excel_jewelry_by_sku


# Функция сохранения данных в excel
def save_sheets_to_excel(
        svod_by_sku,
        svod_excel_by_sku_jewelry,
        svod_for_clusters,
        clusters_mapping_df,
        date_report_created
    ):
    # Директория для сохранения с именем файла
    filepath_supply_svod = (
        f"{marketplace_dir_name}/Clients/{client_name}/SupplySvod/"
        f"{date_report_created}_Расчет_поставок_{client_name}_WB.xlsx"
    )
    with pd.ExcelWriter(
        filepath_supply_svod,
        engine='xlsxwriter',
        engine_kwargs={'options': {'strings_to_numbers': True}}
    ) as w:
        svod_by_sku.to_excel(w, sheet_name='Всего', index=False)
        if svod_excel_by_sku_jewelry is not None:
            svod_excel_by_sku_jewelry.to_excel(w, sheet_name='Всего по ювелирным складам', index=False)
        svod_for_clusters.to_excel(w, sheet_name='По кластерам', index=False)
        clusters_mapping_df.to_excel(w, sheet_name='Таблица соотв. складов', index=False)
        # w.close()


# Функция форматирования Excel по отдельным листам (формулы, цвета и т.д.)
def format_excel(
        client_name,
        date_report_created,
        clusters_mapping_df,
        cluster_column,
        cluster_column_jewelry,
        svod_excel_sku,
        svod_excel_by_sku_jewelry,
        svod_excel_clusters
    ):
    # Директория хранения отчетов по расчету поставок
    path_supply_svod = f"{marketplace_dir_name}/Clients/{client_name}/SupplySvod"

    # Создаем файл excel, в котором будет производиться форматирование
    format_supply_svod.copy_supply_svod_file(
        path_supply_svod,
        client_name,
        date_report_created
    )
    # Форматирование листа "По кластерам"
    format_supply_svod.format_sheet_clusters(
        path_supply_svod,
        client_name,
        date_report_created,
        svod_excel_clusters,
        sheet_name='По кластерам'
    )
    # Форматирование листа "Всего"
    format_supply_svod.format_sheet_total(
        path_supply_svod,
        client_name,
        date_report_created,
        svod_excel_sku,
        svod_excel_clusters,
        clusters_mapping_df,
        cluster_column,
        sheet_name='Всего'
    )
    # Форматирование листе "Всего по ювелирным кластерам"
    if svod_excel_by_sku_jewelry is not None:
        format_supply_svod.format_sheet_total(
            path_supply_svod,
            client_name,
            date_report_created,
            svod_excel_by_sku_jewelry,
            svod_excel_clusters,
            clusters_mapping_df,
            cluster_column_jewelry,
            sheet_name='Всего по ювелирным складам'
        )


# %% Вызов всех функций
# Дата формирования отчета
# date_report_created = '2025-05-28'
date_report_created = str(date.today())
logger.info(f"Creating supply svod for {client_name}")
# Добавляем недостающие кластеры
# metrics_df_all_clusters = add_clusters(date_report_created)
metrics_df_all_clusters = read_metrics_file(date_report_created, sheet_name='claster_report')
# cluster_list = get_cluster_list(metrics_df_all_clusters)
# Отдельный df для записи в excel на лист "По кластерам" (без объединения кластеров)
svod_for_clusters = calc_svod_for_clusters(metrics_df_all_clusters)
# Считываем таблицу соответствия кластеров
clusters_mapping_df = read_clusters_mapping(cargo_type)
# Из таблицы соответствия выбираем, какой столбец использовать для группировки кластеров
cluster_column = get_cluster_column_from_mapping(is_heavy_cargo)
cluster_column_jewelry = get_cluster_column_from_mapping(is_heavy_cargo, specific_mapping=True)
# Объединяем несколько кластеров в один
metrics_df_union_clusters = replace_clusters(clusters_mapping_df, cluster_column, metrics_df_all_clusters)
# metrics_df_union_clusters = metrics_df.copy()
# Расчитываем доп. колонки с оборачиваемостью и потребностью для листа "По кластерам"
metrics_df_with_stats = calc_svod_for_clusters(metrics_df_union_clusters)
# Группировка по кластеру (лист "Сводная по кластерам")
metrics_df_by_cluster, cluster_list = calc_svod_by_clusters(metrics_df_with_stats)
# Лист "Всего". Берем из файла с метриками
metrics_df_by_sku = read_metrics_file(date_report_created)
# Считаем доп. колонки с оборачиваемостью и потребностью для листа "Всего"
metrics_df_by_sku_stats = calc_svod_for_sku(metrics_df_by_sku)
# Добавляем колонки с потребностью по кластерам
svod = add_demand_for_cluster_columns(metrics_df_by_cluster, metrics_df_by_sku_stats, cluster_list)
# Добавляем колонки, заполняемые вручную
catalog = read_catalog()
svod_with_catalog_by_sku = add_columns_from_catalog(svod, catalog)
# svod_with_catalog_by_sku = svod.copy()
svod_with_catalog_for_clusters = add_columns_from_catalog(svod_for_clusters, catalog)
# svod_with_catalog_for_clusters = svod_for_clusters.copy()
# Чтение файла с датами формирования выгрузок
report_dates = read_dates_file(date_report_created)
# Создаем свод для записи в excel на лист "Всего"
svod_excel_by_sku = add_columns_for_excel(svod_with_catalog_by_sku, cluster_list,
                                   date_report_created,
                                   report_dates['date_start_file'][0],
                                   report_dates['date_end_file'][0],
                                   svod_type='Всего')
# Создаем свод для записи в excel на лист "Всего по ювелирным кластерам"
svod_excel_by_sku_jewelry = create_jewelry_clusters_svod(clusters_mapping_df, metrics_df_all_clusters ,cluster_column_jewelry)
# Создаем свод для записи в excel на лист "По кластерам"
svod_excel_for_clusters = add_columns_for_excel(svod_with_catalog_for_clusters, cluster_list,
                                   date_report_created,
                                   report_dates['date_start_file'][0],
                                   report_dates['date_end_file'][0],
                                   svod_type='По кластерам')
# Сохранение в Excel
save_sheets_to_excel(svod_excel_by_sku,
                     svod_excel_by_sku_jewelry,
                     svod_excel_for_clusters,
                     clusters_mapping_df,
                     date_report_created
                     )
# Форматирование файла Excel
format_excel(
    client_name,
    date_report_created,
    clusters_mapping_df,
    cluster_column,
    cluster_column_jewelry,
    svod_excel_by_sku,
    svod_excel_by_sku_jewelry,
    svod_excel_for_clusters
)
