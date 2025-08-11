# %% Определение всех функций
import pandas as pd
import numpy as np
import os
import openpyxl
import json
from loguru import logger
from datetime import date,datetime,timedelta
from itertools import product



# Файл с некоторыми константами
from ozon.scripts.constants import(
    client_name,
    marketplace_dir_name,
    shop_type,
    clusters_mapping,
    clusters_mapping_jewelry,
    catalog_supply_columns,
    svod_total_columns,
    svod_clusters_columns
)
# Файл с функциями форматирования листов excel
import ozon.scripts.format_supply_svod as format_supply_svod


# Директории с загруженными файлами из апи
uploaddir = f"{marketplace_dir_name}/Clients/{client_name}/UploadFiles"
# uploaddir_today = f"{uploaddir}/UploadFiles_{str(date.today())}"

# Директория с метриками
metricsdir = f"{marketplace_dir_name}/Clients/{client_name}/Metrics"

# Таблица объединения кластеров в один (какие в какой объединяем)
clusters_mapping_df = pd.DataFrame(clusters_mapping)
clusters_mapping_jewelry_df = pd.DataFrame(clusters_mapping_jewelry)


# Функция чтения дат формирования отчета
def read_dates_file(date_report_created):
    report_dates = pd.read_csv(f"{uploaddir}/UploadFiles_{date_report_created}/{date_report_created}_dates_from_to.csv", sep=';')
    for col in report_dates:
        report_dates[col] = pd.to_datetime(report_dates[col])
    return report_dates


# Функция чтения файла с метриками
def read_metrics_file(date_report_created, sheet_name='summary'):
    metrics = pd.read_excel(f"{metricsdir}/{date_report_created}_МетрикиИтоги.xlsx",
                            sheet_name=sheet_name)
    # Удаляем ненужные колонки
    for col in ['Размер', 'Цвет', 'РРЦ']:
        if col in metrics.columns:
            metrics.drop(columns=col, inplace=True)
    return metrics


# Добавление недостающих кластеров к товарам
def add_clusters(date_report_created):
    # Чтение файла с метриками
    metrics_clusters = read_metrics_file(date_report_created, sheet_name='claster_report')

    # Чтение файла с метрками (лист со всеми товарами)
    metrics_summary = read_metrics_file(date_report_created, sheet_name='summary')

    # Чтение файла с кластерами
    clusters = pd.read_csv(f'{marketplace_dir_name}/scripts/clusters_and_warehouses.csv', sep=';')
    # clusters.drop('Unnamed: 0', axis=1, inplace=True)

    # Получение уникальных комбинаций Артикул-Кластер
    sku_clusters = list(
        product(
            metrics_summary['SKU'].unique(),
            clusters['cluster_name'].unique()
        )
    )
    sku_clusters = pd.DataFrame(sku_clusters, columns=['SKU', 'Кластер'])
    # Колонки, содержащие данные метрик (продажи, заказы, остатки, поставки)
    sales_columns = ['Заказы', 'Заказы_руб', 'Продажи', 'ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ', 'Остатки', 'Остатки_fbs']
    # Мердж с комбинациями Артикул-Кластер.
    # Таким образом, получим все товары во всех кластерах
    metrics_df_all_clusters = sku_clusters.merge(metrics_clusters[['SKU', 'Кластер'] + sales_columns],
                                    on=['SKU', 'Кластер'],
                                    how='left')
    # metrics_df_all_clusters = metrics_cluster.merge(sku_clusters,
    # 								left_on=['SKU', 'Кластер'],
    # 								right_on=['SKU', 'cluster'],
    # 								how='right')
    # metrics_df_all_clusters.sort_values(by=['SKU', 'cluster'], inplace=True)

    # В пропусках считаем, что в данном кластере по данному товару
    # не было товаров, заказов и продаж
    for col in sales_columns:
        metrics_df_all_clusters[col] = metrics_df_all_clusters[col].fillna(0)

    # Мердж для получения остальных колонок
    metrics_df_all_clusters = metrics_df_all_clusters.merge(metrics_summary.drop(columns=sales_columns),
                                       on='SKU',
                                       how='left')
    # Заполняем пропуски в остальных колонках,
    # которые получились в результате мерджа
    # for col in metrics_df_all_clusters.columns[:-2]:
    # 	metrics_df_all_clusters[col] = metrics_df_all_clusters.groupby('SKU')[col].ffill().bfill()

    # Меняем старый кластер на кластер из мерджа
    # metrics_df_all_clusters['Кластер'] = metrics_df_all_clusters['cluster']
    # Удаляем вспомогательные колонки
    # metrics_df_all_clusters.drop(columns=['SKU', 'cluster'], inplace=True)
    return metrics_df_all_clusters


# Объединение нескольких кластеров в один
def replace_clusters(
        clusters_mapping_df,
        df_with_clusters,
    ):

    # Создаем копию для избежания изменений в оригинальном df
    df_with_replaced_clusters = df_with_clusters.copy()
    # Замена названий кластеров только на нужные
    # (несколько кластеров могут сливаться в один)
    for i in range(len(clusters_mapping_df)):
        df_with_replaced_clusters.loc[df_with_replaced_clusters['Кластер'].isin(clusters_mapping_df["subcluster"][i]), "Кластер"] = clusters_mapping_df["cluster"][i]

    return df_with_replaced_clusters


# Расчет потребности и оборачиваемости для товаров по кластерам
# лист 'По Кластерам'
def calc_svod_for_clusters(metrics_df_all_clusters):
    # Создаем копию для избежания изменений в оригинальном df
    svod_for_clusters = metrics_df_all_clusters.copy()
    # Расчет доп. колонок (лист 'По кластерам')
    # svod_for_clusters['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0
    svod_for_clusters['Оборачиваемость'] = (svod_for_clusters['Заказы'] + svod_for_clusters['Продажи']) / 2 / 30
    svod_for_clusters['Потребность на 40 дней'] = svod_for_clusters['Оборачиваемость'] * 40
    svod_for_clusters['Дефицит/Избыток 40 дней'] = svod_for_clusters['Остатки'] + svod_for_clusters['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - svod_for_clusters['Потребность на 40 дней']
    # svod_for_clusters['Дефицит/Избыток 40 дней'] = svod_for_clusters['Остатки'] +  svod_for_clusters['Товары_в_пути'] - svod_for_clusters['Потребность на 40 дней']
    svod_for_clusters['Потребность на 40 дней (округл.)'] = np.where(svod_for_clusters['Дефицит/Избыток 40 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(svod_for_clusters['Дефицит/Избыток 40 дней']))
)
    svod_for_clusters['Потребность на 60 дней'] = svod_for_clusters['Оборачиваемость'] * 60
    svod_for_clusters['Дефицит/Избыток 60 дней'] = svod_for_clusters['Остатки'] + svod_for_clusters['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - svod_for_clusters['Потребность на 60 дней']
    # svod_for_clusters['Дефицит/Избыток 60 дней'] = svod_for_clusters['Остатки'] + svod_for_clusters['Товары_в_пути'] - svod_for_clusters['Потребность на 60 дней']
    svod_for_clusters['Потребность на 60 дней (округл.)'] = np.where(svod_for_clusters['Дефицит/Избыток 60 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(svod_for_clusters['Дефицит/Избыток 60 дней']))
    )
    svod_for_clusters['Корректировка с учетом нулевых остатков'] = svod_for_clusters['Потребность на 60 дней (округл.)'] # + svod_for_clusters['Потребность на 40 дней (округл.)']
    # Добавляем колонку с номером
    svod_for_clusters['№'] = svod_for_clusters.index + 1
    return svod_for_clusters


# Расчет сумм по кластерам после объединения кластеров
def calc_svod_by_clusters(metrics_df_with_stats):
    # Сумма по каждому кластеру
    metrics_df_by_sku_cluster = metrics_df_with_stats.groupby(['SKU', 'Кластер']).agg(
        Корректировка_с_учетом_нулевых_остатков=('Корректировка с учетом нулевых остатков', 'sum')
    ).reset_index()
    return metrics_df_by_sku_cluster


# Расчет потребности и оборачиваемости для товаров, группированных по SKU
# лист 'Всего'
def calc_svod_for_sku(metrics_df_by_sku):
    # Расчет доп. колонок (лист 'Всего')
    metrics_df_by_sku_ = metrics_df_by_sku.copy()
    metrics_df_by_sku_['Оборачиваемость'] = (metrics_df_by_sku_['Заказы'] + metrics_df_by_sku_['Продажи']) / 2 / 30
    # metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] = 0
    metrics_df_by_sku_['Потребность на 40 дней'] = metrics_df_by_sku_['Оборачиваемость'] * 40
    metrics_df_by_sku_['Дефицит/Избыток 40 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - metrics_df_by_sku_['Потребность на 40 дней']
    metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['Остатки_fbs'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - metrics_df_by_sku_['Потребность на 40 дней']
    # metrics_df_by_sku_['Дефицит/Избыток 40 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['Товары_в_пути'] - metrics_df_by_sku_['Потребность на 40 дней']
    metrics_df_by_sku_['Потребность на 40 дней (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 40 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 40 дней']))
    )
    metrics_df_by_sku_['Потребность на 40 дней с учетом FBS (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 40 дней с учетом FBS']))
    )
    metrics_df_by_sku_['Потребность на 60 дней'] = metrics_df_by_sku_['Оборачиваемость'] * 60
    metrics_df_by_sku_['Дефицит/Избыток 60 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['ОЖИДАЕТСЯ_ПОСТУПЛЕНИЕ'] - metrics_df_by_sku_['Потребность на 60 дней']
    # metrics_df_by_sku_['Дефицит/Избыток 60 дней'] = metrics_df_by_sku_['Остатки'] + metrics_df_by_sku_['Товары_в_пути'] - metrics_df_by_sku_['Потребность на 60 дней']
    metrics_df_by_sku_['Потребность на 60 дней (округл.)'] = np.where(metrics_df_by_sku_['Дефицит/Избыток 60 дней'] > 0,
                                                                  0,
                                                                  np.ceil(abs(metrics_df_by_sku_['Дефицит/Избыток 60 дней']))
    )
    # Добавляем колонку с номером
    metrics_df_by_sku_['№'] = metrics_df_by_sku_.index + 1
    return metrics_df_by_sku_


# Получение колонок потребности для конкретного кластера
def add_demand_for_cluster_columns(
        df_pivot_by_clusters,
        metrics_df_by_sku_stats,

    ):
    # # Создаем копию для избежания изменений в оригинальном df
    # metrics_df_by_cluster_ = metrics_df_by_cluster.copy()

    # # Объединяем кластеры кластеры, если нужно
    # if settings['replace_clusters'][client_number]:
    #     for i in range(len(clusters_mapping_df)):
    #         metrics_df_by_cluster_.loc[tmp_df['Кластер'].isin(clusters_mapping_df["subcluster"][i]), "Кластер"] = clusters_mapping_df["cluster"][i]
    # metrics_df_by_cluster_ = metrics_df_by_cluster_.groupby(['SKU', 'Кластер']).sum().reset_index()

    # Из сводной по кластерам берем нужный кластер и делаем merge по артикулу
    # for cluster in metrics_df_by_cluster['Кластер'].unique():
    #     tmp_df = metrics_df_by_cluster.loc[metrics_df_by_cluster['Кластер'] == cluster, ['SKU', 'Корректировка_с_учетом_нулевых_остатков']]
    #     tmp_df = tmp_df.rename(columns={'Корректировка_с_учетом_нулевых_остатков': 'Потребность' + ' ' + cluster})
    #     metrics_df_by_sku_with_stats = metrics_df_by_sku_with_stats.merge(tmp_df, how='left', on='SKU')

    # Делаем merge с сводной таблицей по кластерам
    svod_for_sku = (
        metrics_df_by_sku_stats
            .merge(
            df_pivot_by_clusters,
            how='left',
            on='Артикул'
        )
        .fillna(0)
    )

    return svod_for_sku


# Заполнение некоторых столбцов из справочных таблиц, заполненных вручную
def add_columns_from_catalog(metrics_df_by_sku_with_stats):
    # Создаем копию для избежания изменений в оригинальном df
    metrics_df_by_sku_with_stats_ = metrics_df_by_sku_with_stats.copy()
    # Считываем справочную таблицу
    catalog = pd.read_excel(f"{marketplace_dir_name}/Clients/{client_name}/catalog/Справочная_таблица_{client_name}_Ozon.xlsx")
    # Переименовываем колонку с артикулом
    catalog = catalog.rename(columns={'Артикул продавца': 'Артикул'})
    # Переводим колонку с артикулом в строку
    catalog['Артикул'] = catalog['Артикул'].astype(str)

    # metrics_df_by_sku_with_stats_.drop(columns=['Размер'], inplace=True)
    metrics_df_by_sku_with_stats_ = metrics_df_by_sku_with_stats_.merge(
        catalog[catalog_supply_columns],
        how='left',
        on='Артикул'
    )

    return metrics_df_by_sku_with_stats_


# Функция создания листа "Всего по ювелирным кластерам"
def create_jewelry_clusters_svod(metrics_df_all_clusters):
    if shop_type == 'jewelry':
        # Заменяем кластеры по маппингу ювелирных кластеров
        metrics_df_with_clusters = replace_clusters(clusters_mapping_jewelry_df, metrics_df_all_clusters, flag_replace_clusters=True)
        # Расчитываем доп. колонки с оборачиваемостью и потребностью для листа "По кластерам"
        svod_for_replaced_clusters = calc_svod_for_clusters(metrics_df_with_replaced_clusters)
        # Группировка по кластеру
        # metrics_df_by_cluster = calc_svod_by_clusters(metrics_df_with_stats)
        df_pivot_by_replaced_clusters = create_pivot_clusters(svod_for_replaced_clusters)
        # Лист "Всего". Берем из файла с метриками
        metrics_df_by_sku = read_metrics_file(date_report_created)
        # Считаем доп. колонки с оборачиваемостью и потребностью для листа "Всего"
        metrics_df_by_sku_stats = calc_svod_for_sku(metrics_df_by_sku)
        # Добавляем колонки с потребностью по кластерам
        svod_for_sku = add_demand_for_cluster_columns(df_pivot_by_replaced_clusters, metrics_df_by_sku_stats)
        # Добавляем колонки из справочной таблицы
        svod_with_catalog_for_sku = add_columns_from_catalog(svod_for_sku)
        # svod_with_catalog_by_sku = svod.copy()
        # svod_with_catalog_for_clusters = add_columns_from_catalog(svod_for_clusters)
        # Чтение файла с датами формирования выгрузок
        report_dates = read_dates_file(date_report_created)
        # Добавляем недостающие колонки для excel
        cluster_list = clusters_mapping_jewelry_df['cluster'].unique().tolist()
        svod_excel_by_sku = add_columns_for_excel(
            svod_with_catalog_for_sku,
            cluster_list,
            date_report_created,
            report_dates['date_start_file'][0],
            report_dates['date_end_file'][0],
            svod_type='Всего'
        )
    else:
        svod_excel_by_sku = None
    return svod_excel_by_sku


# Функция создания сводной таблицы по кластерам
def create_pivot_clusters(svod_for_clusters):
    # Считаем сводную таблицу
    df_pivot_by_clusters = (
        pd.pivot_table(
            svod_for_clusters,
            columns='Кластер',
            index='Артикул',
            values=['Корректировка с учетом нулевых остатков', 'Остатки',],
            aggfunc='sum',
            fill_value=0
        )
            # .add_prefix('Потребность ')
            # .reset_index()
    )
    # Объединяем мультииндексы в один с нужными префиксами
    df_pivot_by_clusters.columns = [
        f"Потребность {col[1]}" if col[0] == 'Корректировка с учетом нулевых остатков'
        else f"Остаток {col[1]}" if col[0] == 'Остатки'
        else col[0]
        for col in df_pivot_by_clusters.columns.values
    ]

    # Делаем reset_index после расчета сводной таблицы
    df_pivot_by_clusters = df_pivot_by_clusters.reset_index()

    # Делаем удобный порядок колонок
    clusters = svod_for_clusters['Кластер'].unique()
    cols_order = ['Артикул'] + [val for cluster in clusters for val in (f'Потребность {cluster}', f'Остаток {cluster}')]
    df_pivot_by_clusters = df_pivot_by_clusters[cols_order]


    return df_pivot_by_clusters


# Переименование и вставка колонок для соответствия шаблону
def add_columns_for_excel(
        df_with_stats,
        cluster_list,
        date_report_created,
        day_start,
        day_end,
        svod_type='Всего'
    ):

    # Создаем копию для избежания изменений в оригинальном df
    svod_excel = df_with_stats.copy()
    date_report_created_ = datetime.strptime(date_report_created, '%Y-%m-%d')


    # Колонки, которые должны быть в итоговом файле на листе "Всего"
    if svod_type == 'Всего':
        svod_columns = svod_total_columns
        cluster_columns = [val for cluster in cluster_list for val in (f'Потребность {cluster}', f'Остаток {cluster}')]
        svod_columns = svod_columns + cluster_columns
    # Колонки, которые должны быть в итоговом файле на листе "По кластерам"
    else:
        svod_columns = svod_clusters_columns

    # Добавляем недостающие колонки (они либо заполняются вручную,
    # либо добавляются в процессе)
    for col in svod_columns:
        if col not in svod_excel.columns.to_list():
            svod_excel[col] = np.nan

    # Порядок колонок
    svod_excel = svod_excel[svod_columns]

    # Сортировка
    svod_excel = svod_excel.sort_values(by=['Артикул',])

    # Переименование некоторых колонок для соответствия шаблону
    svod_excel.rename(columns={
        'SKU': 'Ozon SKU ID',
        'Наименование товара': 'Наименование',
        'Заказы': f"ЗАКАЗЫ с {day_start.strftime('%d.%m')} по {day_end.strftime('%d.%m')}, шт.",
        'Продажи': f"ПРОДАЖИ с {day_start.strftime('%d.%m')} по {day_end.strftime('%d.%m')}, шт.",
        'Остатки': f"ОСТАТОК {date_report_created_.strftime('%d.%m')}",
        'Остатки_fbs': f"ОСТАТОК FBS {date_report_created_.strftime('%d.%m')}",
        },
        inplace=True)



    # Запись в Excel
    # svod_excel.to_excel("Svod" +"\\" + date_report_created +"_Расчет поставок.xlsx", sheet_name='Всего', index=False)
    return svod_excel



# Функция сохранения данных в excel
def save_sheets_to_excel(
        svod_excel_for_sku,
        svod_excel_for_clusters,
        df_pivot_by_clusters,
        svod_excel_for_sku_jewelry,
        date_report_created
    ):
    # Задаем имя файла (с путем) для сохранения
    file_name_supply_svod = (
        f"{marketplace_dir_name}/Clients/{client_name}/SupplySvod/"
        f"{date_report_created}_Расчет_поставок_{client_name}_Ozon.xlsx"
    )
    with pd.ExcelWriter(file_name_supply_svod) as w:
        svod_excel_for_sku.to_excel(w, sheet_name='Всего', index=False)
        # df_pivot_by_clusters.to_excel(w, sheet_name='Сводная по кластерам', index=False)
        svod_excel_for_clusters.to_excel(w, sheet_name='По кластерам', index=False)
        if shop_type == 'jewelry':
            svod_excel_for_sku_jewelry.to_excel(w, sheet_name='Всего по ювелирным кластерам', index=False)
        # w.close()


# Функция форматирования Excel по отдельным листам (формулы, цвета и т.д.)
def format_excel(
        client_name,
        date_report_created,
        svod_excel_for_sku,
        svod_excel_for_sku_jewelry,
        svod_excel_for_clusters,
        clusters_mapping_df,
        clusters_mapping_jewelry_df
    ):
    # Определяем путь, где лежат расчеты поставок
    path_supply_svod = f"{marketplace_dir_name}/Clients/{client_name}/SupplySvod"
    # Создаем файл excel, в котором будет производиться форматирование
    format_supply_svod.copy_supply_svod_file(path_supply_svod, client_name, date_report_created)
    # Форматирование листа "Всего"
    format_supply_svod.format_sheet_total(
        path_supply_svod,
        client_name,
        date_report_created,
        svod_excel_for_sku,
        svod_excel_for_clusters,
        clusters_mapping_df,
        sheet_name='Всего'
    )
    # Форматирование листа "Всего по ювелирным кластерам"
    if shop_type == 'jewelry':
            format_supply_svod.format_sheet_total(
                path_supply_svod,
                client_name,
                date_report_created,
                svod_excel_for_sku_jewelry,
                svod_excel_for_clusters,
                clusters_mapping_jewelry_df,
                sheet_name='Всего по ювелирным кластерам'
            )
    # Форматирование листа "По кластерам"
    format_supply_svod.format_sheet_clusters(
        path_supply_svod,
        client_name,
        date_report_created,
        svod_excel_for_clusters
    )

    # Сохранение файла Excel
    # wb.save("Svod" + "\\" + date_report_created + "_Расчет Поставок_formatted.xlsx")


# %% Вызов всех функций
if __name__ == '__main__':
    # Дата формирования отчета
    # date_report_created = '2024-12-17'
    date_report_created = str(date.today())
    logger.info(f"Creating supply svod for client {client_name} for date {date_report_created}")
    # Добавляем недостающие кластеры
    metrics_df_all_clusters = add_clusters(date_report_created)
    # Отдельный df для записи в excel на лист "По кластерам" (без объединения кластеров)
    svod_for_clusters = calc_svod_for_clusters(metrics_df_all_clusters)
    # Сводная таблица по кластерам для записи в excel
    df_pivot_by_clusters = create_pivot_clusters(svod_for_clusters)
    # Объединяем несколько кластеров в один
    metrics_df_with_replaced_clusters = replace_clusters(clusters_mapping_df, metrics_df_all_clusters)
    # metrics_df_with_clusters = metrics_df.copy()
    # Расчитываем доп. колонки с оборачиваемостью и потребностью для листа "По кластерам"
    svod_for_replaced_clusters = calc_svod_for_clusters(metrics_df_with_replaced_clusters)
    # Группировка по кластеру
    # metrics_df_by_cluster = calc_svod_by_clusters(metrics_df_with_stats)
    df_pivot_by_replaced_clusters = create_pivot_clusters(svod_for_replaced_clusters)
    # Лист "Всего". Берем из файла с метриками
    metrics_df_by_sku = read_metrics_file(date_report_created)
    # Считаем доп. колонки с оборачиваемостью и потребностью для листа "Всего"
    metrics_df_by_sku_stats = calc_svod_for_sku(metrics_df_by_sku)
    # Добавляем колонки с потребностью по кластерам
    svod_for_sku = add_demand_for_cluster_columns(df_pivot_by_replaced_clusters, metrics_df_by_sku_stats)
    # Добавляем колонки из справочной таблицы
    svod_with_catalog_for_sku = add_columns_from_catalog(svod_for_sku)
    # svod_with_catalog_by_sku = svod.copy()
    svod_with_catalog_for_clusters = add_columns_from_catalog(svod_for_clusters)
    # svod_with_catalog_for_clusters = svod_for_clusters.copy()
    # Чтение файла с датами формирования выгрузок
    report_dates = read_dates_file(date_report_created)
    # Добавляем недостающие колонки для excel
    cluster_list = clusters_mapping_df['cluster'].unique().tolist()
    # Лист "Всего"
    svod_excel_for_sku = add_columns_for_excel(
        svod_with_catalog_for_sku,
        cluster_list,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0],
        svod_type='Всего'
    )
    # Лист "Всего по ювелирным кластерам"
    svod_excel_for_sku_jewelry = create_jewelry_clusters_svod(metrics_df_all_clusters)
    # Лист "По кластерам"
    svod_excel_for_clusters = add_columns_for_excel(
        svod_with_catalog_for_clusters,
        cluster_list,
        date_report_created,
        report_dates['date_start_file'][0],
        report_dates['date_end_file'][0],
        svod_type='По кластерам'
    )
    # Сохранение в Excel
    save_sheets_to_excel(
        svod_excel_for_sku,
        svod_excel_for_clusters,
        df_pivot_by_clusters,
        svod_excel_for_sku_jewelry,
        date_report_created
    )

    # Форматирование файла Excel
    format_excel(
        client_name,
        date_report_created,
        svod_excel_for_sku,
        svod_excel_for_sku_jewelry,
        svod_excel_for_clusters,
        clusters_mapping_df,
        clusters_mapping_jewelry_df
    )
