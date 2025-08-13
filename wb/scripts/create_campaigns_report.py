# %% Определение функций
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
pd.options.mode.chained_assignment = None  # default='warn'

# Файл с настройками и номером клиента
from options import settings, headers, client_number


# Функция получения списка кампаний
def get_company_list(
        headers,
        date_start = str(date.today() - timedelta(days=30)),
        date_end = str(date.today()),
        filter_companies=[]
):
    # Делаем запрос для получения списка кампаний
    resp_data_company_list = requests.get("https://advert-api.wildberries.ru/adv/v1/promotion/count", headers=headers).json()
    # Переводим в df
    df_company_list_stats = pd.DataFrame(resp_data_company_list['adverts'])
    # Распаковываем список кампаний
    df_company_list_unpacked = df_company_list_stats.explode(column='advert_list').reset_index()
    # Получаем id кампаний
    df_company_ids = pd.json_normalize(df_company_list_unpacked['advert_list'])
    # Объединяем с исходным df
    df_company_list = pd.concat([df_company_ids, df_company_list_unpacked], axis=1)
    # Переводим колонку с датой в tiemstamp
    df_company_list['changeTime'] = pd.to_datetime(df_company_list['changeTime'], format='mixed').dt.tz_localize(None)
    # Если требуется, делаем фильтр по дате
    if len(filter_companies) > 0:
        # dt_start = pd.to_datetime(date_start).tz_localize(None)
        # dt_end = pd.to_datetime(date_end).tz_localize(None)
        # df_company_list = df_company_list.loc[df_company_list['changeTime'].between(dt_start, dt_end, inclusive='both')]
        df_company_list = df_company_list.loc[df_company_list['advertId'].isin(filter_companies), :]

    return df_company_list


# Функция получения информации о кампаниях
def get_companies_info(headers, df_company_list):
    # Создаем копию для избежания изменений в оригинальном df
    df_company_list_ = df_company_list.copy()
    # Разбиваем df с кампаниями на интервалы по 50 кампаний
    step = 50
    df_company_list_['id'] = np.arange(0, len(df_company_list_))
    df_company_list_['chunks'] = df_company_list_['id'].apply(lambda x: int(x/step) + 1)
    # df для каждого из типов кампаний
    df_companies_6 = pd.DataFrame()
    df_companies_8 = pd.DataFrame()
    df_companies_9 = pd.DataFrame()
    logger.info("Uploading companies info")
    # Цикл по каждому интервалу
    for chunk in df_company_list_['chunks'].unique():
        # Получаем список кампаний
        df_company_list_chunk = df_company_list_.loc[df_company_list_['chunks'] == chunk, :]
        company_list = df_company_list_chunk['advertId'].to_list()
        # Параметры запроса
        # params_companies_info = {company_list}
        resp_data_companies_info = requests.post("https://advert-api.wildberries.ru/adv/v1/promotion/adverts", headers=headers, json=company_list).json()
        # Переводим в df
        tmp_df_companies_info = pd.DataFrame(resp_data_companies_info)
        # Переводим колонки с датой в timestamp
        for col in ['endTime', 'createTime', 'changeTime', 'startTime']:
            tmp_df_companies_info[col] = pd.to_datetime(tmp_df_companies_info[col], format='mixed')
        # Делаем выборку на каждый из типов кампаний
        tmp_df_companies_6 = tmp_df_companies_info.loc[~tmp_df_companies_info['type'].isin([8, 9]), :]
        tmp_df_companies_8 = tmp_df_companies_info.loc[tmp_df_companies_info['type'] == 8, :]
        tmp_df_companies_9 = tmp_df_companies_info.loc[tmp_df_companies_info['type'] == 9, :]
        # Объединяем с предыдущей итерацией цикла
        df_companies_6 = pd.concat([df_companies_6, tmp_df_companies_6])
        df_companies_8 = pd.concat([df_companies_8, tmp_df_companies_8])
        df_companies_9 = pd.concat([df_companies_9, tmp_df_companies_9])

    # Обработка данных кампаний со статусами, отличными от 8 и 9
    if not df_companies_6.empty:
        # Делаем reset_index после loc
        df_companies_6 = df_companies_6.reset_index(drop=True)
        # Убираем лишние колонки
        df_companies_6 = df_companies_6.loc[:, ~df_companies_6.columns.isin(['nmCPM', 'unitedParams', 'autoParams'])]
        # Переводим колонки с датой в timestamp
        # for col in ['endTime', 'createTime', 'changeTime', 'startTime']:
        #     df_companies_6[col] = pd.to_datetime(df_companies_6[col], format='mixed')
        # Получаем параметры кампании из списка
        df_companies_6_params = (
            df_companies_6
            .loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'params']]
            .explode(column='params')
            .reset_index(drop=True)
        )
        # Получаем параметры кампании в виде столбцов df
        df_companies_6_params = pd.concat([
            df_companies_6_params,
            pd.json_normalize(df_companies_6_params['params'])
        ], axis=1)
        # Получаем список номенклатур кампании
        df_companies_6_nms = df_companies_6_params.explode(column='nms').reset_index(drop=True)
        # Получаем список номенклатур кампании в виде столбцов df
        df_companies_6_nms = pd.concat([df_companies_6_nms, pd.json_normalize(df_companies_6_nms['nms'])], axis=1)
        # Убираем лишние колонки
        df_companies_6_nms = df_companies_6_nms.loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'nm']]
        # Переименовываем колонку чтобы дальше использовать concat
        df_companies_6_nms = df_companies_6_nms.rename(columns={'nm': 'nms'})
    else:
        df_companies_6_nms = pd.DataFrame()

    # Обработка данных кампаний со статусом 8
    if not df_companies_8.empty:
        # Делаем reset_index после loc
        df_companies_8 = df_companies_8.reset_index(drop=True)
        # Убираем лишние колонки
        df_companies_8 = df_companies_8.loc[:, ~df_companies_8.columns.isin(['nmCPM', 'unitedParams'])]
        # Переводим колонки с датой в timestamp
        # for col in ['endTime', 'createTime', 'changeTime', 'startTime']:
        #     df_companies_8[col] = pd.to_datetime(df_companies_8[col], format='mixed')
        # Получаем параметры кампании из списка
        df_companies_8_params = (
            df_companies_8
            .loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'autoParams']]
        )
        # Получаем параметры кампании в виде столбцов df
        df_companies_8_params = pd.concat([df_companies_8_params, pd.json_normalize(df_companies_8_params['autoParams'])], axis=1)
        # Получаем список номенклатур
        df_companies_8_nms = df_companies_8_params.explode(column='nms')
        # Убираем лишние колонки
        df_companies_8_nms = df_companies_8_nms.loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'nms']]
    else:
        df_companies_8_nms = pd.DataFrame()

    # Обработка данных кампаний со статусами, отличными от 8 и 9
    if not df_companies_9.empty:
        # Делаем reset_index после loc
        df_companies_9 = df_companies_9.reset_index(drop=True)
        # Убираем лишние колонки
        df_companies_9 = df_companies_9.loc[:, ~df_companies_9.columns.isin(['nmCPM', 'autoParams'])]
        # Переводим колонки с датой в timestamp
        # for col in ['endTime', 'createTime', 'changeTime', 'startTime']:
        #     df_companies_9[col] = pd.to_datetime(df_companies_9[col], format='mixed')
        # Получаем параметры кампании из списка
        df_companies_9_params = (
            df_companies_9
            .loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'unitedParams']]
            .explode(column='unitedParams')
            .reset_index(drop=True)
        )
        # Получаем параметры кампании в виде столбцов df
        df_companies_9_params = pd.concat([
            df_companies_9_params,
            pd.json_normalize(df_companies_9_params['unitedParams'])
        ], axis=1)
        # Получаем список номенклатур кампании
        df_companies_9_nms = df_companies_9_params.explode(column='nms').reset_index(drop=True)
        # Получаем список номенклатур кампании в виде столбцов df
        # df_companies_9_nms = pd.concat([df_companies_9_nms, pd.json_normalize(df_companies_9_nms['nms'])], axis=1)
        # Убираем лишние колонки
        df_companies_9_nms = df_companies_9_nms.loc[:, ['endTime', 'createTime', 'changeTime', 'startTime', 'advertId', 'nms']]
    else:
        df_companies_9_nms = pd.DataFrame()

    # Формируем итоговый список номенклатур по всем кампаниям
    df_companies_nms_all = pd.concat([df_companies_6_nms, df_companies_8_nms, df_companies_9_nms])
    # Переименовываем некоторые колонки для удобства
    df_companies_nms_all = df_companies_nms_all.rename(columns={
        'nms': 'Артикул WB'
    })
    # Получаем количество артикулов в кампании
    df_companies_nms_all['Количество артикулов в кампании'] = df_companies_nms_all.groupby('advertId').transform('size')

    # Формируем словарь с результатами
    result_companies_info = {
        'df_companies_list': df_company_list,
        'df_companies_nms_all': df_companies_nms_all
    }

    return result_companies_info


def get_company_stats(
        headers,
        result_companies_info,
        date_start,
        date_end
):
    # df, в который будем помещать результаты выгрузки по апи
    df_companies_stats = pd.DataFrame()

    # Получаем df со списком кампаний
    df_companies_list = result_companies_info['df_companies_list'].copy()

    # Формируем столбцы с датами для запроса к апи
    # Генерация списка дат с частотой одного дня
    date_range = pd.date_range(start=date_start, end=date_end, freq='D')
    # Преобразование дат в строки
    date_list = date_range.strftime('%Y-%m-%d').tolist()

    # df_companies_list['date_start'] = date_start
    # df_companies_list['date_end'] = date_end
    # Для запроса с датами
    # df_companies_list['dates'] = df_companies_list[['date_start', 'date_end']].values.tolist()
    df_companies_list['dates'] = [date_list] * len(df_companies_list)
    # Для запроса с интервалом
    dates_interval = {'interval': {
        'begin': date_start,
        'end': date_end
        }
    }
    df_companies_list['interval'] = [dates_interval] * len(df_companies_list)
    df_companies_list = df_companies_list.loc[:, ~df_companies_list.columns.isin(['date_start', 'date_end'])]

    # Разбиваем df на диапазоны по 100 измерений
    step = 100
    df_companies_list['id'] = np.arange(0, len(df_companies_list))
    df_companies_list['chunks'] = df_companies_list['id'].apply(lambda x: int(x/step) + 1)
    max_chunks = max(df_companies_list['chunks'])

    logger.info("Uploading companies stats")
    # Цикл по каждому диапазону
    for chunk in df_companies_list['chunks'].unique():
        # Выбираем нужный диапазон и колонки
        tmp_df_companies_list = df_companies_list.loc[df_companies_list['chunks'] == chunk, ['advertId', 'dates']]
        # Переименовываем колонку для соответствия параметрам запроса
        tmp_df_companies_list = tmp_df_companies_list.rename(columns={'advertId': 'id'})
        # Переводим в словарь (это и будут параметры запроса)
        params_companies_stats = tmp_df_companies_list.to_dict(orient='records')
        logger.info(f"Uploading {chunk} of {max_chunks} chunks")
        # Делаем запрос
        resp_data_companies_stats = requests.post("https://advert-api.wildberries.ru/adv/v2/fullstats", headers=headers, json=params_companies_stats)
        # Если пришли данные по кампаниям
        if resp_data_companies_stats.status_code == 200:
            # Переводим в словарь
            resp_data_companies_stats = resp_data_companies_stats.json()
            # Переводим в df
            tmp_df_companies_stats = pd.DataFrame(resp_data_companies_stats)
            # Объединяем с предыдущим проходом цикла
            df_companies_stats = pd.concat([df_companies_stats, tmp_df_companies_stats])
            # Ждем 1 минуту до выгрузки следующего диапазона кампаний (если это не последний диапазон)
            logger.info("Waiting 1 minute before uploading next chunk")
            if chunk != max_chunks:
                time.sleep(60)
        # Если не пришли данные по кампаниям, выводим ошибку
        else:
            print(f"{resp_data_companies_stats.json()}")
            if chunk != max_chunks:
                time.sleep(60)
                # Ждем 1 минуту до выгрузки следующего диапазона кампаний (если это не последний диапазон)
                logger.info("Waiting 1 minute before uploading next chunk")

    # Убираем лишние колонки
    df_companies_stats.loc[:, ~df_companies_stats.columns.isin(['dates', 'days', 'boosterStats'])]

    # # Получаем df с номенклатурами кампаний
    # df_companies_nms = result_companies_info['df_companies_nms_all']

    # # Распаковываем статистику по дням
    # df_companies_stats_unpacked = df_companies_stats.explode(column='days').reset_index(drop=True)
    # df_companies_stats_by_days = pd.concat([
    #     pd.json_normalize(df_companies_stats_unpacked['days']),
    #     df_companies_stats_unpacked['advertId']
    #     ], axis=1)

    # # Распаковываем статистику по платформам
    # df_by_days_unpacked = df_companies_stats_by_days.explode(column='apps').reset_index(drop=True)
    # df_by_apps = pd.concat([
    #     pd.json_normalize(df_by_days_unpacked['apps']),
    #     df_by_days_unpacked['advertId']
    #     ], axis=1)

    # # Распаковываем статистику по артикулам
    # df_by_apps_unpacked = df_by_apps.explode(column='nm').reset_index(drop=True)
    # df_by_nms = pd.concat([
    #     pd.json_normalize(df_by_apps_unpacked['nm']),
    #     df_by_apps_unpacked['advertId']
    #     ], axis=1)

    # # Мерджим со статистикой по кампаниям
    # df_companies_stats_with_sku = df_companies_stats.merge(df_companies_nms,
    #                                                        how='left',
    #                                                        on='advertId',
    #                                                        indicator=True)

    return df_companies_stats


# Функция получения истории затрат
def get_costs_history(headers, date_start, date_end):
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
        'date_start': dt_start.strftime('%Y-%m-%d'),
        'date_end': first_month_end.strftime('%Y-%m-%d'),
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
            'date_start': monthly_start.strftime('%Y-%m-%d'),
            'date_end': monthly_end.strftime('%Y-%m-%d'),
            'dt_start': monthly_start,
            'dt_end': monthly_end
        })

        # Переход к следующему месяцу
        current_start = monthly_start + pd.DateOffset(months=1)

    # Создание датафрейма
    date_range_df = pd.DataFrame(intervals)

    # df, куда будем помещать результаты истории затрат
    df_history_costs = pd.DataFrame()

    # Сколько всего нужно выгрузить интервалов
    total_intervals = date_range_df.shape[0]

    # Делаем запрос для каждого интервала
    for i in range(date_range_df.shape[0]):
        logger.info(f"Uploading history costs for dates "
                    f"{date_range_df['date_start'][i]} - {date_range_df['date_end'][i]}"
                    )
        # Параметры запроса
        params_history_costs = {
            'from': date_range_df['date_start'][i],
            'to': date_range_df['date_end'][i]
        }
        # Делаем запрос
        result_history_costs = requests.get('https://advert-api.wildberries.ru/adv/v1/upd',
                                            headers=headers,
                                            params=params_history_costs).json()
        # Получаем результаты
        tmp_df_history_costs = pd.DataFrame(result_history_costs)
        # Добавляем в резульаты даты выгрузки
        tmp_df_history_costs = tmp_df_history_costs.assign(
            date_start = date_range_df['date_start'][i],
            date_end = date_range_df['date_end'][i]
        )

        # Объединяем с предыдущим проходом цикла
        df_history_costs = pd.concat([df_history_costs, tmp_df_history_costs])

        # Ждем 1 минуту перед следующим запросом, если это не последний интервал
        if (i + 1) != total_intervals:
            logger.info("Wating 1 minute before uploading next interval")
            time.sleep(60)
        else:
            logger.info("Done uploading history costs")

    # Сбрасываем index после concat
    df_history_costs = df_history_costs.reset_index(drop=True)

    # Считаем расходы на каждую из кампаний
    df_history_costs_stats = (
        df_history_costs
        .groupby(['advertId'])
        #.groupby(['updNum', 'advertId'])
        .agg(
            Расходы=('updSum', 'sum')
        )
        .reset_index()
        .rename(columns={
            'updNum': 'Номер документа',
            'advertId': 'ID Кампании'
        })
    )

    return df_history_costs_stats

# Функция разбивки рекламных расходов по артикулам
def calc_costs_by_sku(result_companies_info, df_costs_history_stats):
    # Получаем список номенклатур в кампаниях
    df_companies_nms_all = result_companies_info['df_companies_nms_all'].copy()
    # Убираем колонку с количеством артикулов в кампании, чтобы посчитать её заново
    df_companies_nms_all = df_companies_nms_all.loc[:, ~df_companies_nms_all.columns.isin(['Количество артикулов в кампании'])]
    # Получаем количество артикулов в кампании
    df_companies_nms_all['Количество артикулов в кампании'] = df_companies_nms_all.groupby('advertId').transform('size')

    # Мерджим список номенклатур с корректными расходами на кампанию
    df_companies_sku = df_costs_history_stats.merge(df_companies_nms_all,
                                                    on='advertId',
                                                    how='left')

    # Разбиваем расходы по артикулам, если их было несколько в одной кампании
    df_companies_sku['Разбивка расходов на кампанию'] = df_companies_sku['Расходы'] / df_companies_sku['Количество артикулов в кампании']

    # Считаем расходов по каждому артикулу
    df_companies_sku_stats = (
        df_companies_sku
        .groupby(['Артикул WB'])
        .agg(
            Расходы_на_продвижение=('Разбивка расходов на кампанию', 'sum')
            )
        .reset_index()
    )

    return df_companies_sku_stats


# %% Вызов всех функций
if __name__ == '__main__':
    date_start = '2025-05-19T00:00:00.000Z'
    date_end = '2025-05-25T00:00:00.000Z'
    # Кампании, по которым нужно получить отчет
    filter_companies = [24205903]
    # Получаем список кампаний
    df_company_list = get_company_list(headers, date_start, date_end, filter_companies=filter_companies)
    # Получаем информацию о номенклатурах кампаний
    result_companies_info = get_companies_info(headers, df_company_list)
    # Получаем статистику кампаний
    df_companies_stats = get_company_stats(headers, result_companies_info, date_start, date_end)
    # Получаем историю затрат (для корректных расходов на кампании)
    df_costs_history_stats = get_costs_history(headers, date_start, date_end)
    # Считаем расходы на номенклатуры в кампании
    # df_companies_sku_stats = calc_costs_by_sku(result_companies_info, df_costs_history_stats)
