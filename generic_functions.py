import pandas as pd
import warnings

def move_columns(df, columns_to_move, position, insert_type='after'):
    """
    Перемещает указанные колонки в DataFrame на новую позицию.

    :param df: pandas DataFrame, в котором нужно переместить колонки
    :param columns_to_move: список названий колонок или одно название (строка) для перемещения
    :param position: номер позиции или название колонки для вставки
    :param insert_type: 'before' или 'after', определяющее, куда вставлять колонки
    :return: DataFrame с перемещёнными колонками
    """

    existing_columns = df.columns.tolist()

    # Приводим columns_to_move к списку (на случай, если передали строку)
    if isinstance(columns_to_move, str):
        columns_list = [columns_to_move]
    else:
        columns_list = list(columns_to_move)  # поддержка для любого итерируемого

    # Определяем, какие из указанных колонок реально существуют в DataFrame
    found_columns = [col for col in columns_list if col in existing_columns]
    not_found_columns = [col for col in columns_list if col not in existing_columns]

    # Если некоторые колонки не найдены — предупреждаем об этом
    if not_found_columns:
        print(f"\nВ DataFrame отсутствуют следующие колонки:\n  {not_found_columns}")

    # Если не найдено ни одной колонки для перемещения — возвращаем копию исходного DataFrame
    if not found_columns:
        return df.copy()

    # Формируем список столбцов, которые остаются на местах
    remaining_columns = [col for col in existing_columns if col not in found_columns]

    # --- ЛОГИКА ОПРЕДЕЛЕНИЯ ПОЗИЦИИ ВСТАВКИ ---

    if isinstance(position, int):
        # Пользователь хочет вставить колонки в позицию с заданным индексом
        # Проверяем, что индекс допустимый
        if position < 0 or position > len(remaining_columns):
            raise ValueError("Позиция должна быть в пределах от 0 до {}".format(len(remaining_columns)))
        # Формируем новый порядок: (до позиции) + (перемещаемые колонки) + (после позиции)
        new_order = remaining_columns[:position] + found_columns + remaining_columns[position:]

    elif isinstance(position, str) and position in existing_columns:
        # Пользователь указал имя колонки для вставки до или после
        # Может быть ситуация, что эта колонка попадает в список перемещаемых (в таком случае она уже не в remaining_columns)
        if position not in remaining_columns:
            # Если колонка-ориентир перемещения совпадает с одной из перемещаемых колонок,
            # вставляем найденные колонки в начало списка (позиция = 0)
            pos_index = 0
        else:
            # Индекс колонки-ориентира в списке оставшихся
            pos_index = remaining_columns.index(position)

        if insert_type == 'before':
            # Вставляем перемещаемые колонки перед найденным индексом
            new_order = remaining_columns[:pos_index] + found_columns + remaining_columns[pos_index:]
        elif insert_type == 'after':
            # Вставляем после найденного индекса
            new_order = remaining_columns[:pos_index + 1] + found_columns + remaining_columns[pos_index + 1:]
        else:
            # На случай ошибки в параметрах
            raise ValueError("insert_type должен быть 'before' или 'after'")
    else:
        # Если позиция указана неправильно (не число и не существующее имя колонки)
        raise ValueError("position должен быть либо индексом, либо названием существующей колонки")

    # Возвращаем DataFrame с новым порядком столбцов, основная логика закончена
    return df[new_order]



def add_element_to_list(lst, search, new_value, after=True, by_index=False):
    """
    Вставляет новый элемент (или элементы) до или после указанного значения или индекса.

    :param lst: исходный список (list)
    :param search: значение или индекс, относительно которого будет вставка
    :param new_value: значение или список значений для вставки
    :param after: если True — после; если False — до
    :param by_index: если True — поиск по индексу, если False — по значению
    :return: новый список
    """
    # Определяем значения для вставки
    if isinstance(new_value, list) and not isinstance(new_value, str):
        values_to_insert = new_value
    else:
        values_to_insert = [new_value]

    # Работа с пустым списком
    if not lst:
        return values_to_insert

    if by_index:
        idx = search
        if not isinstance(idx, int) or not (-len(lst) <= idx < len(lst)):
            print(f"Индекс {idx} вне диапазона списка.")
            return lst
        # Для отрицательных индексов Python сам корректно вставит
    else:
        try:
            idx = lst.index(search)
        except ValueError:
            print(f"{search} не найден в списке.")
            return lst

    insert_at = idx + 1 if after else idx
    return lst[:insert_at] + values_to_insert + lst[insert_at:]
