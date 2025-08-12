import shutil
import os

from loguru import logger

# Файл с некоторыми константами
from wb.scripts.constants import marketplace_dir_name, client_name


# Функция создания директорий (для новых клиентов)
def create_dirs():
    # Директория для клиента
    client_dir = f"{marketplace_dir_name}/Clients/{client_name}/"
    # Список директорий для создания
    dir_names = ['UploadFiles', 'Metrics', 'catalog', 'SupplySvod', 'Actions', 'FinanceReports', 'SaleSvod']
    for dir_name in dir_names:
        dir_path = os.path.join(client_dir, dir_name)
        if not os.path.exists(dir_path):
            logger.info(f"Creating folder {dir_path} for client {client_name}")
            os.makedirs(dir_path)
    logger.info(f"Done creating folders for client {client_name}")


if __name__ == '__main__':
    create_dirs()
