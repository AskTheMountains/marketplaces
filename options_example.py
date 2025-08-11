import json

# открываем файлик с API-ключами для передачи в заголовки запроса
def loadFromJSON(file_name):
    with open(file_name, 'r') as f:
        tmp_dict = json.load(f)
    return tmp_dict

# 0 - Example Client
settings = loadFromJSON('settings_all.json')
client_number = 0
