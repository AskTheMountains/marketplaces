# Пути к файлам
imported_file = "only_imported_modules.txt"
freeze_file = "actual_modules_versions.txt"  # файл с pip freeze
output_file = "requirements.txt"

# Читаем используемые библиотеки
with open(imported_file, "r", encoding="utf-8") as f:
    # Получаем только имена библиотек, без версий и с учетом регистра
    imported = {line.strip().split('==')[0].lower() for line in f if line.strip()}

# Читаем актуальные версии (pip freeze)
version_dict = {}
with open(freeze_file, "r", encoding="utf-8") as f:
    for line in f:
        if '==' in line:
            pkg, ver = line.strip().split('==', 1)
            version_dict[pkg.lower()] = line.strip()  # сохраняем всю строку (с регистром)

# Формируем requirements.txt
with open(output_file, "w", encoding="utf-8") as f:
    for pkg in imported:
        if pkg in version_dict:
            f.write(version_dict[pkg] + "\n")
        else:
            # Можно сообщить об отсутствующих библиотеках, если нужно
            print(f"Внимание! Библиотека '{pkg}' не найдена в pip freeze.")
