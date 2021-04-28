import csv

load_file = 't.csv' # Название файла, который нужно загрузить
result_file = 'result.csv' # Название файла, который выгрузится

# Тэги, добавляемые в конец поля IE_DETAIL_TEXT
tag1 = '#PARAMETERS_SKU_TAG#'
tag2 = '#MOTHERLAND_SKU_TAG#'

headers = ['IE_XML_ID', 'IE_NAME', 'IE_DETAIL_TEXT'] # Заголовки в начале csv файла


mass = []
with open (load_file, encoding='utf-8') as File:
    reader = csv.reader(File, delimiter=';')
    next(reader, None) # Пропускаем заголовок csv-файла при загрузке данных

    for row in reader:

        # Добавляем тэги в конец последнего элемента саиска - IE_DETAIL_TEXT
        text = row.pop() + '\n' + tag1 + '\n' + tag2
        row.insert(2, text)

        mass.append(row)

with open(result_file, 'w', encoding='cp1251', errors='replace') as f:
     write = csv.writer(f, delimiter=';')
     write.writerow(headers)
     write.writerows(mass)