import os
from datetime import datetime
import pandas as pd

#------------------------------------------------------------
def get_all_files(folder_path):
    files_list = []
    # Рекурсивная функция для обхода всех файлов в папке и подпапках
    def recursive_list_files(folder):
        for root, dirs, files in os.walk(folder):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.isfile(file_path):
                    files_list.append(file_path)
    recursive_list_files(folder_path)
    return files_list
#------------------------------------------------------------
# Укажите путь к папке
folder_path = r"c:\Pet_project\source"
# Получите список всех файлов в папке и подпапках
all_files = get_all_files(folder_path)
# задаём нулевую итоговую таблицу
cumulative_rosreestr_dataframe=None
# считаем количество файлов в папке для счетчика
max_count_files=len(all_files)
# запускаем цикл обработки файлов в папке c 01 региона
folder_count='01'
for file_counter,file_to_read in enumerate(all_files):
    # берем данные из фала по квартирам
    if "flat_ds2" in file_to_read:
        #читаем файл
        #print(f'читаем файл {datetime.now().strftime("%H:%M:%S")}')
        rosreestr_dataframe=pd.read_json(file_to_read, dtype=False)
        #разворачиваем json в плоскую таблицу
        #print(f'разворачиваем json в плоскую таблицу {datetime.now().strftime("%H:%M:%S")}')
        rosreestr_dataframe=pd.concat([rosreestr_dataframe,pd.json_normalize(rosreestr_dataframe['right'])], axis=1)
        rosreestr_dataframe=pd.concat([rosreestr_dataframe,pd.json_normalize(rosreestr_dataframe['purpose'])], axis=1)
        rosreestr_dataframe=pd.concat([rosreestr_dataframe,pd.json_normalize(rosreestr_dataframe['address'])], axis=1)
        rosreestr_dataframe = rosreestr_dataframe.drop(['right','purpose','address'], axis=1)
        #если файл содержит цену сделки
        if 'dealPrice' in rosreestr_dataframe.columns:
            #убираем пустые значения по цене или площади
            rosreestr_dataframe= rosreestr_dataframe.dropna(subset=['area', 'dealPrice'])
            #убираем дубликаты
            rosreestr_dataframe= rosreestr_dataframe.drop_duplicates()           
            #добавляем столбец с исходным файлом
            rosreestr_dataframe['source_file']=file_to_read
#-------------------------------------------------------------------------------------------
            # обрабатываем датафрейм по результатам анализа ydata_profiling: (до обработки объем паркета составлял 229,9 MB)
            # удаляем столбец type тк он содержит только 1 уникальное значение и не несет информационной нагрузки
            rosreestr_dataframe.drop('type', axis=1, inplace=True)
            # удаляем столбец status: содержит два типа уникальных значений смысл неизвестен
            rosreestr_dataframe.drop('status', axis=1, inplace=True)
            #============================================rosreestr_dataframe.drop('code', axis=1, inplace=True)
            # устанавливаем для столбца "status" тип данных категория, так как он содержит только два значения
            rosreestr_dataframe['status'] = rosreestr_dataframe['status'].astype('category')
            rosreestr_dataframe['objKind'] = rosreestr_dataframe['objKind'].astype('category')
            #rosreestr_dataframe['text'] = rosreestr_dataframe['text'].astype('category')
            
            # исправляем ошибку, когда площадь указана как отрицательное число
            rosreestr_dataframe['area'] = rosreestr_dataframe['area'].apply(lambda x: abs(x) if x < 0 else x)
            # удаляем где площадь равна 0 
            rosreestr_dataframe.drop(index=[row for row in rosreestr_dataframe.index if 0 == rosreestr_dataframe.loc[row, 'area']], inplace=True)
            # удаляем где цена сделки равна 0
            rosreestr_dataframe.drop(index=[row for row in rosreestr_dataframe.index if 0 == rosreestr_dataframe.loc[row, 'dealPrice']], inplace=True)
            rightKind_wrong=['001002000000', '001003000000', '001099000000', '001005000000', '001004000000', '001008000000', '001009000000']
            rosreestr_dataframe = rosreestr_dataframe[~rosreestr_dataframe['rightKind'].isin(rightKind_wrong)]




#собрать полную статистику по столбцу dealCurrency и тогда принять решение, что делать. Пока что не удаляем
            # удаляем сделки которые != "купля-продажа"
            #print(f'{file_to_read} есть столбцы {rosreestr_dataframe.columns}')
#---------------------------------------------------------------------------------------------------------------
            #если итоговый датафрейм ещё не создан, то создаем его
            if cumulative_rosreestr_dataframe is None:
                #print(f'наращиваем датафрейм {datetime.now().strftime("%H:%M:%S")}')
                cumulative_rosreestr_dataframe=rosreestr_dataframe
            #иначе добавляем считанный файл к итоговому датафрейму
            else:
                # определяем номер региона из папки с данными (22-23 символы)
                folder_current=file_to_read[22:24]
                # если обработаны все файлы в папке, сохраняем датафрейм в файле
                if folder_current!=folder_count:
                    # сохранение в файл
                    cumulative_rosreestr_dataframe.to_parquet(r'c:/Pet_project/output/'+'part_'+folder_count+'.parquet')
                    # обнуление датафрейма
                    cumulative_rosreestr_dataframe=None
                    # обновление счетчика регионов
                    folder_count=folder_current
                # добавляем текущий датафрейм к нарастающему итогу
                cumulative_rosreestr_dataframe=pd.concat([cumulative_rosreestr_dataframe, rosreestr_dataframe], ignore_index=True)
                # показываем прогрессбар
                print (f'обработано файлов {round(file_counter/max_count_files*100,1)}')
# по окончании обработки всех файлов сохраняем последний датафрейм нарастающим итогом в файл.
cumulative_rosreestr_dataframe.to_parquet(r'c:/Pet_project/output/'+'part_'+folder_count+'.parquet')
print (f'END at {datetime.now().strftime("%H:%M:%S")}' )
