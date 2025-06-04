import pandas as pd
import psycopg2

# Данные для подключения к БД
database = {'host': 'localhost',
            'port': '5432',
            'database': 'mydatabase',
            'user': 'myuser',
            'password': 'mypassword'
            }

sql_query = """
CREATE table IF NOT EXISTS  companies
(
    ОГРН char(13),
    ИНН char(10),
    НазваниеКомпании varchar,
    Телефон varchar,
    ДатаОбн date
)
"""

# Чтение файла, создание датафрейма на его основе
df = pd.read_xml('./data/companies.xml')


# Приведения pd.Series к нужным типам (по дефолту у всех тип object)
df['ОГРН'] = df['ОГРН'].astype('string')
df['ИНН'] = df['ИНН'].astype('string')
df['НазваниеКомпании'] = df['НазваниеКомпании'].astype('string')
df['Телефон'] = df['Телефон'].astype('string')
df['ДатаОбн'] = pd.to_datetime(df['ДатаОбн'], errors='coerce')


print("ИНН: кол-во цифр != 10")
print(df[df["ИНН"].str.len() != 10])
print("\n")
print("ОГРН: кол-во цифр != 13")
print(df[df["ОГРН"].str.len() != 13])
print("\n")
print('Кол-во строк с null в дате', len(df[df["ДатаОбн"].isnull()]))


# Удаление несоответствующих ИНН
df = df[df["ИНН"].str.len() == 10]

# Удаление несоответствующих ОГРН
df = df[df["ОГРН"].str.len() == 13]

# Удаление строк с null в дате (по факту, ничего не удаляет, так как таких строк нет)
df = df.dropna(subset=["ДатаОбн"])

# Группирую данные по ОГРН и считаю минимальную дату (самую раннюю), затем объединяю это с изначальной таблицой по двум полям (чтобы избежать дублирующих ОГРН)
df = df.merge(
    df.groupby("ОГРН").agg({'ДатаОбн': 'max'}),
    how='left',
    on=["ОГРН", "ДатаОбн"],
    indicator="is_max_date")

print('Строки-дубликаты ОГРН')
print(df[df["is_max_date"] == "left_only"])

# Удаляю всмпомогательную колонку и очищаю датафрем от дубликатов
df = df[df["is_max_date"] == "both"]
df = df.drop('is_max_date', axis=1)

# Создаю соединение к PG (хост и порт - дефолтные)
conn = psycopg2.connect(database=database['database'],
                        user=database['user'],
                        password=database['password'])

# Создаю курсор
cursor = conn.cursor()

# Выполняю запрос на создания таблицы и вставке в нее данных
cursor.execute(sql_query)
for _, row in df.iterrows():
    cursor.execute("INSERT INTO companies (ОГРН, ИНН, НазваниеКомпании, Телефон, ДатаОбн) "
    "VALUES (%s, %s, %s, %s, %s)",
    (row["ОГРН"], row["ИНН"], row["НазваниеКомпании"], row["Телефон"], row["ДатаОбн"]))

# Коммичу изменения
conn.commit()

# Закрываю курсор и соединение
cursor.close()
conn.close()
