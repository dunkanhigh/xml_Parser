# xml_Parser

# Создание окружения и установка библиотек (Python 3.6+)
```bash
python -m venv venv
source venv/Scripts/activate
pip install psycopg2 lxml pandas
```

# Запуск Docker-контейнера с Postgresql
```bash
docker compose up -d
```

# Подключение к БД
```python
database = {'host': 'localhost',
            'port': '5432',
            'database': 'mydatabase',
            'user': 'myuser',
            'password': 'mypassword'
            }
```
