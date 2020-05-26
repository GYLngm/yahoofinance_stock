from sqlalchemy.ext.declarative import declarative_base

Trial_mode = True

dbConfig = {
    'host': '127.0.0.1',
    'user': 'mgnl',
    'password': 'password',
    'database': ('py_sql_test' if Trial_mode else 'py_sql'),
    'port': 3306,
}


Base = declarative_base()