import psycopg2
from datetime import datetime
from datetime import date


class PostgreSQL:

    def __init__(self, _hostname, _username, _password, _database, _timeout=0):
        self.config = {
            "host": _hostname,
            "user": _username,
            "password": _password,
            "database": _database
        }
        self.conn = 0
        self.cursor = 0
        self.open()
        if _timeout > 0:
            self.command("SET statement_timeout=" + str(_timeout))

    def open(self):
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()

    def query(self, sql):
        if self.conn is None:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except (Exception, psycopg2.extensions.QueryCanceledError) as error:
            self.cursor.execute("ROLLBACK")
            self.commit()
            return "timeout"
        except (Exception, psycopg2.DatabaseError) as error:
            return error

    def command(self, sql):
        if self.conn is None:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(sql)
            self.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return True

    def close(self):
        self.cursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    @staticmethod
    def string_to_datetime(_string):
        return datetime.strptime(_string, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def datetime_to_string(_datetime):
        return _datetime.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def string_to_date(_string):
        return datetime.strptime(_string, "%Y-%m-%d").date()

    @staticmethod
    def date_to_string(_date):
        return _date.strftime("%Y-%m-%d")

