import pymysql.cursors

from utils.common import read_config


class DbManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DbManager, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.conn = None
        self.config = read_config().get('mysql')

    def __connect(self):
        return pymysql.connect(host=self.config.get('host'),
                               user=self.config.get('user'),
                               password=self.config.get('password'),
                               db=self.config.get('db'),
                               port=self.config.get('port'),
                               charset=self.config.get('charset'))

    def __execute(self, query):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute(query)
            result = cur.fetchall()
        except Exception as e:
            msg = f'[Error in execute query]\n{e}'
            msg += f'\n\nQuery : {query}'

            cur.close()
            self.conn.close()
            return None

        cur.close()
        self.conn.close()
        return result

    def __execute_values(self, query, values):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.executemany(query, values)
        except Exception as e:
            msg = f'[Error in execute_values query]\n{e}'
            msg += f'\n\nQuery : {query}'

            cur.close()
            self.conn.rollback()
            self.conn.close()
            return False

        cur.close()
        self.conn.commit()
        self.conn.close()
        return True

    def __execute_commit(self, query):
        self.conn = self.__connect()
        cur = self.conn.cursor(pymysql.cursors.DictCursor)
        try:
            cur.execute(query)
        except Exception as e:
            msg = f'[Error in execute_commit query]\n{e}'
            msg += f'\n\nQuery : {query}'

            cur.close()
            self.conn.rollback()
            self.conn.close()
            return False

        cur.close()
        self.conn.commit()
        self.conn.close()
        return True

    def insert_row(self, table, params):
        query = f'INSERT INTO {table} ({", ".join(params.keys())}) VALUES ({", ".join(["%s"] * len(params.values()))})'
        return self.__execute_values(query, [tuple(params.values())])

    def insert_bulk_row(self, table, params):
        query = f'INSERT INTO {table} ({", ".join(params[0].keys())}) VALUES ({", ".join(["%s"] * len(params[0]))})'
        return self.__execute_values(query, [tuple(p.values()) for p in params])

    def get_parsed_info(self, parsed_date):
        query = "SELECT a.* " \
                "FROM dtnn.ark AS a " \
                "WHERE a.parsed_date = '{parsed_date}' "
        query = query.format(parsed_date=parsed_date)
        return self.__execute(query)
