import mysql.connector
from typing import List, Tuple


DB_NAME = 'states'
TABLE_NAME = 'states'


class StateSQL:
    def __init__(self, sql_host, sql_user, sql_password):
        self.db = mysql.connector.connect(
            host=sql_host,
            user=sql_user,
            password=sql_password,
            # database='states'
        )

        self.cursor = self.db.cursor()

    def close(self):
        self.db.close()

    def init_db(self):
        '''
        init db and table if not exists
        '''
        # create (states) database if not exists
        sql = f'CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET = "utf8mb4";'
        self.cursor.execute(sql)

        # create (states) table if not exists
        sql = 'USE states;'
        self.cursor.execute(sql)
        sql = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (timestamp BIGINT PRIMARY KEY, state INT DEFAULT 0);'
        self.cursor.execute(sql)

    def update_state(self, timestamp: int, state: int):
        '''
        table schema: [timestamp, state]
        '''
        sql = f'INSERT INTO {DB_NAME} (timestamp, state) VALUES ({timestamp}, {state})'
        self.cursor.execute(sql)
        self.db.commit()

    def update_states(self, items: List[Tuple[int, int]]):
        '''
        table schema: [timestamp, state]
        items is a list of tuples (timestamp, state)
        '''
        # using UPSERT
        sql = f'INSERT INTO {DB_NAME} (timestamp, state) VALUES (%s, %s)' \
              f' ON DUPLICATE KEY UPDATE `state` = VALUES(`state`)'

        self.cursor.executemany(sql, items)
        self.db.commit()

    def get_state(self, timestamp: int) -> int:
        '''
        get the state of a single timestamp
        return state
        '''
        sql = f'SELECT state FROM {TABLE_NAME} WHERE timestamp = {timestamp}'
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return res[0][0]

    def get_states(self, start: int, end: int) -> List[int]:
        '''
        get the states for a time range
        return a list of states
        '''
        sql = f'SELECT state FROM {TABLE_NAME} WHERE timestamp >= {start} and timestamp <= {end}'
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return [state[0] for state in res]


if __name__ == "__main__":
    import time
    state_sql = StateSQL(sql_host='localhost',
                         sql_user='mysql',
                         sql_password='mysql')
    state_sql.init_db()
    # state_sql.update_state(1,2)
    # state_sql.update_states([(4,4), (5,5), (6,6)])
    # print(state_sql.get_state(timestamp=5))
    # print(state_sql.get_states(start=1, end=4))
    time.sleep(1)
    state_sql.close()
