import mysql.connector


class StateSQL:
    def __init__(self, sql_host, sql_user, sql_password):
        self.cnx = mysql.connector.connect(
            host=sql_host,
            user=sql_user,
            password=sql_password,
            # database='states'
        )

    def close(self):
        self.cnx.close()

    def init_db(self):
        '''
        init db and table if not exists
        '''
        db_name = 'states'
        table_name = 'states'

        # create (states) database if not exists
        cursor = self.cnx.cursor()
        sql = f'CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET = "utf8mb4";'
        cursor.execute(sql)

        # create (states) table if not exists
        sql = 'USE states;'
        cursor.execute(sql)
        sql = f'CREATE TABLE IF NOT EXISTS {table_name} (timestamp INT PRIMARY KEY, state INT DEFAULT 0);'
        cursor.execute(sql)

    def update_state(self, timestamp: int, state: int):
        '''
        table schema: [timestamp, state]
        '''
        # lilo:TODO
        pass

    def get_state(self, timestamp: int):
        '''
        get the state of a single timestamp
        return state
        '''
        # lilo:TODO
        pass

    def get_states(self, start: int, end: int):
        '''
        get the states for a time range
        return a list of pairs (timestamp, state)
        '''
        # lilo:TODO
        pass


if __name__ == "__main__":
    import time
    state_sql = StateSQL(sql_host='localhost',
                         sql_user='mysql',
                         sql_password='mysql')
    state_sql.init_db()
    time.sleep(1)
    state_sql.close()
