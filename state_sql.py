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


    def init(self):
        '''
        init db and tables if not exists
        '''
        # lilo:TODO
        cursor = self.cnx.cursor()
        sql = 'CREATE DATABASE IF NOT EXISTS states DEFAULT CHARACTER SET = "utf8mb4";'
        cursor.execute(sql)


    def update_state(self, timestamp: int, state: int):
        '''
        table schema: [timestamp, state]
        '''
        # lilo:TODO
        pass


if __name__ == "__main__":
    import time
    state_sql = StateSQL(sql_host='localhost',
                         sql_user='mysql',
                         sql_password='mysql')
    state_sql.init()
    time.sleep(1)
    state_sql.close()
