from utilities.db_connection import DB


class SQLLiteUtils:

    def __init__(self, conn_string):
        self.connection = DB.get_db(conn_string=conn_string)

    def put_df(self, df, table, conn_string):
        for i in range(0, 3):
            try:
                df.to_sql(name=table, con=self.connection, index=False, if_exists='replace')
                break
            except Exception as e:
                print("An exception has occurred as : {0}".format(e))
                self.connection = DB.reset_db_conn(conn_string=conn_string)
                continue
