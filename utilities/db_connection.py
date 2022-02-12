import os
import sys
import threading
from sqlalchemy import create_engine


class DbConnection:

    def __init__(self):
        self.threadLocal = threading.local()

    def get_db(self, conn_string):
        conn_pool = getattr(self.threadLocal, 'conn_pool', {})
        con = conn_string
        try:
            if con in conn_pool and conn_pool[con] is not None:
                print("Existing connection id is : {0}".format(id(conn_pool[con])))
                return conn_pool[con]
            else:
                r_conn = self.reset_db_conn(conn_string)
                return r_conn
        except Exception as exception:
            exc_obj = sys.exc_info()
            exc_type = exc_obj[0]
            exc_tb = exc_obj[2]
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_string = str(exc_type) + " & " + str(fname) + " & " + str(exc_tb.tb_lineno)
            print("An exception has occurred in the DB connection over-all as : {0}, at : {1}".format(exception, exception_string))

    def reset_db_conn(self, conn_string):
        conn = None
        con_key = conn_string
        conn_pool = getattr(self.threadLocal, 'conn_pool', {})
        try:
            if con_key in conn_pool and conn_pool[con_key] is not None:
                conn_pool[con_key].dispose()
        except Exception as e:
            exc_obj = sys.exc_info()
            exc_type = exc_obj[0]
            exc_tb = exc_obj[2]
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            exception_string = str(exc_type) + " & " + str(fname) + " & " + str(exc_tb.tb_lineno)
            print("An exception has occurred in the DB connection as : {0}, at : {1}".format(e, exception_string))

        for i in range(3):
            try:
                conn = create_engine(conn_string)
                break
            except Exception as e:
                exc_obj = sys.exc_info()
                exc_type = exc_obj[0]
                exc_tb = exc_obj[2]
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                exception_string = str(exc_type) + " & " + str(fname) + " & " + str(exc_tb.tb_lineno)
                print("An exception has occurred in the DB connection as : {0}, at : {1}".format(e, exception_string))
                conn = None
                continue
        if conn is not None:
            print("Created a connection with id : {0}".format(id(conn)))
            conn_pool[con_key] = conn
            self.threadLocal.conn_pool = conn_pool
        return conn


DB = DbConnection()
