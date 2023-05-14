from configs.settings import settings
import pandas as pd
import configs.connections as connections
from psycopg2.extras import execute_values

def _get_conn():
    settings.psql_conn = ""
    connections.create_psql_connection()
    conn = settings.psql_conn
    cur = conn.cursor()
    return cur,conn

def run_select_query(sql):
    cur, conn = _get_conn()
    try:
        cur.execute(sql)
        result = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(result, columns=cols)
    except Exception as e:
        settings.logger.error(f"Exception {e} in running select query!")
    conn.close()
    settings.logger.info("PSQL Connection closed!")
    return df

def check_exists(db,tbl_name):
    sql = f"select to_regclass('{db}.{tbl_name}') as \"exists\";"
    df = run_select_query(sql)
    df.fillna('')
    return df

def table_ddl(sql):
    cur, conn = _get_conn()
    try:
        cur.execute(sql)
    except Exception as e:
        settings.logger.error(f"Exception {e} in running DDL query!")
    conn.commit()
    conn.close()
    settings.logger.info("PSQL Connection closed!")

def fast_insert(db,tbl_name,df):
    cur, conn = _get_conn()
    field_tuple = tuple(df.columns.to_list())
    ins_sql = f"INSERT INTO {db}.{tbl_name} {field_tuple} VALUES %s".replace("'","")
    df_lists = list(df.values)
    if len(df_lists):
        df_tuples = [tuple(df_list) for df_list in df_lists]
        try:
            execute_values(cur,ins_sql,df_tuples)
            conn.commit()
            conn.close()
            settings.logger.info("PSQL Connection closed!")
        except Exception as e:
            conn.close()
            settings.logger.info("PSQL Connection closed!")
            settings.logger.error(f"Exception {e} in executing fast inserts!")
    else:
        settings.logger.warning(f"No data to be inserted to {db}.{tbl_name}!")