from util import get_connection

def build_insert(table,columns):
    columns_str=', '.join(columns)
    column_values=tuple(map(lambda column:column.replace(column,'%s'),columns))
    column_values_str=', '.join(column_values)
    query = f'''insert into {table} ({columns_str}) VALUES ({column_values_str})'''
    query_trunc=f'''truncate table {table}'''
    return query,query_trunc

def insert_data(connection, cursor, query,query_trunc,data,batch_size=100):
    cursor.execute(query_trunc)
    recs=[]
    count=1
    for rec in data:
        recs.append(rec)
        if(count%batch_size==0):
            cursor.executemany(query,recs)
            connection.commit()
            rec=[]
        count=count+1
    cursor.executemany(query,recs)
    connection.commit()

def load_table(db_details,table,columns,data):
    TARGET_DB=db_details['TARGET_DB']
    connection=get_connection(db_type=TARGET_DB['DB_TYPE'],
                              db_host=TARGET_DB['DB_HOST'],
                              db_name=TARGET_DB['DB_NAME'],
                              db_user=TARGET_DB['DB_USER'],
                              db_pass=TARGET_DB['DB_PASS']
                              )
    cursor=connection.cursor()
    query,query_trunc=build_insert(table,columns)
    insert_data(connection,cursor,query,query_trunc,data)
    connection.close()


