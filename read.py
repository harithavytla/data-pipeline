from util import get_tables,load_db_details,get_connection


def read_table(db_details,table_name,limit=0):
    source_db=db_details['SOURCE_DB']
    connection=get_connection(db_type=source_db['DB_TYPE'],
                              db_host=source_db['DB_HOST'],
                              db_name=source_db['DB_NAME'],
                              db_user=source_db['DB_USER'],
                              db_pass=source_db['DB_PASS']
                            )
    cursor=connection.cursor()
    if limit==0:
        query=f'select * from {table_name}'
    else :
        query=f'select * from {table_name} LIMIT {limit}'
    cursor.execute(query)
    data=cursor.fetchall()
    columns=cursor.column_names
    connection.close()
    return data,columns

