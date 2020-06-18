import sys
from config import DB_DETAILS
from read import read_table
from util import get_tables, load_db_details
from write import build_insert, load_table


def main():
    env=sys.argv[1]
    db_details=load_db_details(env)
    print(db_details)
    tables=get_tables('table_list')
    db_details=DB_DETAILS[env]
    for table in tables['table_name']:
        print(f'Reading data from {table}')
        data,columns=read_table(db_details,table)
        print(data)
        print(f'Loading data into {table}')
        load_table(db_details,table,columns,data)

if __name__=='__main__':
    main()



