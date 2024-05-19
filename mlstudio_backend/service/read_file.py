import duckdb
import numpy as np

def getRowCount(filepath : str, sql : str = '') :
    if not filepath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filepath}] This file format is not supported.')

    read_function = 'read_csv'
    if filepath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    sql = f"SELECT count(*) AS row_count \nFROM {read_function}('{filepath}')"
    if sql != '' :
        sql = f"SELECT count(*) as row_count FROM ({sql})"

    print('----------- count ---------------')
    print(sql)
    print('--------------------------')

    return duckdb.sql(sql).df().replace({np.nan: 'NaN'}).to_dict('records')[0]

def getColumnsInfo(filepath : str, sql : str = '') :
    if not filepath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filepath}] This file format is not supported.')

    read_function = 'read_csv'
    if filepath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'
    
    if sql == '' :
        sql = f"DESCRIBE SELECT * FROM {read_function}('{filepath}') LIMIT 1"

    return duckdb.sql(sql).df().replace({np.nan: 'NaN'}).to_dict('records')

def agGridGetRows(filepath : str, startRow : int, endRow : int, fields : list, sql : dict ) :
    res = {}
    where_clause = ''
    select_clause = ''
    sql_query = ''

    if not filepath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filepath}] This file format is not supported.')

    read_function = 'read_csv'
    if filepath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    sqlSearchMode = False
    if sql is None or len(sql) < 1 :
        columns = '*'
        if fields is not None and len(fields) > 0:
            columns = ', '.join(fields)

        sql_query = f"SELECT {columns} \nFROM {read_function}('{filepath}') \nLIMIT {endRow-startRow} OFFSET {startRow}"
    else:
        sqlSearchMode = True
        select_clause = '*'
        if 'select_clause' in sql:
            select_clause = sql['select_clause']

        if 'where_clause' in sql and sql['where_clause']:
            where_clause = '\nWHERE ' + sql['where_clause']
        sql_query = f"SELECT {select_clause} \nFROM {read_function}('{filepath}') {where_clause} \nLIMIT {endRow-startRow} OFFSET {startRow}"

    print('--------------------------')
    print(sql_query)
    print('--------------------------')

    r = duckdb.sql(sql_query).df().replace({np.nan: 'NaN'}).to_dict('records')

    if startRow < 1 :
        res['lastRow'] = getRowCount(filepath, sql_query if sqlSearchMode else '')['row_count']

        cols = []
        if select_clause != '' and select_clause != '*' :
            for k, v in r[0].items() :
                cols.append({'column_name' : k, 'null' : 'None', 'key' : 'None', 'default' : 'None', 'extra' : 'None'})
            res['columnInfo'] = cols
        else:
            res['columnInfo'] = getColumnsInfo(filepath)

    res['dataRow'] = r
    res['success'] = True

    return res


if __name__ == '__main__':
    # filepath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    filepath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'
    getColumnsInfo(filepath)
    print(getColumnsInfo(filepath))
    # agGridGetRows(filepath, 500000, 500002)
