import duckdb
import numpy as np

def getReadFunction(filePath : str) :
    read_function = ''
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'
    elif filePath.lower().endswith('.csv') :
        read_function = 'read_csv'

    return read_function

def getRowCount(filePath : str, sql : str = '') :
    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = getReadFunction(filePath)

    sql_query = f"SELECT count(*) AS row_count \nFROM {read_function}('{filePath}')"
    if sql != '' :
        sql_query = f"SELECT count(*) as row_count FROM ({sql})"

    print('--------------------')
    print(sql_query)
    print('--------------------')

    return duckdb.sql(sql_query).df().replace({np.nan: 'NaN'}).to_dict('records')[0]

def getColumnsInfo(sql_query : str = '', filePath : str = '') :
    if filePath :
        read_function = getReadFunction(filePath)
        sql_query = f"DESCRIBE SELECT * FROM {read_function}('{filePath}')"
    else:
        sql_query = f"DESCRIBE {sql_query}"

    print('--------------------')
    print(sql_query)
    print('--------------------')

    return duckdb.sql(sql_query).df().replace({np.nan: None}).to_dict('records')

def agGridGetRows(filePath : str, startRow : int, endRow : int, fields : list, sql : dict ) :
    res = {}
    where_clause = ''
    select_clause = ''
    sql_query = ''

    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = getReadFunction(filePath)
    sqlSearchMode = False

    if sql is None or len(sql) < 1 :
        columns = '*'
        if fields is not None and len(fields) > 0:
            columns = ', '.join(fields)

        sql_query = f"SELECT {columns} \nFROM {read_function}('{filePath}')"
    else:
        sqlSearchMode = True
        select_clause = 'SELECT * '
        if 'select_clause' in sql and sql['select_clause']:
            select_clause = sql['select_clause']

        if 'where_clause' in sql and sql['where_clause']:
            where_clause = '\n' + sql['where_clause']
        sql_query = f"{select_clause} \nFROM {read_function}('{filePath}') {where_clause}"

    print('--------------------')
    print(sql_query)
    print('--------------------')

    r = duckdb.sql(sql_query + f" \nLIMIT {endRow-startRow} OFFSET {startRow}").df().replace({np.nan: 'NaN'}).to_dict('records')

    if startRow < 1 :
        res['lastRow'] = getRowCount(filePath, sql_query if sqlSearchMode else '')['row_count']

        cols = []
        if sql and 'select_clause' in sql and sql['select_clause']:
            res['filteredColumnInfo'] = getColumnsInfo(sql_query=sql_query)

        res['columnsInfo'] = getColumnsInfo(filePath=filePath)

    res['dataRow'] = r
    res['success'] = True

    return res

if __name__ == '__main__':
    filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/total_bank_data.csv'
    # filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'
    # filePath = 'E:/minikube_mnt/mlstudio/interface/sample/kaggle/bank.csv'
    # getColumnsInfo(filePath)
    # print(getColumnsInfo(filePath))
    # agGridGetRows(filePath, 500000, 500002)
    sql = f'''
    DESCRIBE SELECT *
    FROM read_csv('{filePath}')
    '''
    r = duckdb.sql(sql).df() #.replace({np.nan: 'NaN'}).to_dict('records')
    print(r)
