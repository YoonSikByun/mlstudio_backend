import duckdb
import numpy as np

def getRowCount(filePath : str, sql : str = '') :
    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    sql_query = f"SELECT count(*) AS row_count \nFROM {read_function}('{filePath}')"
    if sql != '' :
        sql_query = f"SELECT count(*) as row_count FROM ({sql})"

    return duckdb.sql(sql_query).df().replace({np.nan: 'NaN'}).to_dict('records')[0]

def getColumnsInfo(filePath : str, sql : str = '') :
    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'
    
    sql_query = f"DESCRIBE SELECT * FROM {read_function}('{filePath}') LIMIT 1"

    return duckdb.sql(sql_query).df().replace({np.nan: 'NaN'}).to_dict('records')

def agGridGetRows(filePath : str, startRow : int, endRow : int, fields : list, sql : dict ) :
    res = {}
    where_clause = ''
    select_clause = ''
    sql_query = ''
    sql_count_query = ''

    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    sqlSearchMode = False

    if sql is None or len(sql) < 1 :
        columns = '*'
        if fields is not None and len(fields) > 0:
            columns = ', '.join(fields)

        sql_query = f"SELECT {columns} \nFROM {read_function}('{filePath}') \nLIMIT {endRow-startRow} OFFSET {startRow}"
    else:
        sqlSearchMode = True
        select_clause = '*'
        if 'select_clause' in sql and sql['select_clause']:
            select_clause = sql['select_clause']

        if 'where_clause' in sql and sql['where_clause']:
            where_clause = '\nWHERE ' + sql['where_clause']
        sql_query = f"SELECT {select_clause} \nFROM {read_function}('{filePath}') {where_clause} \nLIMIT {endRow-startRow} OFFSET {startRow}"
        sql_count_query = f"SELECT {select_clause} \nFROM {read_function}('{filePath}') {where_clause}"

    r = duckdb.sql(sql_query).df().replace({np.nan: 'NaN'}).to_dict('records')

    if startRow < 1 :
        res['lastRow'] = getRowCount(filePath, sql_count_query if sqlSearchMode else '')['row_count']
        print(f'res[lastRow] : {res["lastRow"]}')

        cols = []
        if select_clause != '' and select_clause != '*' :
            for k, v in r[0].items() :
                cols.append({'column_name' : k, 'column_type' : 'None', 'null' : 'None', 'key' : 'None', 'default' : 'None', 'extra' : 'None'})
            res['filteredColumnInfo'] = cols

        res['columnsInfo'] = getColumnsInfo(filePath)

    res['dataRow'] = r
    res['success'] = True

    return res


if __name__ == '__main__':
    # filePath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'
    getColumnsInfo(filePath)
    print(getColumnsInfo(filePath))
    # agGridGetRows(filePath, 500000, 500002)
