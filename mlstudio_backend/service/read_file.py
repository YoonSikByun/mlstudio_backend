import duckdb
import numpy as np

def getRowCount(filepath : str, where : str = '') :
    if not filepath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filepath}] This file format is not supported.')

    read_function = 'read_csv'
    if filepath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    sql = f"SELECT count(*) AS row_count \nFROM {read_function}('{filepath}')"
    if where != '' :
        sql = sql + ' ' + where

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

    if not filepath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filepath}] This file format is not supported.')

    read_function = 'read_csv'
    if filepath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    if sql is None or len(sql) < 1 :
        columns = '*'
        if fields is not None and len(fields) > 0:
            columns = ', '.join(fields)

        sql = f"SELECT {columns} \nFROM {read_function}('{filepath}') \nLIMIT {endRow-startRow} OFFSET {startRow}"
    else:
        select_clause = '*'
        if 'select_clause' in sql:
            select_clause = sql['select_clause']

        if 'where_clause' in sql and sql['where_clause']:
            where_clause = '\nWHERE ' + sql['where_clause']
        sql = f"SELECT {select_clause} \nFROM {read_function}('{filepath}') {where_clause} \nLIMIT {endRow-startRow} OFFSET {startRow}"

    print('--------------------------')
    print(sql)
    print('--------------------------')

    r = duckdb.sql(sql).df().replace({np.nan: 'NaN'}).to_dict('records')

    if startRow < 1 :
        res['lastRow'] = getRowCount(filepath, where_clause)['row_count']
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
