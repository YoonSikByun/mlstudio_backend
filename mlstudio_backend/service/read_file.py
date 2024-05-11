import duckdb
import numpy as np

def getRowCount(filepath : str) :
    return duckdb.sql(f"SELECT count(*) AS row_count FROM read_csv('{filepath}')").df().replace({np.nan: 'NaN'}).to_dict('records')[0]

def getColumnsInfo(filepath : str) :
    return duckdb.sql(f"DESCRIBE SELECT * FROM read_csv('{filepath}') LIMIT 1").df().replace({np.nan: 'NaN'}).to_dict('records')

def agGridGetRows(filepath : str, startRow : int, endRow : int) :
    res = {}

    if (startRow < 1) :
        res['lastRow'] = getRowCount(filepath)['row_count']
        res['columnInfo'] = getColumnsInfo(filepath)

    r = duckdb.sql(f"SELECT * FROM read_csv('{filepath}') LIMIT {endRow-startRow} OFFSET {startRow}").df().replace({np.nan: 'NaN'}).to_dict('records')
    res['dataRow'] = r
    res['success'] = True

    print(res)
    return res


if __name__ == '__main__':
    filepath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    # filepath = '/Users/yoonsikbyun/Documents/bank.csv'
    # getColumnsInfo(filepath, 500000, 500002)
    # print(getColumnsInfo(filepath))
    agGridGetRows(filepath, 500000, 500002)
