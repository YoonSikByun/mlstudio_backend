import duckdb
import numpy as np

def scatterChart(filePath : str, read_function : str, queryOption : dict) :

    xAxis = queryOption['xAxis']
    yAxis = queryOption['yAxis']
    yAxisValueType = queryOption['yAxisValueType']
    limitCount = queryOption['limitCount']

    sql_query = f"SELECT {xAxis}, {yAxis} \nFROM {read_function}('{filePath}') \nLIMIT {limitCount} OFFSET 0"
    
    return duckdb.sql(sql_query).df().replace({np.nan: 0}).values.tolist()

def getChartData(filePath : str, queryOption : dict) :
    res = {}

    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')


    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    chartType = queryOption['chartType']
    
    r = None
    min = 0
    max = 0
    if chartType == 'Scatter':
        r = scatterChart(filePath, read_function, queryOption)
        
        if len(r) > 0:
            min = max = r[0][1]
            for i in r:
                if min > i[1] : min = i[1]
                if max < i[1] : max = i[1]
    else:
        raise TypeError(f'[{chartType}] This chart type is not supported.')

    res['chartData'] = r
    res['min'] = min
    res['max'] = max
    res['success'] = True

    return res


if __name__ == '__main__':
    import json
    # filePath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'

    queryOption = {
    'chartType' : 'Scatter',
    'xAxis' : 'age',
    'yAxis' : 'balance',
    'yAxisValueType' : '',
    'limitCount' : 5000}

    r = getChartData(filePath=filePath, queryOption=queryOption)
    print(json.dumps(r, indent=2))
