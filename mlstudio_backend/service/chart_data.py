import duckdb
import numpy as np

def queryChartDdata(filePath : str, read_function : str, queryOption : dict) :

    xAxis = queryOption['xAxis']
    yAxis = queryOption['yAxis']
    yAxisValueType = queryOption['yAxisValueType']
    limitCount = queryOption['limitCount']

    if yAxisValueType : yAxisValueType = yAxisValueType.lower()

    fucntionField = ''
    if yAxisValueType == 'sum': 
        fucntionField = f'SUM({yAxis})'
    elif yAxisValueType == 'average':
        fucntionField = f'AVG({yAxis})'
    elif yAxisValueType == 'count':
        fucntionField = f'COUNT({yAxis})'
    elif yAxisValueType == 'uniquecount':
        fucntionField = f'COUNT(DISTINCT{yAxis})'
    elif yAxisValueType == 'min':
        fucntionField = f'MIN({yAxis})'
    elif yAxisValueType == 'max':
        fucntionField = f'MAX({yAxis})'

    if fucntionField:
        sql_query = f"SELECT {xAxis}, {fucntionField} \nFROM {read_function}('{filePath}') \nGROUP BY {xAxis} \nLIMIT {limitCount} OFFSET 0"
    else:
        sql_query = f"SELECT {xAxis}, {yAxis} \nFROM {read_function}('{filePath}') \nLIMIT {limitCount} OFFSET 0"
    
    print('--------------------------')
    print(sql_query)
    print('--------------------------')
    
    return duckdb.sql(sql_query).df().replace({np.nan: 0}).values.tolist()

def getChartData(filePath : str, queryOption : dict) :
    res = {}

    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')


    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    # chartType = queryOption['chartType']
    
    r = None
    min = 0
    max = 0

    r = queryChartDdata(filePath, read_function, queryOption)
        
    if len(r) > 0:
        min = max = r[0][1]
        for i in r:
            if min > i[1] : min = i[1]
            if max < i[1] : max = i[1]

    res['chartData'] = r
    res['min'] = min
    res['max'] = max
    res['success'] = True

    return res


if __name__ == '__main__':
    import json
    # filePath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    # filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'
    filePath = 'E:/minikube_mnt/mlstudio/interface/sample/kaggle/bank.csv'
    # queryOption = {
    # 'chartType' : 'Scatter',
    # 'xAxis' : 'age',
    # 'yAxis' : 'balance',
    # 'yAxisValueType' : '',
    # 'limitCount' : 5000}

    # r = getChartData(filePath=filePath, queryOption=queryOption)
    # print(json.dumps(r, indent=2))

    # sum
    # sql = f"SELECT age, sum(balance) FROM read_csv('{filePath}') group by age LIMIT 10"

    # avg
    # sql = f"SELECT age, avg(balance) FROM read_csv('{filePath}') group by age LIMIT 10"

    # count
    # sql = f"SELECT age, count(balance) FROM read_csv('{filePath}') group by age LIMIT 10"

    # min
    # sql = f"SELECT age, min(balance) FROM read_csv('{filePath}') group by age LIMIT 10"

    # unique count
    sql = f"SELECT age, count(distinct balance) FROM read_csv('{filePath}') group by age LIMIT 10"

    r = duckdb.sql(sql).df().to_dict('records')
    print(r)