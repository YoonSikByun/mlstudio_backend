import duckdb
import numpy as np

def queryChartDdata(chartType : str, filePath : str, read_function : str, queryOption : dict) :

    xAxis = queryOption['xAxis']
    yAxis = queryOption['yAxis']
    yAxisValueType = queryOption['yAxisValueType']
    isXCatergorical = queryOption['isXCatergorical']
    limitCount = queryOption['limitCount']
    whereClause = queryOption['whereClause']

    print('--------------------------')
    print(queryOption)
    print('--------------------------')

    res = {}
    res['dataCount'] = 0
    res['chartType'] = chartType
    res['chartData'] = None
    res['success'] = True

    if yAxisValueType : yAxisValueType = yAxisValueType.lower()
    if chartType : chartType = chartType.lower()

    fucntionField = ''
    if yAxisValueType == 'sum': 
        fucntionField = f'SUM({yAxis})'
    elif yAxisValueType == 'average':
        fucntionField = f'AVG({yAxis})'
    elif yAxisValueType == 'count':
        fucntionField = f'COUNT({yAxis})'
    elif yAxisValueType == 'uniquecount':
        fucntionField = f'COUNT(DISTINCT {yAxis})'
    elif yAxisValueType == 'min':
        fucntionField = f'MIN({yAxis})'
    elif yAxisValueType == 'max':
        fucntionField = f'MAX({yAxis})'

    if whereClause:
        whereClause = '\n' + whereClause

    if fucntionField:
        sql_query = f"SELECT {xAxis}, {fucntionField} \nFROM {read_function}('{filePath}') {whereClause}\nGROUP BY {xAxis} \nLIMIT {limitCount} OFFSET 0"
    else:
        if yAxis:
            sql_query = f"SELECT {xAxis}, {yAxis} \nFROM {read_function}('{filePath}') {whereClause}\nLIMIT {limitCount} OFFSET 0"
        else:
            sql_query = f"SELECT {xAxis} \nFROM {read_function}('{filePath}') {whereClause}\nLIMIT {limitCount} OFFSET 0"
    
    print('--------------------------')
    print(sql_query)
    print('--------------------------')

    if yAxis:
        df = duckdb.sql(sql_query).df().replace({np.nan: None})
        # r = df.sort_values(by=df.columns[0]).values.tolist()
        r = df.values.tolist()
        # hearder = df.columns.values.tolist()
        # r.insert(0, hearder)
    else:
        r = duckdb.sql(sql_query).df().replace({np.nan: None}).stack().tolist()

    dataCount = len(r)
    res['dataCount'] = dataCount

    if dataCount < 1:
        return res

    if chartType == 'boxplot':
        category = {}
        for item in r:
            if item[0] not in category:
                category[item[0]] = []

            category[item[0]].append(item[1])

        res['chartData'] = category
    else:
        res['chartData'] = r

    if chartType == 'scatterplot':
        min = max = r[0][1]
        for i in r:
            if min > i[1] : min = i[1]
            if max < i[1] : max = i[1]

        res['min'] = min
        res['max'] = max

    return res

def getChartData(filePath : str, queryOption : dict) :
    if not filePath.lower().endswith(('.csv', '.parquet', '.parq')) :
        raise TypeError(f'[{filePath}] This file format is not supported.')

    read_function = 'read_csv'
    if filePath.lower().endswith(('.parquet', '.parq')) :
        read_function = 'read_parquet'

    chartType = queryOption['chartType']

    return queryChartDdata(chartType, filePath, read_function, queryOption)


if __name__ == '__main__':
    import json
    # filePath = '/Users/yoonsikbyun/Documents/total_bank_data.csv'
    filePath = '/Users/yoonsikbyun/Documents/minikube_mnt/mlstudio/interface/sample/bank.csv'
    # filePath = 'E:/minikube_mnt/mlstudio/interface/sample/kaggle/bank.csv'
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
    sql = f"SELECT job, balance FROM read_csv('{filePath}') LIMIT 10"

    df = duckdb.sql(sql).df()
    hearder = df.columns.values.tolist()
    l = df.sort_values(by=df.columns[0]).values.tolist()
    l.insert(0, hearder)
    print(l)
    
    # category = {}
    # category_list = []
    # index = 0
    # for row in r:
    #     if row[0] in category:
    #         continue
    #     category[row[0]] = index
    #     category_list.append(row[0])

    # category_list.sort()    
    # print(category_list)
        