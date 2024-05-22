from flask import Flask, request, jsonify
import traceback
from library.util import Response, get_timestamp
from service import read_file, chart_data

app = Flask(__name__)
app.config['DEBUG'] = False

@app.route('/', methods=['GET', 'POST'])
def heartbeat():
    timestamp = get_timestamp('micro')
    ret = {'message': timestamp}

    return jsonify(ret), 200

@app.route('/data/<option>', methods=['POST'])
def data(option):
    if option == 'ag_grid_dataview':
        ret = ag_grid_dataview(request)
    elif option == 'chart_data':
        ret = get_chart_data(request)
    else:
        ret = {'status': False, 'reason': f'"{option}" is unacceptable option.'}

    return Response(ret=ret, option='web').get()

def ag_grid_dataview(request):
    try:
        data = request.get_json(force=True)
        filePath = data['filePath'] if 'filePath' in data else ''
        startRow = data['startRow']
        endRow = data['endRow']
        fields = data['fields'] if 'fields' in data else []
        sql = data['sql'] if 'sql' in data else {}

        print(filePath)

        data = read_file.agGridGetRows(filePath=filePath, startRow=startRow, endRow=endRow, fields=fields, sql=sql)
        rtn = {'status': True, 'reason': 'success', 'data' : data}

    except BaseException:
        msg = traceback.format_exc()
        rtn = {'status': False, 'reason': msg}

    return rtn

def get_chart_data(request):
    try:
        data = request.get_json(force=True)
        filePath = data['filePath'] if 'filePath' in data else ''
        data['queryOption']
  
        data = chart_data.getChartData(filePath=filePath, queryOption=data['queryOption'])
        rtn = {'status': True, 'reason': 'success', 'data' : data}

    except BaseException:
        msg = traceback.format_exc()
        rtn = {'status': False, 'reason': msg}

    return rtn

def start(standalone_mode=False):

    global app

    if not standalone_mode:
        # 쿠버와 독립된 컨테이너로 동작할 때 고려할 부분
        pass

    return app

# Gunicorn 실행
# gunicorn -w 1 -b 0.0.0.0:8880 -t 300 'mlstudio_backend.app:start(standalone_mode=False)'

if __name__ == '__main__':
    app = start()
    app.run(host='0.0.0.0', port=8881, threaded=False, debug=True)
