from flask import Flask, request, jsonify
import traceback
from library.util import Response, get_timestamp
from mlstudio_backend.service import grid_data, json_data
from service import chart_data
import os

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
    elif option == 'text_data':
        ret = get_text_data(request)
    elif option == 'test_compress':
        ret = test_compress(request)
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

        data = grid_data.agGridGetRows(filePath=filePath, startRow=startRow, endRow=endRow, fields=fields, sql=sql)
        rtn = {'status': True, 'reason': 'success', 'data' : data}
    except BaseException:
        msg = traceback.format_exc()
        rtn = {'status': False, 'reason': msg}

    return rtn

def get_chart_data(request):
    try:
        data = request.get_json(force=True)
        filePath = data['filePath'] if 'filePath' in data else ''
  
        data = chart_data.getChartData(filePath=filePath, queryOption=data['queryOption'])
        rtn = {'status': True, 'reason': 'success', 'data' : data}
    except BaseException:
        msg = traceback.format_exc()
        rtn = {'status': False, 'reason': msg}

    return rtn

def get_text_data(request):
    try:
        data = request.get_json(force=True)
        filePath = data['filePath'] if 'filePath' in data else ''

        fileName = os.path.basename(filePath)
        fileName, ext = os.path.splitext(fileName)
        ext = ext.lower()

        if ext == '.json' or ext == '.meta' :  
            data = json_data.readJSONFile(file_path=filePath)
        else:
            raise Exception('This file is in an unsupported format.')
    
        rtn = {'status': True, 'reason': 'success', 'data' : data}

    except BaseException:
        msg = traceback.format_exc()
        rtn = {'status': False, 'reason': msg}

    return rtn

def test_compress(request):
    import gzip
    try:
        data = request.get_json(force=True)
        test_dict = {
            'name' : 'test_name',
            'value' : 123123432453546,
            'key1' : {
                'sub_key' : 'test_subkey',
                'list_data' : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            }
        }

        # content = gzip.compress(json.dumps(test_dict).encode('utf8'), 5)
        text='waeklsadlkdfjasldkfjwaoiejroiawjefoiawejfoiawejfoiwjefoiwjaeiofq2093rujqi234rq3940f53894nq8u4jrcnqi4jrq398u3q89nv4trqe8mc93q8u4rmj9q38rfcq3n98jmrqd98ncqdrjhmq39fuqdqmn3urnm3urqm3-urnq34uvrqjc3u0r9qdkur0q9j09w4urnvq34utqfkd093qiurj9uawrnvcq0cuj3w-kdtk3w40futoe94ut9jqc3ft98qj3wf94t9w8fty954g'
        content = gzip.compress(bytes(text, 'utf-8'), 5)
        print(content)
        content = gzip.decompress(content)
        print(content.decode('utf-8'))    
        rtn = {'status': True, 'reason': 'success', 'data' : content}

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
