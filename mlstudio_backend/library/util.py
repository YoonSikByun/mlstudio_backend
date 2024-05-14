from collections import defaultdict

import datetime
import os

def get_timestamp(mode='now'):
    """
    현재 시점으로 타임스탬프 정보 가져오기

    :return: YYYYMMDDHHMMSSμμμμμμ (마이크로 초 까지 출력)
    """
    if mode == 'now':
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    elif mode == 'micro':
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    elif mode == 'min':
        return datetime.datetime.min.strftime("%Y%m%d%H%M%S%f")
    elif mode == 'max':
        return datetime.datetime.max.strftime("%Y%m%d%H%M%S%f")
    else:
        raise KeyError('Invalid param.')

from flask import jsonify

class Response:
    def __init__(self, status=9999, message='Not known error', data=None, ret=None, option=''):

        if ret:
            if option and option == 'web':
                self.status = 0 if ret['status'] else 400
            else:
                self.status = 200 if ret['status'] else 400
            self.message = ret['reason']
            self.data = ret['data'] if 'data' in ret else None
        else:
            self.status = status
            self.message = message
            self.data = data

        self.option = option

    def get_JSON(self):
        if self.status == 0:
            custom_status = 200
        else:
            custom_status = 500

        if self.option != 'web':
            return {'message': self.data or self.message}, custom_status

        if not self.data:
            return {'status': self.status, 'statusText': self.message, 'data': {}}, custom_status

        if type(self.data) == str:
            self.data = {self.data}
        return {'status': self.status, 'statusText': self.message, 'data': self.data}, custom_status

    def get(self):
        if self.status == 0:
            custom_status = 200
        else:
            custom_status = 500

        if self.option != 'web':
            return jsonify({'message': self.data or self.message}), custom_status

        if not self.data:
            return jsonify({'status': self.status, 'statusText': self.message, 'data': {}}), custom_status

        if type(self.data) == str:
            self.data = {self.data}
        return jsonify({'status': self.status, 'statusText': self.message, 'data': self.data}), custom_status

    @staticmethod
    def error_JSON(msg, status=9999, data=None):
        if not data:
            return {'status': status, 'statusText': msg}
        return {'status': status, 'statusText': msg, 'data': data}

    @staticmethod
    def error(msg, status=9999, data=None):
        if not data:
            return jsonify({'status': status, 'statusText': msg})
        return jsonify({'status': status, 'statusText': msg, 'data': data})


class SingletonType(type):
    def __call__(cls, *args, **kwargs):
        """
        싱글톤 타입 클래스 구현

        :param args:
        :param kwargs:
        :return: 싱글톤 타입 인스턴스
        """
        try:
            return cls.__instance
        except AttributeError:
            cls.__instance = super(SingletonType, cls).__call__(*args, **kwargs)
            return cls.__instance
