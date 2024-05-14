import sys
import os
import importlib
import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from sampl2.common.metaclasses import SingletonType
import time
from logstash.formatter import LogstashFormatterBase
# without midnight: version 1.0
from logstash.handler_tcp import TCPLogstashHandler
from logstash import LogstashHandler

hostname = os.getenv('HOSTNAME')
g_bOnlineServingLogger = True if len(hostname) >= 6 and hostname[:6] == "online" else False

# SAMPL1.0에서 exception handler
try:
    sam1_logger = getattr(importlib.import_module('sampl.common.logger'), 'SAMPLLogger')
    logger1 = sam1_logger().get_logger()
    logger1.debug('[ Created SAMPL1 Logger ]')
except:
    sam1_logger = None
    logger1 = None

LOG_LEVEL = logging.DEBUG
LOG_FORMAT = '[%(asctime)s][%(levelname)s][%(filename)s][line:%(lineno)s][%(funcName)s][pid:%(process)s] %(message)s'
LOG_FILE_NAME = 'sampl.log'
LOG_MAX_BYTES = 100 * 1024 * 1024
LOG_BACKUP_COUNT = 5
LOG_ENCODING = 'utf-8'

import atexit


class ExitHooks(object):
    def __init__(self):
        self.exit_code = None
        self.exception = None

    def hook(self):
        self._orig_exit = sys.exit
        sys.exit = self.exit
        sys.excepthook = self.exc_handler

    def exit(self, code=0):
        self.exit_code = code
        self._orig_exit(code)

    def exc_handler(self, exc_type, exc, *args):
        self.exception = exc


hooks = ExitHooks()
hooks.hook()


def atexit_hadler():
    from sampl2.api.blueprint.inner import get_pname
    import os
    pid = os.getpid()
    pname = get_pname(pid)
    log_file_dir, log_file_name = get_log_path()
    lfp = os.path.join(log_file_dir, log_file_name)

    f = open(lfp, 'a')

    if hooks.exit_code:
        f.write("[------ [%s] terminated by sys.exit(%d) ------]\n" % pname, hooks.exit_code)
    elif hooks.exception:
        f.write("[------ [%s] terminated by exception: %s ------]\n" % pname, hooks.exception)
    else:
        f.write("[------ [%s] terminated successfully ------]\n" % pname)

    f.close()


atexit.register(atexit_hadler)


class LogstashFormatterEx(LogstashFormatterBase):
    def __init__(self, index=None, format=None, additional=None):
        super(LogstashFormatterEx, self).__init__()

        self.index = index
        self.additional = additional

        if not format:
            self.FORMAT = '[%(levelname)s][%(filename)s][line:%(lineno)s][%(funcName)s][pid:%(process)s] %(message)s'
        else:
            self.FORMAT = format

    def format(self, record):
        # msg = record.getMessage()
        msg = self.FORMAT % record.__dict__
        if not self.index:
            self.index = "sampl_ex1-{}".format(time.strftime('%Y.%m.%d', time.localtime(record.created)))

        # Create message dict
        message = {
            'index': self.index,
            '@version': '1',
            'message': msg,
            'host': self.host,
            'path': record.pathname,
            'tags': self.tags,
            'type': self.message_type,

            # Extra Fields
            'level': record.levelname,
            'logger_name': record.name,
        }

        # Add extra fields
        message.update(self.get_extra_fields(record))

        if self.additional:
            message.update(self.additional)

        # If exception, add debug info
        if record.exc_info:
            message.update(self.get_debug_fields(record))

        return self.serialize(message)


def get_log_path(log_file_name=''):

    global LOG_BACKUP_COUNT

    if 'SAMPL_HOME' in os.environ and os.path.exists(os.getenv('SAMPL_HOME')):
        if g_bOnlineServingLogger:
            log_root_dir = os.getenv('HOME')
            LOG_BACKUP_COUNT = 2
        else:
            log_root_dir = os.getenv('SAMPL_HOME')

        log_file_dir = os.path.join(log_root_dir, 'logs', 'sampl')
        if not os.path.exists(os.path.join(log_root_dir, 'logs')):
            os.mkdir(os.path.join(log_root_dir, 'logs'))

        if not os.path.exists(os.path.join(log_root_dir, 'logs', 'sampl')):
            os.mkdir(os.path.join(log_root_dir, 'logs', 'sampl'))
    else:
        log_file_dir = '.'

    if log_file_name:
        if 'HOSTNAME' in os.environ:
            log_file_name = "{}_{}.log".format(log_file_name, os.environ['HOSTNAME'])
        else:
            log_file_name = log_file_name
    else:
        if 'HOSTNAME' in os.environ:
            log_file_name = "{}.log".format(os.environ['HOSTNAME'])
        else:
            log_file_name = LOG_FILE_NAME

    return log_file_dir, log_file_name


class SAMPLLogger(object, metaclass=SingletonType):
    """
    SAMPL Logger 클래스

    """
    _logger = None

    def __init__(self, log_file_name=''):

        self.logger_name = "SAMPL2Logger"
        self._logger = logging.getLogger(self.logger_name)

        off = os.getenv('SAMPL_LOGGER_OFF')
        if off == 'Y':
            self.logger_off = True
            self.redirection = 'N'
            return
        else:
            self.logger_off = False
            self.redirection = 'Y'

        self.logstash_handler = None
        self.logstash_formatter = None
        self.exec_id = None
        self._stdout = None
        # self._stderr = None

        # 최소 로그 레벨 설정
        # CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
        self._logger.setLevel(LOG_LEVEL)

        # Formatter 설정
        # asctime:시간, name:로거이름, levelname:로깅레벨, message:메세지
        formatter = logging.Formatter(LOG_FORMAT)

        # Stream Handler 추가
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        self._logger.addHandler(stream_handler)

        # Rotating File Handler 추가
        # 파일당 maxBytes 사이즈만큼 최대 backupCount 개수만큼 생성

        # ---------------------------------------
        # Terminal 없이 Background 실행중 Error5 I/O Error 방지 위해 Redirection
        self.log_file_dir, self.log_file_name = get_log_path(log_file_name)

        lfp = os.path.join(self.log_file_dir, self.log_file_name)
        self.f = open(lfp, 'a')

        self._stdout = sys.stdout
        # self._stderr = sys.stderr
        sys.stdout = self.f
        # sys.stderr = self.f
        # ---------------------------------------

        rotating_file_handler = RotatingFileHandler(
            filename=os.path.join(self.log_file_dir, self.log_file_name),
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding=LOG_ENCODING)

        rotating_file_handler.setFormatter(formatter)

        self._logger.addHandler(rotating_file_handler)

        def exception_handler(exctype, value, tb):
            import traceback
            # self._logger.error("Uncaught exception", exc_info=(exctype, value, tb))
            exception_msg = str(traceback.extract_tb(tb))
            self._logger.error('\n#### Uncaught exception ####\n{}'.format(exception_msg.replace(',', '\n')))
            self._logger.error("[{}], [{}], [{}]".format(exctype, value, tb))

            if self.exec_id:
                execLogger = getattr(importlib.import_module('sampl2_exe.framework.exec_logger'), 'ExecLogger')
                execLogger().record_status(self.exec_id, 'Error')

        sys.excepthook = exception_handler

        self.add_logstash_handler()

        # 싱글톤 타입 로거 최초 시작
        self._logger.debug("SAMPLLogger initialize.")

    def set_redirection(self, redirection):
        if self.redirection == redirection:
            return

        if redirection == 'N':
            self.close_redirection()

        self.redirection = redirection

    def close_redirection(self):
        if self.redirection == 'N':
            return

        self.redirection = 'N'
        sys.stdout = self._stdout
        # sys.stderr = self._stderr
        self.f.close()
        self.f = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._logger.infor("processor terminated...")
        self.close_redirection()

    def get_logger_name(self):
        return self.logger_name

    def get_logger(self):
        """
        싱글톤 타입 SAMPL Logger 반환

        :return: 싱글톤 타입 SAMPL Logger
        """
        return self._logger

    def add_logstash_handler(self):
        if not os.getenv('SAMPL_HOME'): return

        try:
            logstash = importlib.import_module('logstash')
            Config = getattr(importlib.import_module('sampl2.common.config'), 'Config')
            config = Config().get_config()
            # ip = config['ELK']['LOGSTASH_IP']
            # port = int(config['ELK']['LOGSTASH_PORT'])
            ip = config['ELK']['LOGSTASH_IP_UDP']
            port = int(config['ELK']['LOGSTASH_PORT_UDP'])
            if not ip or not port:
                self._logger.info('------ Logstash is net available ------')
                return

            self._logger.debug('Using LogStash handler(host: [{}], port: [{}])'.format(ip, port))
            # TCP
            # self.logstash_handler = logstash.TCPLogstashHandler(ip, port, version=1)
            # UDP
            self.logstash_handler = logstash.LogstashHandler(ip, port, version=1)

            self.logstash_formatter = LogstashFormatterEx()
            self.logstash_handler.setFormatter(self.logstash_formatter)
            self._logger.addHandler(self.logstash_handler)

        except BaseException as e:
            self._logger.warning('Not available package: {}'.format(e))
        pass

    def set_logstash_index(self, index):
        self.logstash_formatter.index = index
        self.logstash_handler.setFormatter(self.logstash_formatter)

    def set_logstash_format(self, format):
        self.logstash_formatter.format = format
        self.logstash_handler.setFormatter(self.logstash_formatter)

    def set_logstash_addtional(self, additional):
        self.logstash_formatter.additional = additional
        self.logstash_handler.setFormatter(self.logstash_formatter)


if __name__ == '__main__':
    os.environ['SAMPL_CONF_PATH'] = 'C:\\Project\\sampl2.0\\sampl2_common\\test'
    os.environ['SAMPL_HOME'] = 'C:\\Project\\sampl2.0\\sampl2_common\\test'
    logger = SAMPLLogger().get_logger()
    logger.info('philip-111111111111111')
    additional = {
        'BP_ID': 'BP20200003048',
        'BP_VSN': '1',
        'BP_EX_ID': 'BX20200001336',
        'WF_EX_ID': 'WX20200002385',
        'ND_EX_ID': 'None'
    }

    SAMPLLogger().set_logstash_index(index='sampl2.0-202104')
    SAMPLLogger().set_logstash_addtional(additional=additional)
    logger.info('philip-sdlfjaskljd')

    logger.error('Error: Invalid input data format', extra={
        'service': 'on-demand',
        'project': 'Test',
        'blueprint': 'blueprint',
        'tags': 'tags',
        'is_bulk': 'bulk',
        'result': 'fail'
    }
                 )
