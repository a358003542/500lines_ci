import socket

from loguru import logger


def communicate(host, port, request: str) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.send(request.encode())
    response = s.recv(1024)
    s.close()
    return response.decode()


COMMUNICATE_STATUS = "status"  # report status
COMMUNICATE_OK = "ok"  # status ok
COMMUNICATE_PING = 'ping'  # ping-pong check
COMMUNICATE_PONG = 'pong'
COMMUNICATE_REGISTER = 'register'
COMMUNICATE_DISPATCH = 'dispatch'
COMMUNICATE_RESULT = 'result'
COMMUNICATE_RUNTEST = 'runtest'

def run_or_fail(func, args=None, kwargs=None, info='run command'):
    kwargs = kwargs if kwargs is not None else {}
    args = args if args is not None else ()

    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f'{info}\n{e}')


COMMIT_ID_FILE = '.commit_id'
