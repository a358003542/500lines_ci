
import argparse
import errno
import os
import re
import socket
import socketserver
import time
import threading
import unittest

from git import Repo
from loguru import logger

from helpers import run_or_fail, communicate, COMMIT_ID_FILE, COMMUNICATE_OK, \
    COMMUNICATE_STATUS, COMMUNICATE_PING, COMMUNICATE_PONG, COMMUNICATE_RUNTEST, \
    COMMUNICATE_RESULT

logger.add("test_runner.log")


class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    dispatcher_server = None
    # Holds the dispatcher server host/port information
    last_communication = None
    # Keeps track of last communication from dispatcher
    busy = False  # Status flag
    dead = False  # Status flag


class TestHandler(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our server.
    """

    command_re = re.compile(r"(\w+)(:.+)*")

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).decode().strip()
        command_groups = self.command_re.match(self.data)
        command = command_groups.group(1)

        if not command:
            self.request.sendall("Invalid command".encode())
            return
        if command == COMMUNICATE_PING:
            logger.debug("COMMUNICATE_PING")
            self.server.last_communication = time.time()
            self.request.sendall(COMMUNICATE_PONG.encode())

        elif command == COMMUNICATE_RUNTEST:
            logger.debug(f"got runtest command: am I busy? {self.server.busy}")
            if self.server.busy:
                logger.debug("COMMUNICATE_BUSY")
                self.request.sendall("BUSY".encode())
            else:
                self.request.sendall(COMMUNICATE_OK.encode())
                logger.debug("COMMUNICATE_RUNTEST")
                commit_id = command_groups.group(2)[1:]
                self.server.busy = True
                self.run_tests(commit_id,
                               self.server.repo_folder)
                self.server.busy = False
        else:
            self.request.sendall("Invalid command".encode())

    def run_tests(self, commit_id, repo_folder):
        # update git repo
        test_runner_script(repo_folder, commit_id)

        if os.path.exists("results"):
            os.remove("results")

        # run the tests
        test_folder = os.path.join(repo_folder, "tests")
        suite = unittest.TestLoader().discover(test_folder)
        result_file = open("results", "w")
        unittest.TextTestRunner(result_file).run(suite)
        result_file.close()

        with open("results", 'r') as result_file:
            # give the dispatcher the results
            output = result_file.read()
            communicate(self.server.dispatcher_server["host"],
                        int(self.server.dispatcher_server["port"]),
                        f"{COMMUNICATE_RESULT}:{commit_id}:{len(output)}:{output}")


def serve():
    range_start = 8900
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        help="runner's host, by default it uses localhost",
                        default="localhost",
                        action="store")
    parser.add_argument("--port",
                        help="runner's port, by default it uses values >=%s" % range_start,
                        action="store")
    parser.add_argument("--dispatcher-server",
                        help="dispatcher host:port, by default it uses " \
                             "localhost:8888",
                        default="localhost:8888",
                        action="store")
    parser.add_argument("repo", metavar="REPO", type=str,
                        help="path to the repository this will observe")
    args = parser.parse_args()

    runner_host = args.host
    runner_port = None
    tries = 0
    if not args.port:
        runner_port = range_start
        while tries < 100:
            try:
                server = ThreadingTCPServer((runner_host, runner_port),
                                            TestHandler)
                logger.debug(server)
                logger.debug(runner_port)
                break
            except socket.error as e:
                if e.errno == errno.EADDRINUSE:
                    tries += 1
                    runner_port = runner_port + tries
                    continue
                else:
                    raise e
        else:
            raise Exception("Could not bind to ports in range %s-%s" % (
                range_start, range_start + tries))
    else:
        runner_port = int(args.port)
        server = ThreadingTCPServer((runner_host, runner_port), TestHandler)

    server.repo_folder = args.repo

    dispatcher_host, dispatcher_port = args.dispatcher_server.split(":")
    server.dispatcher_server = {"host": dispatcher_host,
                                "port": dispatcher_port}

    response = communicate(server.dispatcher_server["host"],
                           int(server.dispatcher_server["port"]),
                           "register:%s:%s" %
                           (runner_host, runner_port))

    if response != COMMUNICATE_OK:
        raise Exception("Can't register with dispatcher!")
    else:
        # first register runner need init last_communication
        server.last_communication = time.time()

    def dispatcher_checker(server):
        # Checks if the dispatcher went down. If it is down, we will shut down
        # if since the dispatcher may not have the same host/port
        # when it comes back up.
        while not server.dead:
            time.sleep(5)
            if (time.time() - server.last_communication) > 10:
                try:
                    response = communicate(
                        server.dispatcher_server["host"],
                        int(server.dispatcher_server["port"]),
                        COMMUNICATE_STATUS)
                    if response != COMMUNICATE_OK:
                        logger.debug("Dispatcher is no longer functional")
                        server.shutdown()
                        return
                except socket.error as e:
                    logger.error("Can't communicate with dispatcher: {e}")
                    server.shutdown()
                    return

    t = threading.Thread(target=dispatcher_checker, args=(server,))
    try:
        t.start()
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()
    except (KeyboardInterrupt, Exception):
        # if any exception occurs, kill the thread
        server.dead = True
        t.join()


def test_runner_script(path, commit_id):
    if os.path.exists(COMMIT_ID_FILE):
        os.remove(COMMIT_ID_FILE)

    repo = Repo(path)
    git = repo.git

    run_or_fail(git.clean, args=('-d', '-f', '-x'),
                info='Could not clean repository')
    run_or_fail(git.pull, info='Could not call git pull')
    run_or_fail(git.reset, args=(commit_id,), kwargs={'hard': True},
                info='Could not update to given commit hash')


if __name__ == "__main__":
    serve()
