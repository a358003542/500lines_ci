
import argparse
import os
import re
import socket
import socketserver
import time
import threading

from loguru import logger

from helpers import communicate, COMMUNICATE_PING, COMMUNICATE_PONG, \
    COMMUNICATE_STATUS, COMMUNICATE_OK, COMMUNICATE_REGISTER, \
    COMMUNICATE_DISPATCH, COMMUNICATE_RESULT

logger.add("dispatcher.log")


# Shared dispatcher code
def dispatch_tests(server, commit_id):
    # NOTE: usually we don't run this forever
    while True:
        logger.debug("trying to dispatch to runners")
        for runner in server.runners:
            response = communicate(runner["host"],
                                   int(runner["port"]),
                                   f"runtest:{commit_id}")
            if response == COMMUNICATE_OK:
                logger.debug(f"adding id {commit_id}")
                server.dispatched_commits[commit_id] = runner
                if commit_id in server.pending_commits:
                    server.pending_commits.remove(commit_id)
                return  # first response runner
        time.sleep(2)


class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    runners = []  # Keeps track of test runner pool
    dead = False  # Indicate to other threads that we are no longer running
    dispatched_commits = {}  # Keeps track of commits we dispatched
    pending_commits = []  # Keeps track of commits we have yet to dispatch


class DispatcherHandler(socketserver.BaseRequestHandler):
    """
    The RequestHandler class for our dispatcher.
    This will dispatch test runners against the incoming commit
    and handle their requests and test results
    """

    command_re = re.compile(r"(\w+)(:.+)*")
    BUF_SIZE = 1024

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(self.BUF_SIZE).decode().strip()

        command_groups = self.command_re.match(self.data)
        if not command_groups:
            self.request.sendall("Invalid command".encode())
            return

        command = command_groups.group(1)
        if command == COMMUNICATE_STATUS:
            logger.debug("COMMUNICATE_STATUS")

            self.request.sendall(COMMUNICATE_OK.encode())

        elif command == COMMUNICATE_REGISTER:
            address = command_groups.group(2)
            host, port = re.findall(r":(\w*)", address)
            runner = {"host": host, "port": port}
            logger.debug(f"COMMUNICATE_REGISTER: runner on {host}:{port}")

            self.server.runners.append(runner)
            self.request.sendall(COMMUNICATE_OK.encode())

        elif command == COMMUNICATE_DISPATCH:
            logger.debug("COMMUNICATE_DISPATCH")
            commit_id = command_groups.group(2)[1:]
            if not self.server.runners:
                self.request.sendall("No runners are registered".encode())
            else:
                # The coordinator can trust us to dispatch the test
                self.request.sendall(COMMUNICATE_OK.encode())
                dispatch_tests(self.server, commit_id)

        elif command == COMMUNICATE_RESULT:
            logger.debug("COMMUNICATE_RESULT")
            results = command_groups.group(2)[1:]
            results = results.split(":")
            commit_id = results[0]
            length_msg = int(results[1])
            # 3 is the number of ":" in the sent command
            remaining_buffer = self.BUF_SIZE - (
                    len(command) + len(commit_id) + len(results[1]) + 3)
            if length_msg > remaining_buffer:
                self.data += self.request.recv(
                    length_msg - remaining_buffer).decode().strip()
            del self.server.dispatched_commits[commit_id]

            if not os.path.exists("test_results"):
                os.makedirs("test_results")

            with open("test_results/%s" % commit_id, "w") as f:
                data = self.data.split(":")[3:]
                data = "\n".join(data)
                f.write(data)

            self.request.sendall(COMMUNICATE_OK.encode())
        else:
            self.request.sendall("Invalid command".encode())


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",
                        help="dispatcher's host, by default it uses localhost",
                        default="localhost",
                        action="store")
    parser.add_argument("--port",
                        help="dispatcher's port, by default it uses 8888",
                        default=8888,
                        action="store")
    args = parser.parse_args()

    # Create the server
    server = ThreadingTCPServer((args.host, int(args.port)), DispatcherHandler)
    logger.debug(f'serving on {args.host}:{int(args.port)}')

    # Create a thread to check the runner pool
    def runner_checker(server):
        def manage_commit_lists(runner):
            for commit, assigned_runner in server.dispatched_commits.items():
                if assigned_runner == runner:
                    del server.dispatched_commits[commit]
                    # runner is not ok, remove it to pending list
                    server.pending_commits.append(commit)
                    break
            server.runners.remove(runner)

        while not server.dead:
            time.sleep(1)
            for runner in server.runners:
                try:
                    response = communicate(runner["host"],
                                           int(runner["port"]),
                                           COMMUNICATE_PING)
                    if response != COMMUNICATE_PONG:
                        logger.debug(f"removing runner {runner}")
                        manage_commit_lists(runner)
                except socket.error as e:
                    manage_commit_lists(runner)

    # this will kick off tests that failed
    def redistribute(server):
        while not server.dead:
            for commit in server.pending_commits:
                logger.debug("running redistribute")
                logger.debug(server.pending_commits)
                dispatch_tests(server, commit)
                time.sleep(5)

    runner_heartbeat = threading.Thread(target=runner_checker, args=(server,))
    redistributor = threading.Thread(target=redistribute, args=(server,))

    try:
        runner_heartbeat.start()
        redistributor.start()
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl+C or Cmd+C
        server.serve_forever()
    except (KeyboardInterrupt, Exception):
        # if any exception occurs, kill the thread
        server.dead = True

        runner_heartbeat.join()
        redistributor.join()


if __name__ == "__main__":
    serve()
