
import argparse
import os
import socket
import time

from git import Repo
from loguru import logger

from helpers import COMMIT_ID_FILE, run_or_fail, communicate
from helpers import COMMUNICATE_OK, COMMUNICATE_STATUS

logger.add("repo_observer.log")


def poll():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dispatcher-server",
                        help="dispatcher host:port, " \
                             "by default it uses localhost:8888",
                        default="localhost:8888",
                        action="store")
    parser.add_argument("repo", metavar="REPO", type=str,
                        help="path to the repository this will observe")
    args = parser.parse_args()
    dispatcher_host, dispatcher_port = args.dispatcher_server.split(":")

    while True:
        update_repo(args.repo)

        if os.path.isfile(COMMIT_ID_FILE):
            try:
                response = communicate(dispatcher_host,
                                       int(dispatcher_port),
                                       COMMUNICATE_STATUS)
            except socket.error as e:
                raise Exception(
                    "Could not communicate with dispatcher server: %s" % e)
            if response == COMMUNICATE_OK:
                commit = ""
                with open(COMMIT_ID_FILE, "r") as f:
                    commit = f.readline()

                response = communicate(dispatcher_host,
                                       int(dispatcher_port),
                                       f"dispatch:{commit}")
                if response != COMMUNICATE_OK:
                    raise Exception("Could not dispatch the test: %s" %
                                    response)
                logger.debug("dispatched!")
            else:
                # Something wrong happened to the dispatcher
                raise Exception("Could not dispatch the test: %s" %
                                response)
        time.sleep(5)


def update_repo(path):
    repo = Repo(path)
    git = repo.git

    run_or_fail(git.reset, args=('HEAD',), kwargs={'hard': True},
                info='Could not reset git')

    # get the most recent commit
    log_info = run_or_fail(git.log, args=('-n1',),
                           info="Could not call 'git log' on repository")

    last_commit_id = log_info.split()[1]

    run_or_fail(git.pull,
                info="Could not pull from repository")

    new_log_info = run_or_fail(git.log, args=('-n1',),
                               info="Could not call 'git log' on repository")

    new_commit_id = new_log_info.split()[1]

    if new_commit_id != last_commit_id:
        logger.debug('found changes.')
        with open(COMMIT_ID_FILE, 'wt', encoding='utf8') as f:
            print(new_commit_id, file=f)


if __name__ == "__main__":
    poll()
