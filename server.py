#
# ZMQ server for event publish
#
# Authtor makarov
# Date 20/03/2017
#

import logging
import zmq
from threading import Thread
import json
import time

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


L_PORT = 5555
P_PORT = 5556


def start_listner(ctx, port):
    """
    Start server for incoming commands
    """
    logger.debug("start listner server {0} / {1}".format(ctx, port))

    lsock = ctx.socket(zmq.ROUTER)
    lsock.bind("tcp://*:{0}".format(port))

    while True:
        cmd = lsock.recv_string()
        logger.debug("get command: {0}".format(cmd))

        lsock.send_string("ok")


def start_producer(ctx, port):
    """
    Start publisher
    """
    logger.debug("start publish server {0} / {1}".format(ctx, port))

    lsock = ctx.socket(zmq.PUB)
    lsock.bind("tcp://*:{0}".format(port))

    while True:

        # data stub
        data = {"data": "123"}
        logger.debug("publish data {0}".format(data))

        lsock.send_string(json.dumps(data))
        time.sleep(1)


def main():
    logger.info("start server..")

    #
    # create zmq context
    #
    ctx = zmq.Context()

    #
    # start producer for events
    #
    t = Thread(target=start_producer, args=(ctx, P_PORT))

    #
    # start listner server
    #
    t2 = Thread(target=start_listner, args=(ctx, L_PORT))

    t.start()
    t2.start()


if __name__ == "__main__":
    main()
