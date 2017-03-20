#
# Test ex. Websocket Event Server
#
# Authtor Makarov A
# Date    20/03/2017
#

import asyncio
import websockets
import zmq
from threading import Thread
from queue import Queue
from collections import deque
import redis
import time
import json
import logging
import threading


#
# config
#


HOST = "localhost"
PORT = 8080

RHOST = "localhost"
RPORT = 6379
RDB = 0

L_HOST = "localhost"
L_PORT = 5556

######

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s %(threadName)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _time_log(f):
    """
    util
    time measure decorator
    """
    def wrap(*args, **kwargs):
        tb = time.time()
        ret = f(*args, **kwargs)
        logger.debug("time elapsed: {} {} s.".format(f.__name__, time.time() - tb))
        return ret
    return wrap


class Event(object):
    """
        Event values struct
        and L1 cache (default 300)
    """
    def __init__(self, cached_size=300):
        self.data = {}
        self._cache = deque([], cached_size)

    @_time_log
    def update(self, timestamp, data):
        self.data = {}
        for i in data.items():
            self.data[i[0]] = data

        # add to cache
        self._cache.append(self.data)

    def current(self):
        return self.data

    def get_cache(self):
        return self._cache

    @_time_log
    def set_cache(self, data):
        d = {}
        for i in data:
            tmp = json.loads(i)

            for j in  tmp.items():
                d[j[0]] = j[1]
            self._cache.append(d)


def b_time():
    """ begin date memorizer with skip"""
    tb = time.time()
    while True:
        skip = yield
        if skip:
            tb = time.time()

        yield tb


def event_saver(queue):
    """
        data save consumer
    """
    r = redis.StrictRedis(host=RHOST, port=RPORT, db=RDB)
    _cap = 30 * 60  # 30 min capacity

    while True:
        data = queue.get()
        data = json.dumps(data)  # convert to json
        logger.debug("get data: {0}".format(data))

        # try to save
        while True:
            try:
                r.lpush("p", data)
                r.ltrim("p", 0, _cap - 1)

                break

            except Exception as e:
                logger.debug("cann't save data: {0} .. retry after 3 sec..".format(data))
                time.sleep(3)

                continue


def event_producer(events, queue, ev):

    logger.debug("start recieve from tcp://{0}:{1}".format(L_HOST, L_PORT))

    ctx = zmq.Context()

    sock = ctx.socket(zmq.SUB)
    sock.connect("tcp://{0}:{1}".format(L_HOST, L_PORT))
    sock.setsockopt(zmq.SUBSCRIBE, b"")

    logger.debug("subscribe to tcp://{0}:{1}".format(L_HOST, L_PORT))

    while True:
        logger.debug("begin time is {}".format(time.time()))
        try:

            data = sock.recv_string()
            data = json.loads(data)

            events.update(time.time(), data)
            logger.debug("events's data: {0}".format(events.current()))
            time.sleep(1)

            ev.set()

            if not queue.full():
                logger.debug("queue size is {0}".format(queue.qsize()))
                queue.put_nowait(events.current())

        except Exception as e:
            logger.error(e)
            continue


def _sleep1(delta):
    #
    # get more fair interval
    #
    logger.debug("finish ops for: {}".format(delta))
    if delta >= 1.0:
        logger.warn("time is out..")

    else:
        time.sleep(1.0 - delta)


async def _execute(websocket, data):
        """
           Cmd handler
        """
        global events
        global ev

        cmd = data.get("action")
        logger.debug("cmd is {}".format(cmd))

        #
        # Assets handler
        #
        if cmd == "info":

            res = {"action":"info",
                   "message":{
                       "disk": "sda1"
                    }
                   }

            await websocket.send(json.dumps(res))

        #
        # Subscribe handler
        #
        elif cmd == "subscribe":

            _name = await _push_cache(websocket, data)
            # send last
            while True:
                #
                # long pooling
                # TODO create pub/sub messaging throw registered client &
                # observable class
                #
                await ev.wait()
                ev.clear()

                data = events.current()

                listener_task = asyncio.ensure_future(websocket.recv())
                producer_task = asyncio.ensure_future(_push_last(websocket, data))

                done, pending = await asyncio.wait([listener_task, producer_task],
                                                    return_when=asyncio.FIRST_COMPLETED)

                if producer_task in done:
                    message = producer_task.result()

                    if message == 1:
                        break
                else:
                    producer_task.cancel()

                if listener_task in done:
                    message = json.loads(listener_task.result())
                    if message.get("action") == "subscribe":
                        _name = await _push_cache(websocket, message)

                else:
                    listener_task.cancel()


        #
        # anything handler
        #
        else:
            raise Exception("cmd is not avaliable")


async def _push_last(websocket, data):

    try:
        await websocket.send(json.dumps(data))
    except Exception:
        return 1

    return 0

async def _push_cache(websocket, data):

    # send cache 5 min data
    cache5 = [x for x in events.get_cache()]
    logger.debug("len cache is {}".format(len(cache5)))
    await websocket.send(json.dumps(cache5))

    return 0


async def h_event(websocket, path):
    """
    Handler for ws connections
    """
    global registered

    registered.add(websocket)
    logger.info("connected users: {0}".format(len(registered)))

    try:

        while True:

            data = await websocket.recv()

            data = json.loads(data)
            logger.debug("recv {}".format(data))
            await _execute(websocket, data)

            continue

    finally:
        registered.remove(websocket)
        logger.info("connected users: {0}".format(len(registered)))


@_time_log
def restore_events(events, size=300):
    """
    restore 5 min cache from redis
    """
    try:
        r = redis.StrictRedis(host=RHOST, port=RPORT, db=RDB, decode_responses=True)
        data = r.lrange("p", 0, size - 1)

        logger.debug("reading from cache from redis: {0}".format(len(data)))
        events.set_cache(data)

    except Exception as e:
        logger.warn("error when restore from redis: {0}".format(e))


#
# TODO remove global reference
#
events = Event()
registered = set()

class Event_(asyncio.Event):
        def set(self):
            self._loop.call_soon_threadsafe(super().set)

ev = Event_()

def main():

    global events
    global ev

    restore_events(events)

    queue = Queue(600)  # 10 min capacity

    #
    # start producer for events
    # it pull, write & publish
    #
    t = Thread(target=event_producer, args=(events, queue, ev))

    #
    # start consumer for events
    # save events to any store (last 30 min)
    #
    # if store is not avaliable put events in memory queue
    # for later flush
    #
    t2 = Thread(target=event_saver, args=(queue,))

    t.start()
    t2.start()

    #
    # start event loop in main thread for websocket requests
    # events comes from publisher
    #

    start_server = websockets.serve(h_event, HOST, PORT)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()
