#
# Test ex. Websocket client
#
# Authtor Makarov A
# Date    06/03/17
#

import asyncio
import websockets
import json

async def test():
    async with websockets.connect('ws://localhost:8080') as websocket:
        while True:
            d = json.dumps({"action":"subscribe","message":{"assetId":2}})
            await websocket.send(d)

            n = 10
            while True:
                data = await websocket.recv()
                print(data)
                # break
                n -= 1
                if n == 0:
                    d = json.dumps({"action":"subscribe","message":{"assetId":1}})
                    await websocket.send(d)

                # print("< {}".format(data))

            continue


if __name__ == "__main__":

    tasks = []
    n = 1
    while n > 0:
        n -= 1
        tasks.append(test())


    asyncio.get_event_loop().run_until_complete(
        asyncio.wait(tasks)
    )
