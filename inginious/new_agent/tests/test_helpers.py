# coding=utf-8
import asyncio
import motor.motor_asyncio as motor

from inginious.new_agent.helpers import AsyncTimeoutItr, StreamWatcherWithInit

class TestStreamWatcherWithInit(object):
    """ Test the id checker """

    def test(self):
        client = motor.AsyncIOMotorClient("localhost")
        db = client["INGInious"]
        collection = db["agent_queue"]
        loop = asyncio.get_event_loop()

        sw = StreamWatcherWithInit(collection, [{'$match': {'operationType': 'insert'}}])

        async def init():
            await sw.init()
            print("done")

        async def insert():
            await collection.insert_one({'type': "test"})

        async def read():
            async for x in AsyncTimeoutItr(sw, 1):
                if x is None or x["fullDocument"]["type"] != "test":
                    raise Exception("failed")
                else:
                    break

        async def clean():
            await collection.delete_many({'type': "test"})
            await sw.close()

        loop.run_until_complete(init())
        loop.run_until_complete(asyncio.sleep(3))
        loop.run_until_complete(insert())
        loop.run_until_complete(asyncio.sleep(3))
        loop.run_until_complete(read())
        loop.run_until_complete(clean())