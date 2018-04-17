# coding=utf-8
import asyncio
import logging
import motor.motor_asyncio as motor
import pymongo

from inginious.new_agent.agent import Agent
from inginious.new_agent.grading_unit import GradingUnit
from inginious.new_agent.helpers import AsyncTimeoutItr, StreamWatcherWithInit

async def check_database_format(db,
                                db_collection_submissions="submissions",
                                db_collection_agent_queue="agent_queue",
                                db_collection_frontend_queue="frontend_queue",
                                db_collection_agent_status="agent_status"):
    submissions = db[db_collection_submissions]
    agent_queue = db[db_collection_agent_queue]
    frontend_queue = db[db_collection_frontend_queue]
    agent_status = db[db_collection_agent_status]

    await submissions.create_index([("prio", pymongo.DESCENDING), ("submitted_on", pymongo.ASCENDING)],
                                   partialFilterExpression={"status": "waiting"})
    await agent_status.create_index("name", unique=True)
    await agent_queue.create_index("created", expireAfterSeconds=60 * 60 * 24)
    await agent_queue.create_index("submissionid")
    await agent_queue.create_index("agentid")
    await frontend_queue.create_index("created", expireAfterSeconds=60 * 60 * 24)
    await frontend_queue.create_index("submissionid")
    await frontend_queue.create_index("frontendid")

if __name__ == '__main__':
    class GUDemo(GradingUnit):
        def __init__(self, submission, send_message, agent, name):
            super().__init__(submission, send_message, agent)
            self.name = name

        async def init(self):
            pass

        async def kill(self):
            pass

        async def message(self, message):
            print("Message", message)

        async def grade(self):
            await asyncio.sleep(120)
            return {"result": "success", "text": self.submission.get("text", "") + "Processed by {}\n".format(self.name)}

    logger = logging.getLogger("inginious")
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    client = motor.AsyncIOMotorClient("localhost")
    db = client["INGInious"]

    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_database_format(db))

    create_gudemo = lambda name: (lambda a,b,c: GUDemo(a,b,c,name))

    agent1 = Agent(db, "test1", None, {"a": create_gudemo("test1-a"), "b": create_gudemo("test1-b")})
    agent2 = Agent(db, "test2", None, {"c": create_gudemo("test2-c"), "b": create_gudemo("test2-b")})

    loop.create_task(agent1.run())
    loop.create_task(agent2.run())
    loop.run_forever()
    loop.close()