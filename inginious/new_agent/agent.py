# coding=utf-8
import asyncio
import logging
import datetime
import pymongo

from inginious.new_agent.helpers import AsyncTimeoutItr, StreamWatcherWithInit

async def reserve_submission(db_sub, name, authorized_steps):
    """
    :param db_sub: motor collection
    :param name: name of the current agent/frontend
    :param authorized_steps: a list of authorized grading steps
    :return: a reserved submission
    """
    find = lambda now: db_sub.find_one_and_update(
        {
            "status": "waiting",
            "next_grading_step": {"$in": authorized_steps}
        },
        {
            "$set": {
                "status": "processing",
                "grading_step_last_update": now
            },
            "$push": {
                "grading_step_done_by": name
            }
        },
        sort=[
            ("prio", pymongo.DESCENDING),
            ("submitted_on", pymongo.ASCENDING)
        ]
    )

    match_pipe = {
        'operationType': {"$in": ['insert', 'update', 'replace']},
        "$or": [
            {
                'fullDocument.status': 'waiting',
                'fullDocument.next_grading_step': {"$in": authorized_steps}
            },
            {
                'updateDescription.updatedFields.status': 'waiting',
                'updateDescription.updatedFields.next_grading_step': {"$in": authorized_steps},
            }
        ]
    }

    async with db_sub.watch([{'$match': match_pipe}]) as events_agent_queue:
        sub = await find(datetime.datetime.utcnow())
        if sub is not None:
            return sub
        async for _ in AsyncTimeoutItr(events_agent_queue, 30):
            sub = await find(datetime.datetime.utcnow())
            if sub is not None:
                return sub

    raise RuntimeError("Watch operation failed!")

class Agent(object):
    def __init__(self, db, name, tasks_filesystem, envs,
                 db_collection_submissions="submissions", db_collection_agent_queue="agent_queue",
                 db_collection_frontend_queue="frontend_queue", db_collection_agent_status="agent_status"):
        """
        :param db: a motor db
        :param name: Name of this agent
        :param tasks_filesystem: FileSystemProvider to the course/tasks
        :param envs: a dictionnary of the form {"env_name": GradingUnitConstructor} where GradingUnitConstructor
            is either a GradingUnit class with the default constructor or any function that takes two args
            (submission, send_message, agent) and returns a GradingUnit (see GradingUnit constructor for details).
        """
        # These fields can be read/modified/overridden in subclasses
        self._logger = logging.getLogger("inginious.agent")
        self._loop = asyncio.get_event_loop()
        self._tasks_filesystem = tasks_filesystem
        self._name = name

        # These fields should not be read/modified/overridden in subclasses
        self.__db = db
        self.__db_sub = db[db_collection_submissions]
        self.__db_queue = db[db_collection_agent_queue]
        self.__db_frontend_queue = db[db_collection_frontend_queue]
        self.__db_status = db[db_collection_agent_status]
        self.__closing = False
        self.__envs = envs

        # Used by _register()
        self.__current_submissionid = None

    @property
    def environments(self):
        """
        :return: a list of available environment ids
        """
        return list(self.__envs.keys())

    async def _register(self):
        while not self.__closing:
            await self.__db_status.update_one({"name": self._name},
                                              {"$set": {

                                                  "name": self._name,
                                                  "envs": self.environments,
                                                  "last-alive": datetime.datetime.utcnow()
                                              }}, upsert=True)

            # Update grading_step_last_update for the current submission if needed
            if self.__current_submissionid is not None:
                await self.__db_sub.update_one({"_id": self.__current_submissionid, "status": "processing"},
                                               {"$set": {"grading_step_last_update": datetime.datetime.utcnow()}})

            await asyncio.sleep(10)

    async def _get_next_submission(self):
        """
            Finds a submission to grade. Waits for notification on the queue,
            and attemps to reserve the new submission.
        """
        return await reserve_submission(self.__db_sub, self._name, self.environments)

    async def _cleanup(self, submission_id):
        """ Cleanup the queue after the agent is done. Do it at the end to avoid most race conditions.
            The rare others will be removed using the TTL mechanism.
        """
        await self.__db_queue.delete_many({"submissionid": submission_id})

    async def _send_message_frontend(self, submission_id, payload):
        """ Send a message to the frontend """
        pass

    async def _process_message(self, message, grading_unit):
        """ Process a message received from the frontend """
        try:
            if message["type"] == "kill":
                await grading_unit.kill()
            elif message["type"] == "message":
                await grading_unit.message(message["payload"])
        except:
            self._logger.exception("Grading unit made an exception while managing a message")

    async def _message_handler(self, submission_id, grading_unit):
        """ Receive message from the agent queue, and send them to the GradingUnit """

        # do not use motorcollection.watch() as it is only init when we first call async. Instead, use a modified version.
        try:
            async with StreamWatcherWithInit(self.__db_queue,
                                             [{'$match': {'operationType': {"$in": ['insert', 'replace']},
                                                          'fullDocument.submissionid': submission_id}}]) as events_agent_queue:
                await events_agent_queue.init()
                cursor = self.__db_queue.find({"submissionid": submission_id}).sort('_id', pymongo.ASCENDING)
                last_message_id = None
                async for entry in cursor:
                    last_message_id = entry["_id"]
                    await self._process_message(entry, grading_unit)

                async for entry in events_agent_queue:
                    message_id = entry["fullDocument"]["_id"]
                    if last_message_id is not None and message_id <= last_message_id:
                        continue
                    entry = entry["fullDocument"]
                    await self._process_message(entry, grading_unit)
        except asyncio.CancelledError:
            pass
        except:
            self._logger.exception("Exception in _message_handler")

    async def _store_results(self, submission_id, grading_steps, grading_step_idx, results, error):
        """
        :param submission_id:
        :param results:
            {
                result: "success" | "failed" | "timeout" | "overflow",
                text: str = "",
                grade: float = None,
                problems: Dict[str, SPResult] = None,
                tests: Dict[str, Any] = None,
                custom: Dict[str, Any] = None,
                archive: Optional[bytes] = None,
                stdout: Optional[str] = None,
                stderr: Optional[str] = None
            }
        :return:
        """
        data = {}

        try:
            if results["result"] not in ["success", "failed", "timeout", "overflow", "error"]:
                error = True
                data["result"] = "error"
            else:
                data["result"] = results["result"]
        except:
            error = True

        if not error:
            next_step_idx = grading_step_idx + 1 if grading_step_idx + 1 != len(grading_steps) else -1
            next_step = grading_steps[next_step_idx] if next_step_idx != -1 else "done"
        else:
            next_step_idx = -1
            next_step = "error"

        data.update({
            "status": "waiting",
            "next_grading_step": next_step,
            "next_grading_step_idx": next_step_idx,
            "grade": results.get("grade", 0.0),
            "text": results.get("text", ""),
            "tests": results.get("tests", []),
            "problems": results.get("problems", {}),
            "archive": results.get("archive"),
            "custom": results.get("custom", {}),
            "stdout": results.get("stdout", ""),
            "stderr": results.get("stderr", ""),
        })

        await self.__db_sub.find_one_and_update(
            {
                "_id": submission_id,
                "status": "processing",
            },
            {
                "$set": data
            }
        )

    async def run(self):
        self._logger.info("Agent started")
        self._loop.create_task(self._register())

        while not self.__closing:
            submission = await self._get_next_submission()

            # copy part of the submission. The dictionary might be modified by a grading unit.
            try:
                submission_id = submission["_id"]
                self.__current_submissionid = submission_id
                grading_steps = submission["grading_steps"]
                grading_step_idx = submission["next_grading_step_idx"]

                send_message = lambda p: self._send_message_frontend(submission_id, p)

                # Create the grading unit
                grading_unit = self.__envs[submission["next_grading_step"]](submission, send_message, self)
                await grading_unit.init()
            except:
                self._logger.exception("Probably malformed submission!")
                continue

            # Start the message handler for this task
            message_handler = asyncio.ensure_future(self._message_handler(submission_id, grading_unit))

            try:
                # Grade
                out = await grading_unit.grade()
                error = False
            except:
                self._logger.exception("Error while grading submission")
                out = {}
                error = True

            # Close the message handler
            if not message_handler.done():
                message_handler.cancel()

            try:
                self.__current_submissionid = None  # disables _register()
                await self._store_results(submission_id, grading_steps, grading_step_idx, out, error)
                await self._cleanup(submission["_id"])
            except:
                self._logger.exception("Error while storing grading results")
