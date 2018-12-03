# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Manages submissions """
import asyncio
import io
import gettext
import logging
import os.path
import tarfile
import tempfile
import time
from datetime import datetime, timedelta

import bson
import pymongo
import web
from bson.objectid import ObjectId
from pymongo.collection import ReturnDocument

import inginious.common.custom_yaml
from inginious.frontend.parsable_text import ParsableText
from inginious.new_agent.agent import reserve_submission


class SubmissionManagerAsyncRunner:
    """
        Part of the SubmissionManager that runs in the asyncio loop.
        Detects various states of the submissions. Handles messages.
    """

    def __init__(self, submission_manager, motor_submission_collection, loop):
        self._logger = logging.getLogger("inginious.webapp.submissions.runner")
        self._submission_manager = submission_manager
        self._loop = loop
        self._closing = False
        self._db_sub = motor_submission_collection
        self._current_submissionid = None

    async def run(self):
        """ Main method that starts all the subcomponents """
        self._logger.info("SubmissionManager runner started")
        asyncio.ensure_future(self._closing_runner())
        asyncio.ensure_future(self._register())
        asyncio.ensure_future(self._error_runner())

    async def _closing_runner(self):
        """ Detects finished submissions """
        while not self._closing:
            try:
                submission = await reserve_submission(self._db_sub, "Frontend", ["done", "error"])
                self._current_submissionid = submission["_id"]
                await self._loop.run_in_executor(None, lambda: self._submission_manager._job_done(submission))
                self._current_submissionid = None
            except asyncio.CancelledError:
                self._closing = True
            except:
                self._logger.exception("Exception in _closing_runner")

    async def _register(self):
        """ While a submission is being processed, updates grading_step_last_update"""
        while not self._closing:
            # Update grading_step_last_update for the current submission if needed
            try:
                if self._current_submissionid is not None:
                    await self._db_sub.update_one({"_id": self._current_submissionid, "status": "processing"},
                                                   {"$set": {"grading_step_last_update": datetime.utcnow()}})
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self._closing = True
            except:
                self._logger.exception("Exception in _register")

    async def _error_runner(self):
        """
            Finds submission whose assigned agent/frontend has died, redirect them,
            in error state, to a new frontend
        """
        while not self._closing:
            try:
                result = await self._db_sub.update_many(
                    {"status": "processing", "grading_step_last_update": {"$lt": datetime.utcnow()-timedelta(seconds=60)}},
                    {"$set": {"status": "waiting", "next_grading_step": "error",
                              "next_grading_step_idx": -1, "text": "Agent did not respond in time",
                              "result": "error"}}
                )
                if result.modified_count != 0:
                    self._logger.warning("%i submission(s) in failed state were retrieved", result.modified_count)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                self._closing = True
            except:
                self._logger.exception("Exception in _error_runner")


class WebAppSubmissionManager:
    """ Manages submissions. Communicates with the database and the client. """

    def __init__(self, asyncio_loop, motor_submission_collection, course_factory, user_manager, database, gridfs, hook_manager, lti_outcome_manager):
        """
        :arg asyncio_loop: Asyncio event loop
        :type user_manager: inginious.frontend.user_manager.UserManager
        :type database: pymongo.database.Database
        :type gridfs: gridfs.GridFS
        :type hook_manager: inginious.common.hook_manager.HookManager
        :return:
        """
        self._user_manager = user_manager
        self._database = database
        self._gridfs = gridfs
        self._hook_manager = hook_manager
        self._logger = logging.getLogger("inginious.webapp.submissions")
        self._lti_outcome_manager = lti_outcome_manager
        self._course_factory = course_factory
        self._async_runner = SubmissionManagerAsyncRunner(self, motor_submission_collection, asyncio_loop)
        asyncio_loop.call_soon_threadsafe(lambda: asyncio_loop.create_task(self._async_runner.run()))

    def _job_done(self, submission):
        new_status = "done" if submission["next_grading_step"] == "done" else "error"

        set_obj = {
            "status": new_status
        }

        unset_obj = {
            "jobid": "",
            "ssh_host": "",
            "ssh_port": "",
            "ssh_password": "",
            "next_grading_step": "",
            "next_grading_step_idx": ""
        }

        submission = self._database.submissions.find_one_and_update(
            {"_id": submission["_id"], "status": "processing"},
            {"$set": set_obj, "$unset": unset_obj},
            return_document=ReturnDocument.AFTER
        )

        if submission is None:
            self._logger.error("Submission was reserved for processing, but another agent/frontend modified it!")
            return

        archive = None if submission.get("archive") is None else self._gridfs.get(submission['archive'])
        self._hook_manager.call_hook("submission_done", submission=submission,
                                     archive=archive,
                                     newsub=submission.get("replayed", False))

        task = self._course_factory.get_course(submission["courseid"]).get_task(submission["taskid"])
        for username in submission["username"]:
            self._user_manager.update_user_stats(username, task, submission, submission.get("replayed", False))

        if "outcome_service_url" in submission and "outcome_result_id" in submission and "outcome_consumer_key" in submission:
            for username in submission["username"]:
                self._lti_outcome_manager.add(username,
                                              submission["courseid"],
                                              submission["taskid"],
                                              submission["outcome_consumer_key"],
                                              submission["outcome_service_url"],
                                              submission["outcome_result_id"])

    def _before_submission_insertion(self, task, inputdata, debug, obj):
        """
        Called before any new submission is inserted into the database. Allows you to modify obj, the new document that will be inserted into the
        database. Should be overridden in subclasses.

        :param task: Task related to the submission
        :param inputdata: input of the student
        :param debug: True, False or "ssh". See add_job.
        :param obj: the new document that will be inserted
        """
        username = self._user_manager.session_username()

        if task.is_group_task() and not self._user_manager.has_staff_rights_on_course(task.get_course(), username):
            group = self._database.aggregations.find_one(
                {"courseid": task.get_course_id(), "groups.students": username},
                {"groups": {"$elemMatch": {"students": username}}})

            obj.update({"username": group["groups"][0]["students"]})
        else:
            obj.update({"username": [username]})

        lti_info = self._user_manager.session_lti_info()
        if lti_info is not None and task.get_course().lti_send_back_grade():
            outcome_service_url = lti_info["outcome_service_url"]
            outcome_result_id = lti_info["outcome_result_id"]
            outcome_consumer_key = lti_info["consumer_key"]

            # safety check
            if outcome_result_id is None or outcome_service_url is None:
                self._logger.error("outcome_result_id or outcome_service_url is None, but grade needs to be sent back to TC! Ignoring.")
                return

            obj.update({"outcome_service_url": outcome_service_url,
                        "outcome_result_id": outcome_result_id,
                        "outcome_consumer_key": outcome_consumer_key})

    def _after_submission_insertion(self, task, inputdata, debug, submission, submissionid):
        """
                Called after any new submission is inserted into the database, but before starting the job.  Should be overridden in subclasses.
                :param task: Task related to the submission
                :param inputdata: input of the student
                :param debug: True, False or "ssh". See add_job.
                :param submission: the new document that was inserted (do not contain _id)
                :param submissionid: submission id of the submission
                """
        # If we are submitting for a group, send the group (user list joined with ",") as username
        if "group" not in [p.get_id() for p in task.get_problems()]:  # do not overwrite
            username = self._user_manager.session_username()
            if task.is_group_task() and not self._user_manager.has_staff_rights_on_course(task.get_course(), username):
                group = self._database.aggregations.find_one(
                    {"courseid": task.get_course_id(), "groups.students": username},
                    {"groups": {"$elemMatch": {"students": username}}})
                inputdata["username"] = ','.join(group["groups"][0]["students"])

        return self._delete_exceeding_submissions(self._user_manager.session_username(), task)

    def replay_job(self, task, submission, copy=False, debug=False):
        """
        Replay a submission: add the same job in the queue, keeping submission id, submission date and input data
        :param submission: Submission to replay
        :param copy: If copy is true, the submission will be copied to admin submissions before replay
        :param debug: If debug is true, more debug data will be saved
        :type debug: bool or string
        """
        if not self._user_manager.session_logged_in():
            raise Exception("A user must be logged in to submit an object")

        # Load input data and add username to dict
        inputdata = bson.BSON.decode(self._gridfs.get(submission["input"]).read())

        if copy:
            self.add_job(task, inputdata, debug)
            self._logger.info("Copying submission %s - %s - %s - %s as %s", submission["username"],
                              submission["courseid"],
                              submission["taskid"], submission["_id"], self._user_manager.session_username())
            return

        # Remove the submission archive : it will be regenerated
        if submission.get("archive", None) is not None:
            self._gridfs.delete(submission["archive"])

        to_update = {
            "status": "waiting",
            "debug": debug,
            "response_type": task.get_response_type(),
            "next_grading_step": task.get_environment(),
            "next_grading_step_idx": 0,
            "grading_steps": [task.get_environment()],
            "grading_step_done_by": [],
            "grading_step_last_update": datetime.now()
        }

        to_unset = {
            "result": "",
            "grade": "",
            "text": "",
            "tests": "",
            "problems": "",
            "archive": "",
            "custom": ""
        }

        self._database.submissions.update({"_id": submission["_id"]}, {"$set": to_update, "$unset": to_unset})

        self._logger.info("Replaying submission %s - %s - %s - %s", submission["username"], submission["courseid"],
                          submission["taskid"], submission["_id"])

    def get_available_environments(self):
        """:return a list of available environments """
        # TODO: cache if needed
        return list(self._database.agent_status.distinct("envs"))

    def get_submission(self, submissionid, user_check=True):
        """ Get a submission from the database """
        sub = self._database.submissions.find_one({'_id': ObjectId(submissionid)})
        if user_check and not self.user_is_submission_owner(sub):
            return None
        return sub

    def add_job(self, task, inputdata, debug=False):
        """
        Add a job in the queue and returns a submission id.
        :param task:  Task instance
        :type task: inginious.frontend.tasks.WebAppTask
        :param inputdata: the input as a dictionary
        :type inputdata: dict
        :param debug: If debug is true, more debug data will be saved
        :type debug: bool
        :returns: the new submission id and the removed submission id
        """
        if not self._user_manager.session_logged_in():
            raise Exception("A user must be logged in to submit an object")

        username = self._user_manager.session_username()

        # Prevent student from submitting several submissions together
        waiting_submission = self._database.submissions.find_one({
            "courseid": task.get_course_id(),
            "taskid": task.get_id(),
            "username": username,
            "status": {"$in": ["waiting", "processing"]}})

        if waiting_submission is not None:
            raise Exception("A submission is already pending for this task!")

        obj = {
            "courseid": task.get_course_id(),
            "taskid": task.get_id(),
            "status": "waiting",
            "submitted_on": datetime.now(),
            "username": [username],
            "response_type": task.get_response_type(),
            "grading_steps": [task.get_environment()],
            "next_grading_step": task.get_environment(),
            "next_grading_step_idx": 0,
            "grading_step_done_by": [],
            "grading_step_last_update": datetime.now(),
            "debug": debug
        }

        # Send additional data to the client in inputdata. For now, the username and the language. New fields can be added with the
        # new_submission hook
        inputdata["@username"] = username
        inputdata["@lang"] = self._user_manager.session_language()
        # Retrieve input random
        states = self._database.user_tasks.find_one(
            {"courseid": task.get_course_id(), "taskid": task.get_id(), "username": username},
            {"random": 1, "state": 1})
        inputdata["@random"] = states["random"] if "random" in states else []
        inputdata["@state"] = states["state"] if "state" in states else ""

        self._hook_manager.call_hook("new_submission", submission=obj, inputdata=inputdata)
        obj["input"] = self._gridfs.put(bson.BSON.encode(inputdata))

        self._before_submission_insertion(task, inputdata, debug, obj)
        submissionid = self._database.submissions.insert(obj)
        to_remove = self._after_submission_insertion(task, inputdata, debug, obj, submissionid)

        self._logger.info("New submission from %s - %s - %s/%s - %s", self._user_manager.session_username(),
                          self._user_manager.session_email(), task.get_course_id(), task.get_id(),
                          web.ctx['ip'])

        return submissionid, to_remove

    def _delete_exceeding_submissions(self, username, task, max_submissions_bound=-1):
        """ Deletes exceeding submissions from the database, to keep the database relatively small """

        if max_submissions_bound <= 0:
            max_submissions = task.get_stored_submissions()
        elif task.get_stored_submissions() <= 0:
            max_submissions = max_submissions_bound
        else:
            max_submissions = min(max_submissions_bound, task.get_stored_submissions())

        if max_submissions <= 0:
            return []
        tasks = list(self._database.submissions.find(
            {"username": username, "courseid": task.get_course_id(), "taskid": task.get_id()},
            projection=["_id", "status", "result", "grade", "submitted_on"],
            sort=[('submitted_on', pymongo.ASCENDING)]))

        # List the entries to keep
        to_keep = set([])

        if task.get_evaluate() == 'best':
            # Find the best "status"="done" and "result"="success"
            idx_best = -1
            for idx, val in enumerate(tasks):
                if val["status"] == "done":
                    if idx_best == -1 or tasks[idx_best]["grade"] < val["grade"]:
                        idx_best = idx

            # Always keep the best submission
            if idx_best != -1:
                to_keep.add(tasks[idx_best]["_id"])
        elif task.get_evaluate() == 'student':
            user_task = self._database.user_tasks.find_one({
                "courseid": task.get_course_id(),
                "taskid": task.get_id(),
                "username": username
            })

            submissionid = user_task.get('submissionid', None)
            if submissionid:
                to_keep.add(submissionid)

        # Always keep running submissions
        for val in tasks:
            if val["status"] == "waiting":
                to_keep.add(val["_id"])

        while len(to_keep) < max_submissions and len(tasks) > 0:
            to_keep.add(tasks.pop()["_id"])

        to_delete = {val["_id"] for val in tasks}.difference(to_keep)
        self._database.submissions.delete_many({"_id": {"$in": list(to_delete)}})

        return list(map(str, to_delete))

    def get_input_from_submission(self, submission, only_input=False):
        """
            Get the input of a submission. If only_input is False, returns the full submissions with a dictionnary object at the key "input".
            Else, returns only the dictionnary.
        """
        inp = bson.BSON.decode(self._gridfs.get(submission['input']).read())
        if only_input:
            return inp
        else:
            submission["input"] = inp
            return submission

    def get_feedback_from_submission(self, submission, only_feedback=False, show_everything=False, translation=gettext.NullTranslations()):
        """
            Get the input of a submission. If only_input is False, returns the full submissions with a dictionnary object at the key "input".
            Else, returns only the dictionnary.

            If show_everything is True, feedback normally hidden is shown.
        """
        if only_feedback:
            submission = {"text": submission.get("text", None), "problems": dict(submission.get("problems", {}))}
        if "text" in submission:
            submission["text"] = ParsableText(submission["text"], submission["response_type"], show_everything, translation).parse()
        if "problems" in submission:
            for problem in submission["problems"]:
                if isinstance(submission["problems"][problem], str):  # fallback for old-style submissions
                    submission["problems"][problem] = (submission.get('result', 'crash'), ParsableText(submission["problems"][problem],
                                                                                                       submission["response_type"],
                                                                                                       show_everything, translation).parse())
                else:  # new-style submission
                    submission["problems"][problem] = (submission["problems"][problem][0], ParsableText(submission["problems"][problem][1],
                                                                                                        submission["response_type"],
                                                                                                        show_everything, translation).parse())
        return submission

    def is_running(self, submissionid, user_check=True):
        """ Tells if a submission is running/in queue """
        submission = self.get_submission(submissionid, user_check)
        return submission["status"] == "waiting"

    def is_done(self, submissionid_or_submission, user_check=True):
        """ Tells if a submission is done and its result is available """
        # TODO: not a very nice way to avoid too many database call. Should be refactored.
        if isinstance(submissionid_or_submission, dict):
            submission = submissionid_or_submission
        else:
            submission = self.get_submission(submissionid_or_submission, False)
        if user_check and not self.user_is_submission_owner(submission):
            return None
        return submission["status"] == "done" or submission["status"] == "error"

    def kill_running_submission(self, submissionid, user_check=True):
        """ Attempt to kill the remote job associated with this submission id.
        :param submissionid:
        :param user_check: Check if the current user owns this submission
        :return: True if the job was killed, False if an error occurred
        """
        submission = self.get_submission(submissionid, user_check)
        if not submission:
            return False
        if "jobid" not in submission:
            return False

        # TODO

    def user_is_submission_owner(self, submission):
        """ Returns true if the current user is the owner of this jobid, false else """
        if not self._user_manager.session_logged_in():
            raise Exception("A user must be logged in to verify if he owns a jobid")

        return self._user_manager.session_username() in submission["username"]

    def get_user_submissions(self, task):
        """ Get all the user's submissions for a given task """
        if not self._user_manager.session_logged_in():
            raise Exception("A user must be logged in to get his submissions")

        cursor = self._database.submissions.find({"username": self._user_manager.session_username(),
                                                  "taskid": task.get_id(), "courseid": task.get_course_id()})
        cursor.sort([("submitted_on", -1)])
        return list(cursor)

    def get_user_last_submissions(self, limit=5, request=None):
        """ Get last submissions of a user """
        if request is None:
            request = {}

        request.update({"username": self._user_manager.session_username()})

        # Before, submissions were first sorted by submission date, then grouped
        # and then resorted by submission date before limiting. Actually, grouping
        # and pushing, keeping the max date, followed by result filtering is much more
        # efficient
        data = self._database.submissions.aggregate([
            {"$match": request},
            {"$group": {"_id": {"courseid": "$courseid", "taskid": "$taskid"},
                        "submitted_on": {"$max": "$submitted_on"},
                        "submissions": {"$push": {
                            "_id": "$_id",
                            "result": "$result",
                            "status" : "$status",
                            "courseid": "$courseid",
                            "taskid": "$taskid",
                            "submitted_on": "$submitted_on"
                        }},
            }},
            {"$project": {
                "submitted_on": 1,
                "submissions": {
                    # This could be replaced by $filter if mongo v3.2 is set as dependency
                    "$setDifference": [
                        {"$map": {
                            "input": "$submissions",
                            "as": "submission",
                            "in": {
                                "$cond": [{"$eq": ["$submitted_on", "$$submission.submitted_on"]}, "$$submission", False]
                            }
                        }},
                        [False]
                    ]
                }
            }},
            {"$sort": {"submitted_on": pymongo.DESCENDING}},
            {"$limit": limit}
        ])

        return [item["submissions"][0] for item in data]

    def get_gridfs(self):
        """ Returns the GridFS used by the submission manager """
        return self._gridfs

    def get_submission_archive(self, submissions, sub_folders, aggregations, archive_file=None):
        """
        :param submissions: a list of submissions
        :param sub_folders: possible values:
            []: put all submissions in /
            ['taskid']: put all submissions for each task in a different directory /taskid/
            ['username']: put all submissions for each user in a different directory /username/
            ['taskid','username']: /taskid/username/
            ['username','taskid']: /username/taskid/
        :return: a file-like object containing a tgz archive of all the submissions
        """
        tmpfile = archive_file if archive_file is not None else tempfile.TemporaryFile()
        tar = tarfile.open(fileobj=tmpfile, mode='w:gz')

        for submission in submissions:
            submission = self.get_input_from_submission(submission)

            submission_yaml = io.BytesIO(inginious.common.custom_yaml.dump(submission).encode('utf-8'))

            # Considering multiple single submissions for each user
            for username in submission["username"]:
                # Compute base path in the tar file
                base_path = "/"
                for sub_folder in sub_folders:
                    if sub_folder == 'taskid':
                        base_path = submission['taskid'] + base_path
                    elif sub_folder == 'username':
                        base_path = '_' + '-'.join(submission['username']) + base_path
                        base_path = base_path[1:]
                    elif sub_folder == 'aggregation':
                        if username in aggregations:
                            if aggregations[username] is None:
                                # If classrooms are not used, and user is not grouped, his classroom is replaced by None
                                base_path = '_' + '-'.join(submission['username']) + base_path
                                base_path = base_path[1:]
                            else:
                                base_path = (aggregations[username]["description"] +
                                             " (" + str(aggregations[username]["_id"]) + ")").replace(" ", "_") + base_path

                    base_path = '/' + base_path
                base_path = base_path[1:]

                submission_yaml_fname = base_path + str(submission["_id"]) + '/submission.test'

                # Avoid putting two times the same submission on the same place
                if submission_yaml_fname not in tar.getnames():

                    info = tarfile.TarInfo(name=submission_yaml_fname)
                    info.size = submission_yaml.getbuffer().nbytes
                    info.mtime = time.mktime(submission["submitted_on"].timetuple())

                    # Add file in tar archive
                    tar.addfile(info, fileobj=submission_yaml)

                    # If there is an archive, add it too
                    if 'archive' in submission and submission['archive'] is not None and submission['archive'] != "":
                        subfile = self._gridfs.get(submission['archive'])
                        subtar = tarfile.open(fileobj=subfile, mode="r:gz")

                        for member in subtar.getmembers():
                            subtarfile = subtar.extractfile(member)
                            member.name = base_path + str(submission["_id"]) + "/archive/" + member.name
                            tar.addfile(member, subtarfile)

                        subtar.close()
                        subfile.close()

                    # If there files that were uploaded by the student, add them
                    if submission['input'] is not None:
                        for pid, problem in submission['input'].items():
                            # If problem is a dict, it is a file (from the specification of the problems)
                            if isinstance(problem, dict):
                                # Get the extension (match extensions with more than one dot too)
                                DOUBLE_EXTENSIONS = ['.tar.gz', '.tar.bz2', '.tar.bz', '.tar.xz']
                                ext = ""
                                if not problem['filename'].endswith(tuple(DOUBLE_EXTENSIONS)):
                                    _, ext = os.path.splitext(problem['filename'])
                                else:
                                    for t_ext in DOUBLE_EXTENSIONS:
                                        if problem['filename'].endswith(t_ext):
                                            ext = t_ext

                                subfile = io.BytesIO(problem['value'])
                                taskfname = base_path + str(submission["_id"]) + '/uploaded_files/' + pid + ext

                                # Generate file info
                                info = tarfile.TarInfo(name=taskfname)
                                info.size = subfile.getbuffer().nbytes
                                info.mtime = time.mktime(submission["submitted_on"].timetuple())

                                # Add file in tar archive
                                tar.addfile(info, fileobj=subfile)

        # Close tarfile and put tempfile cursor at 0
        tar.close()
        tmpfile.seek(0)
        return tmpfile

    def _handle_ssh_callback(self, submission_id, host, port, password):
        """ Handles the creation of a remote ssh server """
        if host is not None:  # ignore late calls (a bit hacky, but...)
            obj = {
                "ssh_host": host,
                "ssh_port": port,
                "ssh_password": password
            }
            self._database.submissions.update_one({"_id": submission_id}, {"$set": obj})

    def get_job_queue(self):
        """ Get information about the waiting/being processed jobs

        :returns: A list of dict in the form {
            "_id": ObjectId,
            "status": "waiting" | "processing",
            "courseid": str,
            "taskid": str,
            "grading_step_done_by": list[str],
            "grading_steps": list[str],
            "grading_step_last_update": datetime.datetime,
            "next_grading_step_idx": int,
            "next_grading_step": str,
            "username": list[str],
            "submitted_on": datetime.datetime
        }, sorted by increasing "submitted_on".
        """
        return list(self._database.submissions.find({"status": {"$in": ["waiting", "processing"]}},
                                                    ["_id", "status", "courseid", "taskid", "grading_step_done_by",
                                                     "grading_steps", "grading_step_last_update", "next_grading_step",
                                                     "next_grading_step_idx", "username", "submitted_on"],
                                                    sort=[("submitted_on", 1)]))


def update_pending_jobs(database):
    """ Updates pending jobs status in the database """

    # Updates the submissions that are waiting with the status error, as the server restarted
    database.submissions.update({'status': 'waiting'},
                                {"$unset": {'jobid': ""},
                                 "$set": {'status': 'error', 'grade': 0.0, 'text': 'Internal error. Server restarted'}}, multi=True)