# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.
import logging

import asyncio
import multiprocessing
import threading

import motor.motor_asyncio as motor
from inginious.agent.docker_agent import DockerAgent
from inginious.agent.mcq_agent import MCQAgent

from inginious.new_agent import GradingUnit, Agent

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
        await asyncio.sleep(600)
        return {"result": "success", "text": self.submission.get("text", "") + "Processed by {}\n".format(self.name)}
create_gudemo = lambda name: (lambda a,b,c: GUDemo(a,b,c,name))

def start_asyncio(debug_asyncio=False):
    """ Init asyncio. Starts a daemon thread in which the asyncio loops run.
    :return: a tuple (asyncio loop object, Thread)
    """
    loop = asyncio.get_event_loop()
    if debug_asyncio:
        loop.set_debug(True)

    t = threading.Thread(target=_run_asyncio, args=(loop,), daemon=True)
    t.start()

    return loop, t

def _run_asyncio(loop):
    """
    Run asyncio (should be called in a thread) and close the loop when the thread ends
    :param loop:
    :return:
    """
    try:
        asyncio.set_event_loop(loop)
        loop.run_forever()
    except:
        pass
    finally:
        loop.close()

def create_arch(configuration, database, tasks_fs, loop):
    """ Helper that can start a simple complete INGInious arch locally if needed, or a client to a remote backend.
        Intended to be used on command line, makes uses of exit() and the logger inginious.frontend.
    :param configuration: configuration dict
    :param database: AsyncIOMotorDatabase object
    :param tasks_fs: FileSystemProvider to the courses/tasks folders
    :param loop: an asyncio loop
    :return: a list of asyncio Tasks
    """
    logger = logging.getLogger("inginious.frontend")

    tasks = []

    start_local_agents = configuration.get("start_local_agents", True)
    if start_local_agents:
        logger.info("Starting a simple arch (docker-agent and mcq-agent) locally")

        local_config = configuration.get("local-config", {})
        concurrency = local_config.get("concurrency", multiprocessing.cpu_count())
        debug_host = local_config.get("debug_host", None)
        debug_ports = local_config.get("debug_ports", None)
        tmp_dir = local_config.get("tmp_dir", "./agent_tmp")

        if debug_ports is not None:
            try:
                debug_ports = debug_ports.split("-")
                debug_ports = range(int(debug_ports[0]), int(debug_ports[1]))
            except:
                logger.error("debug_ports should be in the format 'begin-end', for example '1000-2000'")
                exit(1)
        else:
            debug_ports = range(64100, 64111)

        agent1 = Agent(database, "test1", None, {"a": create_gudemo("test1-a"), "b": create_gudemo("test1-b")})
        agent2 = Agent(database, "test2", None, {"c": create_gudemo("test2-c"), "b": create_gudemo("test2-b")})

        loop.call_soon_threadsafe(lambda: loop.create_task(agent1.run()))
        loop.call_soon_threadsafe(lambda: loop.create_task(agent2.run()))

        #agent_docker = DockerAgent("Docker - Local agent", concurrency, tasks_fs, debug_host, debug_ports, tmp_dir)
        #agent_mcq = MCQAgent("MCQ - Local agent", 1, tasks_fs)

        #tasks.append(asyncio.ensure_future(agent_docker.run()))
        #tasks.append(asyncio.ensure_future(agent_mcq.run()))

    # check for old-style configuration entries
    old_style_configs = ["backend", "agents", 'containers', "machines", "docker_daemons"]
    for c in old_style_configs:
        if c in configuration:
            logger.warning("Option %s in configuration.yaml is not used anymore.\n"
                           "Have a look at the 'update' section of the INGInious documentation in order to upgrade your configuration.yaml", c)

    return tasks
