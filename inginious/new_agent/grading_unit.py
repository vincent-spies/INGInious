# coding=utf-8
from abc import ABC, abstractmethod


class GradingUnit(ABC):
    def __init__(self, submission, send_message, agent):
        """
        :param submission: the submission, including input.
        :param send_message: a coroutine that takes a single arg: the payload to be sent to the frontend.
        :param agent: the agent that created this GradingUnit
        """
        self.submission = submission
        self.agent = agent
        self.send_message = send_message

    @abstractmethod
    async def init(self):
        """ Init the GradingUnit, with async possibilities. This is the first function to be called. """
        pass

    @abstractmethod
    async def kill(self):
        """ Kill the submission. Once this function has been called, grade() should end in a reasonably short time.
            Can be called before grade(). In this case, grade() should also end shortly.

            After the GradingUnit has been killed, it should avoid sending messages to the frontend.
        """
        pass

    @abstractmethod
    async def message(self, message):
        """ Process a message. Can be called before or during grade(). The content is defined by plugins.
            The message() function can send message to the frontend using self.send_message()
        """
        pass

    @abstractmethod
    async def grade(self):
        """ Grade the submission. Returns a dictionary in the form {
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

            The grade() function can send message to the frontend using self.send_message()
        """
        pass