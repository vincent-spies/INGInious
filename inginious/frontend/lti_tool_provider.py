# coding=utf-8
from lti import ToolProvider

from inginious.frontend.web_utils import webdict, webinput


class LTIWebPyToolProvider(ToolProvider):
    '''
    ToolProvider that works with Web.py requests
    '''

    @classmethod
    def from_webpy_request(cls, secret=None):
        params = webinput(_method="POST", _raw=True)
        headers = webdict().env.copy()

        headers = dict([(k, headers[k])
                        for k in headers if
                        k.upper().startswith('HTTP_') or
                        k.upper().startswith('CONTENT_')])

        url = webdict().home + webdict().fullpath
        return cls.from_unpacked_request(secret, params, url, headers)
