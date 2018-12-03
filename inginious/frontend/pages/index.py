# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Index page """
from inginious.frontend.pages.utils import INGIniousStaticPage
from inginious.frontend.web_utils import see_other_exception


class IndexPage(INGIniousStaticPage):
    """ Index page """

    def GET(self):  # pylint: disable=arguments-differ
        """ Display main course list page """
        if not self.app.welcome_page:
            raise see_other_exception("/courselist")
        return self.show_page(self.app.welcome_page)

    def POST(self):  # pylint: disable=arguments-differ
        """ Display main course list page """
        if not self.app.welcome_page:
            raise see_other_exception("/courselist")
        return self.show_page(self.app.welcome_page)
