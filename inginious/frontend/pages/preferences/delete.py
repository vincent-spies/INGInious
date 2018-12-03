# -*- coding: utf-8 -*-
#
# This file is part of INGInious. See the LICENSE and the COPYRIGHTS files for
# more information about the licensing of this file.

""" Profile page """
from inginious.frontend.pages.utils import INGIniousAuthPage
from inginious.frontend.web_utils import see_other_exception, not_found_exception, webinput


class DeletePage(INGIniousAuthPage):
    """ Delete account page for DB-authenticated users"""

    def delete_account(self, data):
        """ Delete account from DB """
        error = False
        msg = ""

        username = self.user_manager.session_username()

        # Check input format
        result = self.database.users.find_one_and_delete({"username": username,
                                                          "email": data.get("delete_email", "")})
        if not result:
            error = True
            msg = _("The specified email is incorrect.")
        else:
            self.database.submissions.remove({"username": username})
            self.database.user_tasks.remove({"username": username})

            all_courses = self.course_factory.get_all_courses()

            for courseid, course in all_courses.items():
                if self.user_manager.course_is_open_to_user(course, username):
                    self.user_manager.course_unregister_user(course, username)

            self.user_manager.disconnect_user()
            raise see_other_exception("/index")

        return msg, error

    def GET_AUTH(self):  # pylint: disable=arguments-differ
        """ GET request """
        userdata = self.database.users.find_one({"username": self.user_manager.session_username()})

        if not userdata or not self.app.allow_deletion:
            raise not_found_exception()

        return self.template_helper.get_renderer().preferences.delete("", False)

    def POST_AUTH(self):  # pylint: disable=arguments-differ
        """ POST request """
        userdata = self.database.users.find_one({"username": self.user_manager.session_username()})

        if not userdata or not self.app.allow_deletion:
            raise not_found_exception()

        msg = ""
        error = False
        data = webinput()
        if "delete" in data:
            msg, error = self.delete_account(data)

        return self.template_helper.get_renderer().preferences.delete(msg, error)
