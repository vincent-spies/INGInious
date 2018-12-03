import web

def get_client_ip():
    return webdict().ip

def webdict():
    """
    Fields:
    - ip
    - home
    - fullpath
    """
    return web.ctx

def webenv():
    return web.ctx.env

def template():
    return web.template

def not_found_exception(msg=None):
    return web.notfound(msg)

def see_other_exception(redirect_to):
    return web.seeother(redirect_to)

def redirect_exception(redirect_to):
    return web.redirect(redirect_to)

def not_acceptable_exception():
    return web.notacceptable()

def forbidden_exception(msg=None):
    return web.forbidden(msg)

def webinput(what="both", raw=False):
    """ If what is "both", returns all input from GET and POST.
    If what is POST or GET, returns only the data from post or get."""
    if what is "both":
        return webinput() if not raw else web.webapi.rawinput()
    else:
        return web.input(what) if not raw else web.webapi.rawinput(what)

def add_header(header, value, unique=False):
    web.header(header, value)

def websafe(string):
    return web.websafe(string)