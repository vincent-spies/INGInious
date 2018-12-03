import web

def config_mail(host, port, use_starttls, username, password, sendername):
    web.config.smtp_server = host
    web.config.smtp_port = port
    web.config.smtp_starttls = use_starttls
    web.config.smtp_username = username
    web.config.smtp_password = password
    web.config.smtp_sendername = sendername

def sendmail(from_address, to_address, subject, message, headers=None, **kw):
    """
        Sends the email message `message` with mail and envelope headers
        for from `from_address_` to `to_address` with `subject`.
        Additional email headers can be specified with the dictionary
        `headers.

        Optionally cc, bcc and attachments can be specified as keyword arguments.
        Attachments must be an iterable and each attachment can be either a
        filename or a file object or a dictionary with filename, content and
        optionally content_type keys.

        If `web.config.smtp_server` is set, it will send the message
        to that SMTP server. Otherwise it will look for
        `/usr/sbin/sendmail`, the typical location for the sendmail-style
        binary. To use sendmail from a different path, set `web.config.sendmail_path`.
    """
    return web.sendmail(from_address, to_address, subject, message, headers=None, **kw)

def get_smtp_sendername():
    return web.config.smtp_sendername