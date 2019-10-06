import string
from datetime import datetime
from io import StringIO

import colorama


class FormatRenderer:

    def __init__(self, fmt: str, colors: bool = True, force_colors: bool = False):
        if colors is True:
            if force_colors:
                colorama.deinit()
                colorama.init(strip=False)
            else:
                colorama.init()

            self._level_to_color = {
                "critical": colorama.Fore.RED,
                "exception": colorama.Fore.RED,
                "error": colorama.Fore.RED,
                "warn": colorama.Fore.YELLOW,
                "warning": colorama.Fore.YELLOW,
                "info": colorama.Fore.GREEN,
                "debug": colorama.Fore.WHITE,
                "notset": colorama.Back.RED,
            }

            self._reset = colorama.Style.RESET_ALL
        else:
            self._level_to_color = {
                "critical": '',
                "exception": '',
                "error": '',
                "warn": '',
                "warning": '',
                "info": '',
                "debug": '',
                "notset": '',
            }

            self._reset = ''

        self._vformat = string.Formatter().vformat
        self._fmt = fmt

    def __call__(self, _, __, event_dict):
        message = StringIO()

        event_dict['log_color_reset'] = self._reset

        if 'level' in event_dict:
            level = event_dict['level']
            if level in self._level_to_color:
                event_dict['log_color'] = self._level_to_color[level]
            else:
                event_dict['log_color'] = ''
            event_dict['level_uc'] = level.upper()
        else:
            event_dict['log_color'] = ''

        if 'timestamp' in event_dict:
            event_dict['timestamp_local_ctime'] = datetime.fromtimestamp(event_dict['timestamp']).ctime()

        message.write(self._vformat(self._fmt, [], event_dict))

        stack = event_dict.pop("stack", None)
        exception = event_dict.pop("exception", None)

        if stack is not None:
            message.write("\n" + stack)
        if exception is not None:
            message.write("\n" + exception)

        message.write(self._reset)

        return message.getvalue()
