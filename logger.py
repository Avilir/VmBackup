#!/usr/bin/env python

# Built-in modules
from datetime import datetime
import sys

# 3ed party modules

# Local modules

message = ""


def log(mes, log_w_timestamp=True):
    # note - send_email uses message
    global message

    date = datetime.today().strftime("%y-%m-%d-(%H:%M:%S)")
    if log_w_timestamp:
        str = f"{date} - {mes}"
    else:
        str = mes
    message = f"{message}{str}\n"

    str = str.rstrip()
    print(str)
    sys.stdout.flush()
    sys.stderr.flush()


if __name__ == "__main__":
    log("foo")
