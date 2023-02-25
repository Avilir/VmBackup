#!/usr/bin/env python3

# Built-in modules
import os

# 3ed party modules

# Local modules

# ############################ HARD CODED DEFAULTS
# modify these hard coded default values, only used if not specified in config file
DEFAULT_POOL_DB_BACKUP = 0
DEFAULT_MAX_BACKUPS = 4

# xe vdi-export options: 'raw' or 'vhd'
DEFAULT_VDI_EXPORT_FORMAT = "raw"

# For paths on Linux & Windows systems
DEFAULT_BACKUP_DIR = os.path.join(os.sep, "snapshots", "BACKUPS")
DEFAULT_STATUS_LOG = os.path.join(os.sep, "snapshots", "logs", "status.log")

# ############################ OPTIONAL
# optional email may be triggered by configure next 3 parameters then find MAIL_ENABLE
# and uncommenting out the desired lines
MAIL_TO_ADDR = "your-email@your-domain"
# note if MAIL_TO_ADDR has ipaddr then you may need to change the smtplib.SMTP() call
MAIL_FROM_ADDR = "your-from-address@your-domain"
MAIL_SMTP_SERVER = "your-mail-server"

xe_path = os.path.join(os.sep, "opt", "xensource", "bin")


if __name__ == "__main__":
    print(f"The DEFAULT_BACKUP_DIR is {DEFAULT_BACKUP_DIR}")
    print(f"The DEFAULT_STATUS_LOG is {DEFAULT_STATUS_LOG}")
    print(f"The xe_path is {xe_path}")
