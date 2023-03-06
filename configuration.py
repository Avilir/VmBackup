#!/usr/bin/env python3

# Built-in modules
import datetime
import re
import shutil
import smtplib
import socket
import sys
import time

# 3ed party modules
from email.mime.text import MIMEText
import XenAPI

# Local modules
import argument
from command import run, run_df, run_xe, check_if_vm_is_running, destroy_vdi_snapshot
from constnts import *
from logger import log, message


expected_keys = [
    "pool_db_backup",
    "max_backups",
    "backup_dir",
    "status_log",
    "vdi_export_format",
    "vm-export",
    "vdi-export",
    "exclude",
]


class Config:
    def __init__(self):
        self._data = {}
        self._filename = ""
        self.defaults()

    def filename(self, name=""):
        if name != "":
            self._filename = name
        return self._filename

    def data(self, key="", value=""):
        if key == "":
            return
        if key not in self._data:
            self._data[key] = value
            return value
        if value != "":
            if isinstance(self._data[key], str):
                self._data[key] = value
            elif isinstance(self._data[key], list):
                self._data[key].append(value)

        return self._data[key]

    def remove_data_list(self, key, value):
        if not isinstance(self._data[key], list):
            return
        self._data[key].remove(value)

    def defaults(self):
        # init config param not already loaded then load with default values
        self.data("pool_db_backup", int(DEFAULT_POOL_DB_BACKUP))
        self.data("max_backups", int(DEFAULT_MAX_BACKUPS))
        self.data("vdi_export_format", str(DEFAULT_VDI_EXPORT_FORMAT))
        self.data("backup_dir", str(DEFAULT_BACKUP_DIR))
        self.data("status_log", str(DEFAULT_STATUS_LOG))
        self.data("compress", False)
        self.data("ignore_extra_keys", False)
        self.data("exclude", [])
        self.data("vdi-export", [])
        self.data("vm-export", [])
        self.data("all_vms", [])

    def print(self):
        log("VmBackup.py running with these settings:")
        log(f"  backup_dir        = {self.data('backup_dir')}")
        log(f"  status_log        = {self.data('status_log')}")
        log(f"  compress          = {self.data('compress')}")
        log(f"  max_backups       = {self.data('max_backups')}")
        log(f"  vdi_export_format = {self.data('vdi_export_format')}")
        log(f"  pool_db_backup    = {self.data('pool_db_backup')}")

        for key in ["exclude", "vdi-export", "vm-export"]:
            log(f"  {key} (cnt)= {len(self.data(key))}")
            log(f"  {key}: {','.join(sorted(self.data(key)))}")

    def load(self):
        """ """
        return_value = True
        if not os.path.exists(self.filename()):
            return False

        with open(self.filename(), "r") as fh:
            line = fh.readline().strip()
            if not line.startswith("#") and len(line) > 0:
                (key, value) = line.split("=")
                key = key.strip()
                value = value.strip()

                # check for valid keys
                if key not in expected_keys:
                    if self.data("ignore_extra_keys"):
                        log("ignoring config key: %s" % key)
                    else:
                        print("***ERROR unexpected config key: %s" % key)
                        return_value = False

                if key == "exclude":
                    self.add_exclude(key, value)
                elif key in ["vm-export", "vdi-export"]:
                    self.add_export(key, value)
                else:
                    # all other key's
                    self.data(key, value)

        return return_value

    def add_export(self, key, value):
        if key == "vdi-export" and value == "":
            return

        values = value.split(":")
        vm_name = values[0]
        vm_backups_part = values[1] if len(values) > 1 else ""

        for vm in self.data("all_vms"):
            normal_name = re.match("^[\w\s\-\_]+$", vm_name) is not None
            if (normal_name and vm_name == vm) or (
                not normal_name and re.match(vm_name, vm)
            ):
                if vm_backups_part == "":
                    new_value = vm
                else:
                    new_value = f"{vm}:{vm_backups_part}"
                found_match = True
                # Check if vdi-export already has the vm mentioned and, if so, do not add this vm to vm-export
                if key == "vm-export" and vm in self._data["vdi-export"]:
                    continue
                else:
                    self.data(key, new_value)

        if not found_match:
            log(f"***WARNING - vm not found: {key}={value}")
            warning_match = True

    def add_exclude(self, key, vm_name):
        # save key/value in config[]
        # expected-key: exclude
        # expected-value: vmname (with or w/o regex)
        global warning_match
        global error_regex
        found_match = False
        # Fail fast if exclude param given but empty to prevent from exluding all VMs
        if vm_name == "":
            return
        normal_name = re.match("^[\w\s\-\_]+$", vm_name) is not None
        if not normal_name and not re.compile(vm_name):
            log(f"***ERROR - invalid regex: {key}={vm_name}")
            error_regex = True
            return
        for vm in self.data("all_vms"):
            if (normal_name and vm_name == vm) or (
                not normal_name and re.match(vm_name, vm)
            ):
                found_match = True
                self.data(key, vm)
                self._data["all_vms"].remove(vm)

        if not found_match:
            log(f"***WARNING - vm not found: {key}={vm_name}")
            warning_match = True

    def is_valid(self):
        for int_keys in ["pool_db_backup", "max_backups"]:
            if not isinstance(self.data(int_keys), int):
                return self._print_config_error(int_keys, "non-numeric")

        if int(self.data("max_backups")) < 1:
            return self._print_config_error("max_backups", "out of range")

        if int(self.data("pool_db_backup")) < 0 or int(self.data("pool_db_backup")) > 1:
            return self._print_config_error("pool_db_backup", "out of range")

        if self.data("vdi_export_format") not in ["raw", "vhd"]:
            return self._print_config_error("vdi_export_format", "invalid")

        if not os.path.exists(self.data("backup_dir")):
            return self._print_config_error("backup_dir", "does not exist")

        tmp_return = True
        if len(self.data("vm-export")) == 0 and len(self.data("vdi-export")) == 0:
            log("ERROR no VMs loaded")
            return False

        for key in ["vdi-export", "vm-export"]:
            for vm_parm in config.data(key):
                data = vm_parm.split(":")
                if not (len(data) > 1) and isinstance(data[1], int) and (data[1] > 0):
                    print(f"ERROR: vm_max_backup is invalid - {vm_parm}")
                    tmp_return = False

        return tmp_return

    def _print_config_error(self, key, msg):
        print(f"ERROR: config {key} {msg} -> {self.data(key)}")
        return False


if __name__ == "__main__":
    config = Config()
    # config = RawConfigParser(dict_type = OrderedDict)
    # config.read(["example.cfg"])
    # print (config.get("test",  "foo"))
    # print (config.get("test",  "xxx"))

    # config2 = ConfigParser(dict_type=OrderedMultisetDict)
    # config2.read(["example.cfg"])
    # print(dict(config2.items("defaults")))
    print(f"All VM's are : {','.join(config.data('all_vms'))}")
