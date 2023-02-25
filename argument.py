#!/usr/bin/env python
"""
This module contain a class for argument parser, check the arguments and
verify the arguments data
"""
# Built-in modules
import argparse
import base64
import os
import sys

# 3ed party Modules

# Internal modules


class Arguments:
    def __init__(self):
        """
        Initialize the object, add the arguments and pars the CLI
        """
        self.parser = argparse.ArgumentParser(add_help=True)
        self._build()
        self.args = self.args = self.parser.parse_args()

    def _build(self):
        """
        This function define the program arguments in the command line
        """
        # ==================  Help arguments
        self.parser.add_argument(
            "--config",
            action="store_true",
            default=False,
            help="for config-file parameter usage",
        )
        self.parser.add_argument(
            "--example",
            action="store_true",
            default=False,
            help="for additional parameter usage",
        )

        # ==================  Some boolean arguments
        self.parser.add_argument(
            "--preview",
            action="store_true",
            default=False,
            help="preview/validate VmBackup config parameters and xen-server password (default: False)",
        )
        self.parser.add_argument(
            "--compress",
            action="store_true",
            default=False,
            help="only for vm-export functions automatic compression (default: False)",
        )
        self.parser.add_argument(
            "--ignore_extra_keys",
            action="store_true",
            default=False,
            help="some config files may have extra params (default: False)",
        )
        self.parser.add_argument(
            "--pre_clean",
            action="store_true",
            default=False,
            help="delete older backup(s) before performing new backup (default: False)",
        )

        # ==================  Password arguments
        passwords_group = self.parser.add_mutually_exclusive_group(required=True)
        passwords_group.add_argument(
            "-p",
            "--password",
            help="xen server password",
        )
        passwords_group.add_argument(
            "--password-file",
            help="file name to store the obscured password",
        )

        # ==================  Configuration arguments
        self.parser.add_argument(
            "--config-file",
            help="a common choice for production crontab execution",
        )
        self.parser.add_argument(
            "--vm-selector",
            help="a single vm name or a vm regular expression that defaults to vm-export",
        )

    def help_check(self):
        """
        Function to check if a Help argument was passed, and display the
        appropriate help screen and exit the program.

        """
        if self.args.config or self.args.example:
            if self.args.config:
                print("Debugging - display the configuration")
                pass
                # TODO: update after creating the usage module
                # usage.usage_config_file()
            if self.args.example:
                print("Debugging - display the example")
                pass
                # TODO: update after creating the usage module
                # usage.usage_examples()
            sys.exit(1)

    def get_password(self):
        """
        Function that return the XEN server password, from the CLI or from a file.
        In the file, the password is encoded.
        If it needs to get the password from a file and the file doesn't exist, it
        exits the program with an error message.

        Return:
            str : the decoded password for the XEN server
        """
        if self.args.password is not None:
            return self.args.password
        # At this point, we must read the password from the file
        if os.path.exists(self.args.password_file):
            with open(self.args.password_file, "rb") as fh:
                data = fh.read()
                password = base64.b64decode(data).decode("UTF-8")
            return password
        else:
            print(f"Error: password file ({self.args.password_file}) doesn't exist !")
            self.parser.print_help()
            exit(1)


if __name__ == "__main__":
    args = Arguments()
    args.help_check()
    print(f"The password is {args.get_password()}")
