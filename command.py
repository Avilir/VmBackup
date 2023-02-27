#!/usr/bin/env python
import os.path
# Built-in modules
import subprocess

# 3ed party modules

# Local modules
import constnts
from logger import log


def run(cmd, do_log=True, timeout=600, out_format="string", **kwargs):
    """
    Running command on the OS and return the STDOUT & STDERR outputs
    in case of argument is not string or list, return error message
    Args:
        cmd (str/list): the command to execute
        do_log (bool): if True - log the error (if exist) to the file and add to mail message
        timeout (int): the command timeout in seconds, default is 10 Min.
        out_format (str): in which format to return the output: string / list
        kwargs (dict): dictionary of argument as subprocess get
    Returns:
        list or str : all STDOUT and STDERR output as list of lines, or one string separated by NewLine
                      in case of failure, return False
    """

    if isinstance(cmd, str):
        command = cmd.split()
    elif isinstance(cmd, list):
        command = cmd
    else:
        return "Error in command"

    for key in ["stdout", "stderr", "stdin"]:
        kwargs[key] = subprocess.PIPE

    if "out_format" in kwargs:
        out_format = kwargs["out_format"]
        del kwargs["out_format"]

    if do_log:
        log(f"Going to format output as {out_format}")
        log(f"Going to run {cmd} with timeout of {timeout} seconds")
    try:
        cp = subprocess.run(command, timeout=timeout, **kwargs)
    except Exception as e:
        if do_log:
            log(f"Failed to run the command : {' '.join(command)}")
            log(f"Exit with exception : {e}")
        return ""

    output = cp.stdout.decode()
    err = cp.stderr.decode()
    # exit code is not zero
    if cp.returncode:
        if do_log:
            log(f"Command finished with non zero ({cp.returncode}): {err}")
        output += f"Error in command ({cp.returncode}): {err}"

        if out_format == "rc":
            return cp.returncode

        return ""

    # TODO: adding more output_format types : json / yaml

    if out_format in ["list", "lastline"]:
        output = output.split("\n")  # convert output to list
        if len(output) > 1:
            output.pop()  # remove last empty element from the list

    if out_format == "lastline":
        output = output[-1]

    return output


def run_xe(cmd, out_format="lastline"):
    command = os.path.join(constnts.xe_path, 'xe')
    return run(f"{command} {cmd}", do_log=False, out_format=out_format)


if __name__ == "__main__":
    print(run("kuku", out_format="string"))
    print(run("ls -l", do_log=False, out_format="lastline"))
