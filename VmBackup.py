#!/usr/bin/env python

"""
Avilir/VmBackup.py

V0.01 February 2023

This is a fork of NAUbackup/VmBackup.py

The Intention of this form is to make this script python3 compatible,
run from client which is not the Xen-Server and also containerized.

Copyright (C) 2023  Avi Liani - <avi@liani.co.il>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Title: Avilir/VmBackup - a XenServer vm-export & vdi-export Backup Script
Package Contents: README.md, VmBackup.py (this file), example.cfg

Version History
    V0.01 - python3 compatible

** DO NOT RUN THIS SCRIPT UNLESS YOU ARE COMFORTABLE WITH THESE ACTIONS. **
=> To accomplish the vm backup this script uses the following xe commands
  vm-export:  (a) vm-snapshot, (b) template-param-set, (c) vm-export, (d) vm-uninstall on vm-snapshot
  vdi-export: (a) vdi-snapshot, (b) vdi-param-set, (c) vdi-export, (d) vdi-destroy on vdi-snapshot

See README for usage and installation documentation.
See example.cfg for config file example usage.

Usage w/ vm name for single vm backup, which runs vm-export by default:
   ./VmBackup.py <password> <vm-name>

Usage w/ config file for multiple vm backups, where you can specify either vm-export or vdi-export:
   ./VmBackup.py <password> <config-file-path>
"""

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
from command import run
from constnts import *
from logger import log, message


config = {}
all_vms = []
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


def main(session):
    success_cnt = 0
    warning_cnt = 0
    error_cnt = 0

    # setting autoflush on (aka unbuffered)
    sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)

    server_name = os.uname()[1].split(".")[0]
    if config_specified:
        status_log_begin(server_name)

    log("===========================")
    log(f"VmBackup running on {server_name} ...")

    log("===========================")
    log(f"Check if backup directory {config['backup_dir']} is writable ...")
    touchfile = os.path.join(config["backup_dir"], "00VMbackupWriteTest")

    cmd = f"/bin/touch {touchfile}"
    log(cmd)
    res = run(cmd)
    if res == "":
        log("ERROR failed to write to backup directory area - FATAL ERROR")
        sys.exit(1)
    else:
        cmd = f'/bin/rm -f "{touchfile}"'
        res = run(cmd)
        log("Success: backup directory area is writable")

    log("===========================")
    df_snapshots(f"Space before backups: df -Th {config['backup_dir']}")

    if int(config["pool_db_backup"]):
        log("*** begin backup_pool_metadata ***")
        if not backup_pool_metadata(server_name):
            error_cnt += 1

    ######################################################################
    # Iterate through all vdi-export= in cfg
    log("************ vdi-export= ***************")
    for vm_parm in config["vdi-export"]:
        log(f"*** vdi-export begin {vm_parm}")
        beginTime = datetime.datetime.now()
        this_status = "success"

        # get values from vdi-export=
        vm_name = get_vm_name(vm_parm)
        vm_max_backups = get_vm_max_backups(vm_parm)
        log(f"vdi-export - vm_name: {vm_name} max_backups: {vm_max_backups}")

        if config_specified:
            status_log_vdi_export_begin(server_name, vm_name)

        # verify vm_name exists with only one instance for this name
        #  returns error-message or vm_object if success
        vm_object = verify_vm_name(vm_name)
        if "ERROR" in vm_object:
            log(f"verify_vm_name: {vm_object}")
            if config_specified:
                status_log_vdi_export_end(
                    server_name, f"ERROR verify_vm_name {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue

        vm_backup_dir = os.path.join(config["backup_dir"], vm_name)
        # cleanup any old unsuccessful backups and create new full_backup_dir
        full_backup_dir = process_backup_dir(vm_backup_dir)

        # gather_vm_meta produces status: empty or warning-message
        #   and globals: vm_uuid, xvda_uuid, xvda_uuid
        #   => now only need: vm_uuid
        #   since all VM metadta go into an XML file
        vm_meta_status = gather_vm_meta(vm_object, full_backup_dir)
        if vm_meta_status != "":
            log(f"WARNING gather_vm_meta: {vm_meta_status}")
            this_status = "warning"
            # non-fatal - finsh processing for this vm

        # vdi-export only uses xvda_uuid, xvda_uuid
        if xvda_uuid == "":
            log("ERROR gather_vm_meta has no xvda-uuid")
            if config_specified:
                status_log_vdi_export_end(
                    server_name, f"ERROR xvda-uuid not found {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue
        if xvda_name_label == "":
            log("ERROR gather_vm_meta has no xvda-name-label")
            if config_specified:
                status_log_vdi_export_end(
                    server_name, f"ERROR xvda-name-label not found {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue

        # -----------------------------------------
        # --- begin vdi-export command sequence ---
        log("*** vdi-export begin xe command sequence")
        # is vm currently running?
        cmd = f'{xe_path}/xe vm-list name-label="{vm_name}" params=power-state | /bin/grep running'
        if run(cmd, log_w_timestamp=False, out_format="rc") == 0:
            log("vm is running")
        else:
            log("vm is NOT running")

        # list the vdi we will backup
        cmd = f"{xe_path}/xe vdi-list uuid={xvda_uuid}"
        log(f"1.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-LIST-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # check for old vdi-snapshot for this xvda
        snap_vdi_name_label = f"SNAP_{vm_name}_{xvda_name_label}"
        # replace all spaces with '-'
        snap_vdi_name_label = re.sub(r" ", r"-", snap_vdi_name_label)
        log(f"check for prev-vdi-snapshot: {snap_vdi_name_label}")
        cmd = (
            f"{xe_path}/xe vdi-list name-label='{snap_vdi_name_label}' params=uuid |"
            + " /bin/awk -F': ' '{print $2}' | /bin/grep '-'"
        )
        old_snap_vdi_uuid = run(cmd, do_log=False, out_format="lastline")
        if old_snap_vdi_uuid != "":
            log(f"cleanup old-snap-vdi-uuid: {old_snap_vdi_uuid}")
            # vdi-destroy old vdi-snapshot
            cmd = f"{xe_path}/xe vdi-destroy uuid={old_snap_vdi_uuid}"
            log(f"cmd: {cmd}")
            if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
                log(f"WARNING {cmd}")
                this_status = "warning"
                # non-fatal - finish processing for this vm

        # === pre_cleanup code goes in here ===
        if arg.is_pre_clean():
            pre_cleanup(vm_backup_dir, vm_max_backups)

        # take a vdi-snapshot of this vm
        cmd = f"{xe_path}/xe vdi-snapshot uuid={xvda_uuid}"
        log(f"2.cmd: {cmd}")
        snap_vdi_uuid = run(cmd, do_log=False, out_format="lastline")
        log(f"snap-uuid: {snap_vdi_uuid}")
        if snap_vdi_uuid == "":
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-SNAPSHOT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # change vdi-snapshot to unique name-label for easy id and cleanup
        cmd = f'{xe_path}/xe vdi-param-set uuid={snap_vdi_uuid} name-label="{snap_vdi_name_label}"'
        log(f"3.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-PARAM-SET-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # actual-backup: vdi-export vdi-snapshot
        cmd = f"{xe_path}/xe vdi-export format={config['vdi_export_format']} uuid={snap_vdi_uuid}"
        full_path_backup_file = os.path.join(
            full_backup_dir, vm_name + f'.config["vdi_export_format"]'
        )
        cmd = f'{cmd} filename="{full_path_backup_file}"'
        log(f"4.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") == 0:
            log("vdi-export success")
        else:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-EXPORT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # cleanup: vdi-destroy vdi-snapshot
        cmd = f"{xe_path}/xe vdi-destroy uuid={snap_vdi_uuid}"
        log(f"5.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
            log(f"WARNING {cmd}")
            this_status = "warning"
            # non-fatal - finsh processing for this vm

        log("*** vdi-export end")
        # --- end vdi-export command sequence ---
        # ---------------------------------------

        elapseTime = datetime.datetime.now() - beginTime
        backup_file_size = os.path.getsize(full_path_backup_file) / (1024 * 1024 * 1024)
        final_cleanup(
            full_path_backup_file,
            backup_file_size,
            full_backup_dir,
            vm_backup_dir,
            vm_max_backups,
        )

        if not check_all_backups_success(vm_backup_dir):
            log("WARNING cleanup needed - not all backup history is successful")
            this_status = "warning"

        if this_status == "success":
            success_cnt += 1
            log(
                f"VmBackup vdi-export {vm_name} - ***Success*** t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vdi_export_end(
                    server_name,
                    f"SUCCESS {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

        elif this_status == "warning":
            warning_cnt += 1
            log(
                f"VmBackup vdi-export {vm_name} - ***WARNING*** t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vdi_export_end(
                    server_name,
                    f"WARNING {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

        else:
            # this should never occur since all errors do a continue on to the next vm_name
            error_cnt += 1
            log(
                f"VmBackup vdi-export {vm_name} - +++ERROR-INTERNAL+++ t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vdi_export_end(
                    server_name,
                    f"ERROR-INTERNAL {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

    # end of for vm_parm in config['vdi-export']:
    ######################################################################

    ######################################################################
    # Iterate through all vm-export= in cfg
    log("************ vm-export= ***************")
    for vm_parm in config["vm-export"]:
        log(f"*** vm-export begin {vm_parm}")
        beginTime = datetime.datetime.now()
        this_status = "success"

        # get values from vdi-export=
        vm_name = get_vm_name(vm_parm)
        vm_max_backups = get_vm_max_backups(vm_parm)
        log(f"vm-export - vm_name: {vm_name} max_backups: {vm_max_backups}")

        if config_specified:
            status_log_vm_export_begin(server_name, vm_name)

        vm_object = verify_vm_name(vm_name)
        if "ERROR" in vm_object:
            log(f"verify_vm_name: {vm_object}")
            if config_specified:
                status_log_vm_export_end(server_name, f"ERROR verify_vm_name {vm_name}")
            error_cnt += 1
            # next vm
            continue

        vm_backup_dir = os.path.join(config["backup_dir"], vm_name)
        # cleanup any old unsuccessful backups and create new full_backup_dir
        full_backup_dir = process_backup_dir(vm_backup_dir)

        # gather_vm_meta produces status: empty or warning-message
        #   and globals: vm_uuid, xvda_uuid, xvda_uuid
        vm_meta_status = gather_vm_meta(vm_object, full_backup_dir)
        if vm_meta_status != "":
            log(f"WARNING gather_vm_meta: {vm_meta_status}")
            this_status = "warning"
            # non-fatal - finsh processing for this vm
        # vm-export only uses vm_uuid
        if vm_uuid == "":
            log("ERROR gather_vm_meta has no vm-uuid")
            if config_specified:
                status_log_vm_export_end(
                    server_name, f"ERROR vm-uuid not found {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue

        # ----------------------------------------
        # --- begin vm-export command sequence ---
        log("*** vm-export begin xe command sequence")
        # is vm currently running?
        cmd = f'{xe_path}/xe vm-list name-label="{vm_name}" params=power-state | /bin/grep running'
        if run(cmd, log_w_timestamp=False, out_format="rc") == 0:
            log("vm is running")
        else:
            log("vm is NOT running")

        # check for old vm-snapshot for this vm
        snap_name = f"RESTORE_{vm_name}"
        log(f"check for prev-vm-snapshot: {snap_name}")
        cmd = (
            f"{xe_path}/xe vm-list name-label='{snap_name}' params=uuid | "
            + "/bin/awk -F': ' '{print $2}' | /bin/grep '-'"
        )
        old_snap_vm_uuid = run(cmd, do_log=False, out_format="lastline")
        if old_snap_vm_uuid != "":
            log(f"cleanup old-snap-vm-uuid: {old_snap_vm_uuid}")
            # vm-uninstall old vm-snapshot
            cmd = f"{xe_path}/xe vm-uninstall uuid={old_snap_vm_uuid} force=true"
            log(f"cmd: {cmd}")
            if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
                log(f"WARNING-ERROR {cmd}")
                this_status = "warning"
                if config_specified:
                    status_log_vm_export_end(
                        server_name, f"VM-UNINSTALL-FAIL-1 {vm_name}"
                    )
                # non-fatal - finsh processing for this vm

        # === pre_cleanup code goes in here ===
        # print(f'vm_backup_dir: {vm_backup_dir}'  )
        # print(f'vm_max_backups: {vm_max_backups}'  )
        if arg.is_pre_clean():
            pre_cleanup(vm_backup_dir, vm_max_backups)

        # take a vm-snapshot of this vm
        cmd = f'{xe_path}/xe vm-snapshot vm={vm_uuid} new-name-label="{snap_name}"'
        log(f"1.cmd: {cmd}")
        snap_vm_uuid = run(cmd, do_log=False, out_format="lastline")
        log(f"snap-uuid: {snap_vm_uuid}")
        if snap_vm_uuid == "":
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vm_export_end(server_name, f"SNAPSHOT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # change vm-snapshot so that it can be referenced by vm-export
        cmd = f"{xe_path}/xe template-param-set is-a-template=false ha-always-run=false uuid={snap_vm_uuid}"
        log(f"2.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vm_export_end(
                    server_name, f"TEMPLATE-PARAM-SET-FAIL {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue

        # vm-export vm-snapshot
        cmd = f"{xe_path}/xe vm-export uuid={snap_vm_uuid}"
        if arg.is_compress():
            full_path_backup_file = os.path.join(full_backup_dir, vm_name + ".xva.gz")
            cmd = f'{cmd} filename="{full_path_backup_file}" compress=true'
        else:
            full_path_backup_file = os.path.join(full_backup_dir, vm_name + ".xva")
            cmd = f'{cmd} filename="{full_path_backup_file}"'
        log(f"3.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") == 0:
            log("vm-export success")
        else:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vm_export_end(server_name, f"VM-EXPORT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # vm-uninstall vm-snapshot
        cmd = f"{xe_path}/xe vm-uninstall uuid={snap_vm_uuid} force=true"
        log(f"4.cmd: {cmd}")
        if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
            log(f"WARNING {cmd}")
            this_status = "warning"
            # non-fatal - finsh processing for this vm

        log("*** vm-export end")
        # --- end vm-export command sequence ---
        # ----------------------------------------

        elapseTime = datetime.datetime.now() - beginTime
        backup_file_size = os.path.getsize(full_path_backup_file) / (1024 * 1024 * 1024)
        final_cleanup(
            full_path_backup_file,
            backup_file_size,
            full_backup_dir,
            vm_backup_dir,
            vm_max_backups,
        )

        if not check_all_backups_success(vm_backup_dir):
            log("WARNING cleanup needed - not all backup history is successful")
            this_status = "warning"

        if this_status == "success":
            success_cnt += 1
            log(
                f"VmBackup vm-export {vm_name} - ***Success*** t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vm_export_end(
                    server_name,
                    f"SUCCESS {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

        elif this_status == "warning":
            warning_cnt += 1
            log(
                f"VmBackup vm-export {vm_name} - ***WARNING*** t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vm_export_end(
                    server_name,
                    f"WARNING {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

        else:
            # this should never occur since all errors do a continue on to the next vm_name
            error_cnt += 1
            log(
                f"VmBackup vm-export {vm_name} - +++ERROR-INTERNAL+++ t:{str(elapseTime.seconds / 60)}"
            )
            if config_specified:
                status_log_vm_export_end(
                    server_name,
                    f"ERROR-INTERNAL {vm_name},elapse:{str(elapseTime.seconds / 60)} size:{backup_file_size}G",
                )

    # end of for vm_parm in config['vm-export']:
    ######################################################################

    log("===========================")
    df_snapshots(f"Space status: df -Th {config['backup_dir']}")

    # gather a final VmBackup.py status
    summary = f"S:{success_cnt} W:{warning_cnt} E:{error_cnt}"
    status_log = config["status_log"]
    if error_cnt > 0:
        if config_specified:
            status_log_end(server_name, f"ERROR,{summary}")
            # MAIL_ENABLE: optional email may be enabled by uncommenting out the next two lines
            # send_email(MAIL_TO_ADDR, 'ERROR ' + os.uname()[1] + ' VmBackup.py', status_log)
            # open(status_log, 'w').close() # trunc status log after email
        log(f"VmBackup ended - **ERRORS DETECTED** - {summary}")
    elif warning_cnt > 0:
        if config_specified:
            status_log_end(server_name, f"WARNING,{summary}")
            # MAIL_ENABLE: optional email may be enabled by uncommenting out the next two lines
            # send_email(MAIL_TO_ADDR,'WARNING ' + os.uname()[1] + ' VmBackup.py', status_log)
            # open(status_log, 'w').close() # trunc status log after email
        log(f"VmBackup ended - **WARNING(s)** - {summary}")
    else:
        if config_specified:
            status_log_end(server_name, f"SUCCESS,{summary}")
            # MAIL_ENABLE: optional email may be enabled by uncommenting out the next two lines
            # send_email(MAIL_TO_ADDR, 'Success ' + os.uname()[1] + ' VmBackup.py', status_log)
            # open(status_log, 'w').close() # trunc status log after email
        log(f"VmBackup ended - Success - {summary}")

    # done with main()
    ######################################################################


def get_vm_max_backups(vm_parm):
    # get max_backups from optional vm-export=VM-NAME:MAX-BACKUP override
    # NOTE - if not present then return config['max_backups']
    data = vm_parm.split(":")
    return (
        int(data[1])
        if len(data) > 1 and isinstance(data[1], int) and int(data[1]) > 0
        else int(config["max_backups"])
    )


def is_vm_backups_valid(vm_parm):
    data = vm_parm.split(":")
    results = True
    if len(data) > 1:
        results = data[1] > 0 if isinstance(data[1], int) else False
    return results


def get_vm_backups(vm_parm):
    """
    get max_backups from optional vm-export=VM-NAME:MAX-BACKUP override

    NOTE - if not present then return empty string ''
           else return whatever specified after ':'

    Args:
        vm_parm (str): vm-exporter configuration string
    Return:
        str : '' or the second part of the input string
    """
    data = vm_parm.split(":")
    return data[1] if len(data) > 1 else ""


def get_vm_name(vm_parm):
    # get vm_name from optional vm-export=VM-NAME:MAX-BACKUP override
    return vm_parm.split(":")[0]


def verify_vm_name(tmp_vm_name):
    vm = session.xenapi.VM.get_by_name_label(tmp_vm_name)
    vmref = [
        x
        for x in session.xenapi.VM.get_by_name_label(tmp_vm_name)
        if not session.xenapi.VM.get_is_a_snapshot(x)
    ]
    if len(vmref) > 1:
        log(f"ERROR: duplicate VM name found: {tmp_vm_name} | {vmref}")
        return f"ERROR more than one vm with the name {tmp_vm_name}"
    elif len(vm) == 0:
        return f"ERROR no machines found with the name {tmp_vm_name}"
    return vm[0]


def gather_vm_meta(vm_object, tmp_full_backup_dir):
    global vm_uuid
    global xvda_uuid
    global xvda_name_label
    vm_uuid = ""
    xvda_uuid = ""
    xvda_name_label = ""
    tmp_error = ""

    vm_record = session.xenapi.VM.get_record(vm_object)
    vm_uuid = vm_record["uuid"]

    log("Exporting VM metadata XML info")
    cmd = (
        f"{xe_path}/xe vm-export metadata=true uuid={vm_uuid} filename= "
        + '| tar -xOf - | /usr/bin/xmllint -format - > "{tmp_full_backup_dir}/vm-metadata.xml"'
    )
    if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
        log(f"WARNING {cmd}")
        this_status = "warning"
        # non-fatal - finish processing for this vm

    log("*** vm-export metadata end")

    ### The backup of the VM metadata portion in the code section below is
    ### deprecated since some entries such as name_label can contain
    ### non-standard characters that result in errors. All metadata are now saved
    ### using the code above. The additional VIF, Disk, VDI and VBD outputs
    ### are retained for now.

    #    # Backup vm meta data
    #    log ('Writing vm config file.')
    #    vm_out = open ('%s/vm.cfg' % tmp_full_backup_dir, 'w')
    #    vm_out.write('name_label=%s\n' % vm_record['name_label'])
    #    vm_out.write('name_description=%s\n' % vm_record['name_description'])
    #    vm_out.write('memory_dynamic_max=%s\n' % vm_record['memory_dynamic_max'])
    #    vm_out.write('VCPUs_max=%s\n' % vm_record['VCPUs_max'])
    #    vm_out.write('VCPUs_at_startup=%s\n' % vm_record['VCPUs_at_startup'])
    #    # notice some keys are not always available
    #    try:
    #        # notice list within list : vm_record['other_config']['base_template_name']
    #        vm_out.write('base_template_name=%s\n' % vm_record['other_config']['base_template_name'])
    #    except KeyError:
    #        # ignore
    #        pass
    #    vm_out.write('os_version=%s\n' % get_os_version(vm_record['uuid']))
    #    # get orig uuid for special metadata disaster recovery
    #    vm_out.write('orig_uuid=%s\n' % vm_record['uuid'])
    #    vm_uuid = vm_record['uuid']
    #    vm_out.close()
    #
    # Write metadata files for vdis and vbds.  These end up inside of a DISK- directory.
    log("Writing disk info")
    vbd_cnt = 0
    for vbd in vm_record["VBDs"]:
        log(f"vbd: {vbd}")
        vbd_record = session.xenapi.VBD.get_record(vbd)
        # For each vbd, find out if its a disk
        if vbd_record["type"].lower() != "disk":
            continue
        vbd_record_device = vbd_record["device"]
        if vbd_record_device == "":
            # not normal - flag as warning.
            # this seems to occur on some vms that have not been started in a long while,
            #   after starting the vm this blank condition seems to go away.
            tmp_error += f"empty vbd_record[device] on vbd: {vbd} "
            # if device is not available then use counter as a alternate reference
            vbd_cnt += 1
            vbd_record_device = vbd_cnt

        vdi_record = session.xenapi.VDI.get_record(vbd_record["VDI"])
        log(f"disk: {vdi_record['name_label']} - begin")

        # now write out the vbd info.
        device_path = f"{tmp_full_backup_dir}/DISK-{vbd_record_device}"
        os.mkdir(device_path)
        vbd_out = open(f"{device_path}/vbd.cfg", "w")
        vbd_out.write(f"userdevice={vbd_record['userdevice']}\n")
        vbd_out.write(f"bootable={vbd_record['bootable']}\n")
        vbd_out.write(f"mode={vbd_record['mode']}\n")
        vbd_out.write(f"type={vbd_record['type']}\n")
        vbd_out.write(f"unpluggable={vbd_record['unpluggable']}\n")
        vbd_out.write(f"empty={vbd_record['empty']}\n")
        # get orig uuid for special metadata disaster recovery
        vbd_out.write(f"orig_uuid={vbd_record['uuid']}\n")
        # other_config and qos stuff is not backed up
        vbd_out.close()

        # now write out the vdi info.
        vdi_out = open(f"{device_path}/vdi.cfg", "w")
        # vdi_out.write(f'name_label={vdi_record["name_label"]}\n' % )
        vdi_out.write(f"name_label={vdi_record['name_label'].encode('utf-8')}\n")
        # vdi_out.write(f'name_description={vdi_record["name_description"]}\n' )
        vdi_out.write(
            f"name_description={vdi_record['name_description'].encode('utf-8')}\n"
        )
        vdi_out.write(f"virtual_size={vdi_record['virtual_size']}\n")
        vdi_out.write(f"type={vdi_record['type']}\n")
        vdi_out.write(f"sharable={vdi_record['sharable']}\n")
        vdi_out.write(f"read_only={vdi_record['read_only']}\n")
        # get orig uuid for special metadata disaster recovery
        vdi_out.write(f"orig_uuid={vdi_record['uuid']}\n")
        sr_uuid = session.xenapi.SR.get_record(vdi_record["SR"])["uuid"]
        vdi_out.write(f"orig_sr_uuid={sr_uuid}\n")
        # other_config and qos stuff is not backed up
        vdi_out.close()
        if vbd_record_device == "xvda":
            xvda_uuid = vdi_record["uuid"]
            xvda_name_label = vdi_record["name_label"]

    # Write metadata files for vifs.  These are put in VIFs directory
    log("Writing VIF info")
    for vif in vm_record["VIFs"]:
        vif_record = session.xenapi.VIF.get_record(vif)
        log(f"Writing VIF: {vif_record['device']}")
        device_path = f"{tmp_full_backup_dir}/VIFs"
        if not os.path.exists(device_path):
            os.mkdir(device_path)
        vif_out = open(f"{device_path}/vif-{vif_record['device']}.cfg", "w")
        vif_out.write(f"device={vif_record['device']}\n")
        network_name = session.xenapi.network.get_record(vif_record["network"])[
            "name_label"
        ]
        vif_out.write(f"network_name_label={network_name}\n")
        vif_out.write(f"MTU={vif_record['MTU']}\n")
        vif_out.write(f"MAC={vif_record['MAC']}\n")
        vif_out.write(f"other_config={vif_record['other_config']}\n")
        vif_out.write(f"orig_uuid={vif_record['uuid']}\n")
        vif_out.close()

    return tmp_error


def final_cleanup(
    tmp_full_path_backup_file,
    tmp_backup_file_size,
    tmp_full_backup_dir,
    tmp_vm_backup_dir,
    tmp_vm_max_backups,
):
    # mark this a successful backup, note: this will 'touch' a file named 'success'
    # if backup size is greater than 60G, then nfs server side compression occurs
    if tmp_backup_file_size > 60:
        log(
            f"*** LARGE FILE > 60G: {tmp_full_path_backup_file} : {tmp_backup_file_size}G"
        )
        # forced compression via background gzip (requires nfs server side script)
        open(f"{tmp_full_backup_dir}/success_compress", "w").close()
        log(
            f"*** success_compress: {tmp_full_path_backup_file} : {tmp_backup_file_size}G"
        )
    else:
        open(f"{tmp_full_backup_dir}/success", "w").close()
        log(f"*** success: {tmp_full_path_backup_file} : {tmp_backup_file_size}G")

    # Remove oldest if more than tmp_vm_max_backups
    dir_to_remove = get_dir_to_remove(tmp_vm_backup_dir, tmp_vm_max_backups)
    while dir_to_remove:
        log(f"Deleting oldest backup {tmp_vm_backup_dir}/{dir_to_remove} ")
        # remove dir - if throw exception then stop processing
        shutil.rmtree(tmp_vm_backup_dir + "/" + dir_to_remove)
        dir_to_remove = get_dir_to_remove(tmp_vm_backup_dir, tmp_vm_max_backups)


####  need to just feed in directory and find oldest named subdirectory
### def pre_cleanup( tmp_full_path_backup_file, tmp_full_backup_dir, tmp_vm_backup_dir, tmp_vm_max_backups):
def pre_cleanup(tmp_vm_backup_dir, tmp_vm_max_backups):
    # print(f' ==== tmp_full_backup_dir: {tmp_full_backup_dir}'  )
    # print(f' ==== tmp_vm_backup_dir: {tmp_vm_backup_dir}'  )
    # print(f' ==== tmp_vm_max_backups: {tmp_vm_max_backups}'  )
    log(f"success identifying directory : {tmp_vm_backup_dir} ")
    # Remove oldest if more than tmp_vm_max_backups -1
    pre_vm_max_backups = tmp_vm_max_backups - 1
    log(f"pre_VM_max_backups: {pre_vm_max_backups} ")
    if pre_vm_max_backups < 1:
        log(f"No pre_cleanup needed for {tmp_vm_backup_dir} ")
    else:
        dir_to_remove = get_dir_to_remove(tmp_vm_backup_dir, tmp_vm_max_backups)
        while dir_to_remove:
            log(f"Deleting oldest backup {tmp_vm_backup_dir}/{dir_to_remove} ")
            # remove dir - if throw exception then stop processing
            shutil.rmtree(tmp_vm_backup_dir + "/" + dir_to_remove)
            dir_to_remove = get_dir_to_remove(tmp_vm_backup_dir, tmp_vm_max_backups)


# cleanup old unsuccessful backup and create new full_backup_dir
def process_backup_dir(tmp_vm_backup_dir):
    if not os.path.exists(tmp_vm_backup_dir):
        # Create new dir - if throw exception then stop processing
        os.mkdir(tmp_vm_backup_dir)

    # if last backup was not successful, then delete it
    log(f"Check for last **unsuccessful** backup: {tmp_vm_backup_dir}")
    dir_not_success = get_last_backup_dir_that_failed(tmp_vm_backup_dir)
    if dir_not_success:
        # if (not os.path.exists(tmp_vm_backup_dir + '/' + dir_not_success + '/fail')):
        log(
            f"Delete last **unsuccessful** backup {tmp_vm_backup_dir}/{dir_not_success} "
        )
        # remove last unseccessful backup  - if throw exception then stop processing
        shutil.rmtree(tmp_vm_backup_dir + "/" + dir_not_success)

    # create new backup dir
    return create_full_backup_dir(tmp_vm_backup_dir)


# Setup full backup dir structure
def create_full_backup_dir(vm_base_path):
    # Check that directory exists
    if not os.path.exists(vm_base_path):
        # Create new dir - if throw exception then stop processing
        os.mkdir(vm_base_path)

    date = datetime.datetime.today().strftime("%Y-%m--%d-(%H:%M:%S)")
    tmp_backup_dir = f"{vm_base_path}/backup-{date}"
    log(f"new backup_dir: {tmp_backup_dir}")

    if not os.path.exists(tmp_backup_dir):
        # Create new dir - if throw exception then stop processing
        os.mkdir(tmp_backup_dir)

    return tmp_backup_dir


# Setup meta dir structure
def get_meta_path(base_path):
    # Check that directory exists
    if not os.path.exists(base_path):
        # Create new dir
        try:
            os.mkdir(base_path)
        except OSError as error:
            log(f"ERROR creating directory {base_path} : {str(error)}")
            return False

    date = datetime.datetime.today().strftime("%Y%m%d-%H%M%S")
    backup_path = f"{base_path}/pool_db_{date}.dump"

    return backup_path


def get_dir_to_remove(path, numbackups):
    # Find oldest backup and select for deletion
    dirs = os.listdir(path)
    dirs.sort()
    if len(dirs) > numbackups and len(dirs) > 1:
        return dirs[0]
    else:
        return False


def get_last_backup_dir_that_failed(path):
    # if the last backup dir was not success, then return that backup dir
    dirs = os.listdir(path)
    if len(dirs) <= 1:
        return False
    dirs.sort()
    # note: dirs[-1] is the last entry
    # print(f"==== dirs that failed: {dirs}"  )
    if (
        (not os.path.exists(path + "/" + dirs[-1] + "/success"))
        and (not os.path.exists(path + "/" + dirs[-1] + "/success_restore"))
        and (not os.path.exists(path + "/" + dirs[-1] + "/success_compress"))
        and (not os.path.exists(path + "/" + dirs[-1] + "/success_compressing"))
    ):
        return dirs[-1]
    else:
        return False


def check_all_backups_success(path):
    # expect at least one backup dir, and all should be successful
    dirs = os.listdir(path)
    if len(dirs) == 0:
        return False
    for dir in dirs:
        if (
            (not os.path.exists(path + "/" + dir + "/success"))
            and (not os.path.exists(path + "/" + dir + "/success_restore"))
            and (not os.path.exists(path + "/" + dir + "/success_compress"))
            and (not os.path.exists(path + "/" + dir + "/success_compressing"))
        ):
            log(f"WARNING: directory not successful - {dir}")
            return False
    return True


def backup_pool_metadata(svr_name):
    # xe-backup-metadata can only run on master
    if not is_xe_master():
        log("** ignore: NOT master")
        return True

    metadata_base = os.path.join(config["backup_dir"], "METADATA_" + svr_name)
    metadata_file = get_meta_path(metadata_base)

    cmd = f"{xe_path}/xe pool-dump-database file-name='{metadata_file}'"
    log(cmd)
    if run(cmd, log_w_timestamp=False, out_format="rc") != 0:
        log("ERROR failed to backup pool metadata")
        return False

    return True


def get_os_version(uuid):
    cmd = (
        f"{xe_path}/xe vm-list uuid='{uuid}' params=os-version | /bin/grep 'os-version' | "
        + "/bin/awk -F'name: ' '{print $2}' | /bin/awk -F'|' '{print $1}' | /bin/awk -F';' '{print $1}'"
    )
    return run(cmd, do_log=False, out_format="lastline")


def df_snapshots(log_msg):
    log(log_msg)
    f = os.popen(f"df -Th {config['backup_dir']}")
    for line in f.readlines():
        line = line.rstrip("\n")
        log(line)


def send_email(to, subject, body_fname):
    smtp_send_retries = 3
    smtp_send_attempt = 0

    message = open(body_fname, "r").read()

    msg = MIMEText(message)
    msg["subject"] = subject
    msg["From"] = MAIL_FROM_ADDR
    msg["To"] = to

    while smtp_send_attempt < smtp_send_retries:
        smtp_send_attempt += 1
        if smtp_send_attempt > smtp_send_retries:
            print("Send email count limit exceeded")
            sys.exit(1)
        try:
            # note if using an ipaddress in MAIL_SMTP_SERVER,
            # then may require smtplib.SMTP(MAIL_SMTP_SERVER, local_hostname="localhost")

            ## Optional use of SMTP user authentication via TLS
            ##
            ## If so, comment out the next line of code and uncomment/configure
            ## the next block of code. Note that different SMTP servers will require
            ## different username options, such as the plain username, the
            ## domain\username, etc. The "From" email address entry must be a valid
            ## email address that can be authenticated  and should be configured
            ## in the MAIL_FROM_ADDR variable along with MAIL_SMTP_SERVER early in
            ## the script. Note that some SMTP servers might use port 465 instead of 587.
            s = smtplib.SMTP(MAIL_SMTP_SERVER)
            #### start block
            # username = 'MyLogin'
            # password = 'MyPassword'
            # s = smtplib.SMTP(MAIL_SMTP_SERVER, 587)
            # s.ehlo()
            # s.starttls()
            # s.login(username, password)
            #### end block
            s.sendmail(MAIL_FROM_ADDR, to.split(","), msg.as_string())
            s.quit()
            break
        except socket.error as e:
            print(f"Exception: socket.error -  {e}")
            time.sleep(5)
        except smtplib.SMTPException as e:
            print(f"Exception: SMTPException - {str(e)}")
            time.sleep(5)


def is_xe_master():
    # test to see if we are running on xe master

    cmd = f"{xe_path}/xe pool-list params=master --minimal"
    master_uuid = run(cmd, do_log=False, out_format="lastline")

    hostname = os.uname()[1]
    cmd = f"{xe_path}/xe host-list name-label={hostname} --minimal"
    host_uuid = run(cmd, do_log=False, out_format="lastline")

    if host_uuid == master_uuid:
        return True

    return False


def is_config_valid():
    if not isinstance(config["pool_db_backup"], int):
        print(f"ERROR: config pool_db_backup non-numeric -> {config['pool_db_backup']}")
        return False

    if int(config["pool_db_backup"]) != 0 and int(config["pool_db_backup"]) != 1:
        print(
            f"ERROR: config pool_db_backup out of range -> {config['pool_db_backup']}"
        )
        return False

    if not isinstance(config["max_backups"], int):
        print(f"ERROR: config max_backups non-numeric -> {config['max_backups']}")
        return False

    if int(config["max_backups"]) < 1:
        print(f"ERROR: config max_backups out of range -> {config['max_backups']}")
        return False

    if config["vdi_export_format"] != "raw" and config["vdi_export_format"] != "vhd":
        print(
            f"ERROR: config vdi_export_format invalid -> {config['vdi_export_format']}"
        )
        return False

    if not os.path.exists(config["backup_dir"]):
        print(f"ERROR: config backup_dir does not exist -> {config['backup_dir']}")
        return False

    tmp_return = True
    for vm_parm in config["vdi-export"]:
        if not is_vm_backups_valid(vm_parm):
            print(f"ERROR: vm_max_backup is invalid - {vm_parm}")
            tmp_return = False

    for vm_parm in config["vm-export"]:
        if not is_vm_backups_valid(vm_parm):
            print(f"ERROR: vm_max_backup is invalid - {vm_parm}")
            tmp_return = False

    return tmp_return


def config_load(path):
    return_value = True
    config_file = open(path, "r")
    for line in config_file:
        if not line.startswith("#") and len(line.strip()) > 0:
            (key, value) = line.strip().split("=")
            key = key.strip()
            value = value.strip()

            # check for valid keys
            if not key in expected_keys:
                if arg.is_ignore_extra_keys():
                    log(f"ignoring config key: {key}")
                else:
                    print(f"***ERROR unexpected config key: {key}")
                    return_value = False

            if key == "exclude":
                save_to_config_exclude(key, value)
            elif key in ["vm-export", "vdi-export"]:
                save_to_config_export(key, value)
            else:
                # all other key's
                save_to_config_values(key, value)

    return return_value


def save_to_config_exclude(key, vm_name):
    # save key/value in config[]
    # expected-key: exclude
    # expected-value: vmname (with or w/o regex)
    global warning_match
    global error_regex
    found_match = False
    # Fail fast if exclude param given but empty to prevent from exluding all VMs
    if vm_name == "":
        return
    if not isNormalVmName(vm_name) and not isRegExValid(vm_name):
        log(f"***ERROR - invalid regex: {key}={vm_name}")
        error_regex = True
        return
    # for vm in all_vms:
    #    if ((isNormalVmName(vm_name) and vm_name == vm) or
    #        (not isNormalVmName(vm_name) and re.match(vm_name, vm))):
    #        found_match = True
    #        config[key].append(vm)
    for vm in all_vms:
        if (isNormalVmName(vm_name) and vm_name == vm) or (
            not isNormalVmName(vm_name) and re.match(vm_name, vm)
        ):
            found_match = True
            config[key].append(vm)

    if not found_match:
        log(f"***WARNING - vm not found: {key}={vm_name}")
        warning_match = True
    else:
        for vm in config[key]:
            try:
                all_vms.remove(vm)
            except:
                pass
                # print ("VM not found -- ignore")


def save_to_config_export(key, value):
    # save key/value in config[]
    # expected-key: vm-export or vdi-export
    # expected-value: vmname (with or w/o regex) or vmname:#
    global warning_match
    global error_regex
    found_match = False

    # Fail fast if all VMs excluded or if no VMs exist in the pool
    if all_vms == []:
        return

    # Fail fast if vdi-export given but empty to prevent from matching all VMs first-come-first-served style
    # NOTE: This checks for the vdi-export key only so leaving vm-export empty will still default to all VMs
    if key == "vdi-export" and value == "":
        return

    # Evaluate key/value pairs if we get this far
    values = value.split(":")
    vm_name_part = values[0]
    vm_backups_part = ""
    if len(values) > 1:
        vm_backups_part = values[1]
    if not isNormalVmName(vm_name_part) and not isRegExValid(vm_name_part):
        log(f"***ERROR - invalid regex: {key}={value}")
        error_regex = True
        return
    for vm in all_vms:
        if (isNormalVmName(vm_name_part) and vm_name_part == vm) or (
            not isNormalVmName(vm_name_part) and re.match(vm_name_part, vm)
        ):
            if vm_backups_part == "":
                new_value = vm
            else:
                new_value = f"{vm}:{vm_backups_part}"
            found_match = True
            # Check if vdi-export already has the vm mentioned and, if so, do not add this vm to vm-export
            if key == "vm-export" and vm in config["vdi-export"]:
                continue
            else:
                config[key].append(new_value)
    if not found_match:
        log(f"***WARNING - vm not found: {key}={value}")
        warning_match = True


def isNormalVmName(str):
    if re.match("^[\w\s\-\_]+$", str) is not None:
        # normal vm name such as 'PRD-test123'
        return True
    else:
        # verses vm name using regex such as '^PRD-test[1-2]$'
        return False


def isRegExValid(text):
    try:
        re.compile(text)
        return True
    except re.error:
        return False


def save_to_config_values(key, value):
    # save key/value in config[]
    # expected-key: any key except vm-export or vdi-export or exclude
    # expected-value: any value
    if key in config.keys():
        if type(config[key]) is list:
            config[key].append(value)
        else:
            config[key] = [config[key], value]
    else:
        config[key] = value


def verify_config_vms_exist():
    all_vms_exist = True
    # verify all VMs in vm/vdi-export exist
    vm_export_errors = verify_export_vms_exist()
    if vm_export_errors != "":
        all_vms_exist = False
        log(f"ERROR - vm(s) List does not exist: {vm_export_errors}")

    # verify all VMs in exclude exist
    vm_exclude_errors = verify_exclude_vms_exist()
    if vm_exclude_errors != "":
        # all_vms_exist = False
        log(f"***WARNING - vm(s) Exclude does not exist: {vm_exclude_errors}")

    return all_vms_exist


def verify_export_vms_exist():
    vm_error = ""
    for vm_parm in config["vdi-export"]:
        # verify vm exists
        vm_name_part = get_vm_name(vm_parm)
        if not verify_vm_exist(vm_name_part):
            vm_error += vm_name_part + " "

    for vm_parm in config["vm-export"]:
        # verify vm exists
        vm_name_part = get_vm_name(vm_parm)
        if not verify_vm_exist(vm_name_part):
            vm_error += vm_name_part + " "

    return vm_error


def verify_exclude_vms_exist():
    vm_error = ""
    for vm_parm in config["exclude"]:
        # verify vm exists
        vm_name_part = get_vm_name(vm_parm)
        if not verify_vm_exist(vm_name_part):
            vm_error += vm_name_part + " "

    return vm_error


def verify_vm_exist(vm_name):
    vm = session.xenapi.VM.get_by_name_label(vm_name)
    if len(vm) == 0:
        return False
    else:
        return True


def get_all_vms():
    cmd = f"{xe_path}/xe vm-list is-control-domain=false is-a-snapshot=false params=name-label --minimal"
    vms = run(cmd, do_log=False, out_format="lastline")
    return vms.split(",")


def show_vms_not_in_backup():
    # show all vm's not in backup scope
    all_vms = get_all_vms()
    for vm_parm in config["vdi-export"]:
        # remove from all_vms
        vm_name_part = get_vm_name(vm_parm)
        if vm_name_part in all_vms:
            all_vms.remove(vm_name_part)

    for vm_parm in config["vm-export"]:
        # remove from all_vms
        vm_name_part = get_vm_name(vm_parm)
        if vm_name_part in all_vms:
            all_vms.remove(vm_name_part)

    vms_not_in_backup = ""
    for vm_name in all_vms:
        vms_not_in_backup += vm_name + " "
    log(f"VMs-not-in-backup: {vms_not_in_backup}")


def cleanup_vmexport_vdiexport_dups():
    # if any vdi-export's exist in vm-export's then remove from vm-export
    for vdi_parm in config["vdi-export"]:
        # vdi_parm has form PRD-name or PRD-name:5
        tmp_vdi_parm = get_vm_name(vdi_parm)
        for vm_parm in config["vm-export"]:
            tmp_vm_parm = get_vm_name(vm_parm)
            if tmp_vm_parm == tmp_vdi_parm:
                log(f"***WARNING vdi-export duplicate - removing vm-export={vm_parm}")
                config["vm-export"].remove(vm_parm)
    # remove duplicates
    config["vdi-export"] = RemoveDup(config["vdi-export"])
    config["vm-export"] = RemoveDup(config["vm-export"])


def RemoveDup(duplicate):
    # OK, this access to excludes works, good! Can use internally then.
    # print(f'exclude list: {config["exclude"]} '  )
    # print(f'exclude element 0: {config["exclude"][0]}'  )
    # print(f'exclude element 1: {config["exclude"][1]}'  )
    final_list = []
    for val in duplicate:
        # print(f'===== val: {val}' % )

        # check if version exists and if so, take account of extra versions
        # as well as if a numbered wildcarded version already exists!
        versioned = 0
        accounted = 0
        # version flag here for debugging and tracking purposes, only
        if val.find(":") != -1:
            # found version in new VM entry and need to expand
            (valroot, numb) = val.split(":")
            # print(f'found version to check: {val} {valroot}' )
            versioned = 1
        else:
            versioned = 0
            # set root to be the same
            valroot = val
            # print(f'valroot set to be val if simple name: {valroot}'  )

        # Need to replace old with new if found
        # Redo list and replace with new value
        # Loop on index, starting with 0 and if root is the same,
        # sub in new value; last index in array is len(array)-1 since len(array)
        # is the number of elements in an array.
        alen = len(final_list)
        i = 0
        # # #
        while i < alen:
            if final_list[i].find(":") != -1:
                (finroot, fnumb) = final_list[i].split(":")
            else:
                finroot = final_list[i]
                # print(f'index: val, valroot, final_list, finroot: {i} {val} {valroot} {final_list[i]} {finroot} ')
            if valroot == finroot:
                # root matches, hence replace
                # print(f'*** Replacing final_list with val, i: {final_list[i]} {val} {i})
                final_list[i] = val

                # check again if excluded
                # print('check again if excluded ........')
                j = 0
                elen = len(config["exclude"])
                while j < elen:
                    eroot = config["exclude"][j]
                    # printf('valroot:{valroot}'  )
                    # print(f'eroot:{eroot}'  )
                    # print(f'final_list[i]:{final_list[i]}'  )
                    # print(f'val:{val}'  )
                    if valroot == eroot:
                        # remove from list
                        log(f"***WARNING - forcing exclude of: {final_list[i]} ")
                        accounted = 1
                        final_list.remove(final_list[i])
                        break
                    else:
                        j = j + 1

                # VM has been accounted for
                accounted = 1
                # print(f'VM (val) has been accounted for, accounted: {val} {accounted}' )
                break
            else:
                i = i + 1

        # need to check plain case if not accounted for yet
        # print(f'Not found anywhere else... accounted={accounted}'  )
        # However, check again if excluded and if so, do not add to list
        # print('check YET again if excluded !!!!!!!!')
        j = 0
        elen = len(config["exclude"])
        while j < elen:
            eroot = config["exclude"][j]
            # print(f'valroot:{valroot}'  )
            # print(f'eroot:{eroot}'  )
            # print(f'final_list[i]:{final_list[i]}'  )
            # print(f'val:{val}'  )
            if valroot == eroot:
                # prevent from being added back onto the list
                log(f"***WARNING - forcing exclude of: {val} ")
                accounted = 1
                # print(f'=== Force accounted to be on:{accounted}'
                break
            else:
                j = j + 1

        if accounted == 0:
            if val not in final_list:
                final_list.append(val)
                # print(f' end block -- appended val to list: {val}'  )
            else:
                # it should now never actually get here!
                print(f"SHOULD NEVER GET HERE  ----- found duplicate: {val}")

    return final_list


def config_load_defaults():
    # init config param not already loaded then load with default values
    if not "pool_db_backup" in config.keys():
        config["pool_db_backup"] = str(DEFAULT_POOL_DB_BACKUP)
    if not "max_backups" in config.keys():
        config["max_backups"] = str(DEFAULT_MAX_BACKUPS)
    if not "vdi_export_format" in config.keys():
        config["vdi_export_format"] = str(DEFAULT_VDI_EXPORT_FORMAT)
    if not "backup_dir" in config.keys():
        config["backup_dir"] = str(DEFAULT_BACKUP_DIR)
    if not "status_log" in config.keys():
        config["status_log"] = str(DEFAULT_STATUS_LOG)


def config_print():
    log("VmBackup.py running with these settings:")
    log(f"  backup_dir        = {config['backup_dir']}")
    log(f"  status_log        = {config['status_log']}")
    log(f"  compress          = {arg.is_compress()}")
    log(f"  max_backups       = {config['max_backups']}")
    log(f"  vdi_export_format = {config['vdi_export_format']}")
    log(f"  pool_db_backup    = {config['pool_db_backup']}")

    log(f"  exclude (cnt)= {len(config['exclude'])}")
    str = ""
    for vm_parm in sorted(config["exclude"]):
        str += f"{vm_parm}, "
    if len(str) > 1:
        str = str[:-2]
    log(f"  exclude: {str}")

    log(f"  vdi-export (cnt)= {len(config['vdi-export'])}")
    str = ""
    for vm_parm in sorted(config["vdi-export"]):
        str += f"{vm_parm}, "
    if len(str) > 1:
        str = str[:-2]
    log(f"  vdi-export: {str}")

    log(f"  vm-export (cnt)= {len(config['vm-export'])}")
    str = ""
    for vm_parm in sorted(config["vm-export"]):
        str += f"{vm_parm}, "
    if len(str) > 1:
        str = str[:-2]
    log(f"  vm-export: {str}")


def status_log(server, op="begin", kind="vmbackup.py", status=""):
    date = datetime.datetime.today().strftime("%y%m%d %H:%M:%S")
    message_line = f"{date},{kind},{server},{op},{status}\n"
    open(config["status_log"], "a", 0).write(message_line)


def status_log_begin(server):
    status_log(server)


def status_log_end(server, status):
    status_log(server, op="end", status=status)


def status_log_vm_export_begin(server, status):
    status_log(server, kind="vm-export", status=status)


def status_log_vm_export_end(server, status):
    status_log(server, op="end", kind="vm-export", status=status)


def status_log_vdi_export_begin(server, status):
    status_log(server, kind="vdi-export", status=status)


def status_log_vdi_export_end(server, status):
    status_log(server, op="end", kind="vdi-export", status=status)


if __name__ == "__main__":
    arg = argument.Arguments()
    arg.help_check()
    password = arg.get_password()
    cfg_file = arg.args.config_file or ""

    # init vm-export/vdi-export/exclude in config list
    config["vm-export"] = []
    config["vdi-export"] = []
    config["exclude"] = []
    warning_match = False
    error_regex = False
    all_vms = get_all_vms()

    # process config file
    if os.path.exists(cfg_file):
        # config file exists
        config_specified = 1
        if config_load(cfg_file):
            cleanup_vmexport_vdiexport_dups()
        else:
            print("ERROR in config_load, consider ignore_extra_keys=true")
            sys.exit(1)
    else:
        # no config file exists - so cfg_file is actual vm_name/prefix
        config_specified = 0
        cmd_option = "vm-export"  # default
        cmd_vm_name = cfg_file  # in this case a vm name pattern
        if cmd_vm_name.count("=") == 1:
            (cmd_option, cmd_vm_name) = cmd_vm_name.strip().split("=")
        if cmd_option != "vm-export" and cmd_option != "vdi-export":
            print(f"ERROR invalid config/vm_name: {cfg_file}")
            arg.parser.print_help()
            sys.exit(1)
        save_to_config_export(cmd_option, cmd_vm_name)

    config_load_defaults()  # set defaults that are not already loaded
    log(f"VmBackup config loaded from: {cfg_file}")
    config_print()  # show fully loaded config

    if not is_config_valid():
        log("ERROR in configuration settings...")
        sys.exit(1)
    if len(config["vm-export"]) == 0 and len(config["vdi-export"]) == 0:
        log("ERROR no VMs loaded")
        sys.exit(1)

    # acquire a xapi session by logging in
    try:
        username = "root"
        session = XenAPI.Session("http://localhost/")
        # print (f"session is: {session} ")

        session.xenapi.login_with_password(username, password)
        hosts = session.xenapi.host.get_all()
    except XenAPI.Failure as e:
        print(e)
        if e.details[0] == "HOST_IS_SLAVE":
            session = XenAPI.Session("http://" + e.details[1])
            session.xenapi.login_with_password(username, password)
            hosts = session.xenapi.host.get_all()
        else:
            print("ERROR - XenAPI authentication error")
            sys.exit(1)

    if arg.is_preview():
        # check for duplicate names
        log("Checking all VMs for duplicate names ...")
        for vm in all_vms:
            vmref = [
                x
                for x in session.xenapi.VM.get_by_name_label(vm)
                if not session.xenapi.VM.get_is_a_snapshot(x)
            ]
            if len(vmref) > 1:
                log(f"*** ERROR: duplicate VM name found: {vm} | {vmref}")

    if not verify_config_vms_exist():
        # error message(s) printed in verify_config_vms_exist
        sys.exit(1)
    # OPTIONAL
    # show_vms_not_in_backup()

    # todo - these warning/errors are a little confusing, clean these up later
    if arg.is_preview():
        warning = ""
        if warning_match:
            warning = " - WARNINGS found (see above)"
        if error_regex:
            log(f"ERROR regex errors found (see above) {warning}")
            sys.exit(1)
        log(f"SUCCESS preview of parameters {warning}")
        sys.exit(1)

    warning = ""
    if warning_match:
        warning = " - WARNINGS found (see above)"
    log(f"SUCCESS check of parameters {warning}")
    if error_regex:
        log("ERROR regex errors found (see above)")
        sys.exit(1)

    try:
        main(session)

    except Exception as e:
        print(e)
        log(f"***ERROR EXCEPTION - {sys.exc_info()[0]}")
        log("***ERROR NOTE: see VmBackup output for details")
        raise
    session.logout
