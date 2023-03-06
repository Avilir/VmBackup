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
from pathlib import Path
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
import configuration
from constnts import *
from logger import log, message


config = configuration.Config()

session = None  # Placeholder variable, real value will set in the main function


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
    log(f"Check if backup directory {config.data('backup_dir')} is writable ...")
    touchfile = os.path.join(config.data("backup_dir"), "00VMbackupWriteTest")
    try:
        Path(touchfile).touch()
    except Exception:
        log("ERROR failed to write to backup directory area - FATAL ERROR")
        sys.exit(1)
    os.remove(touchfile)
    log("Success: backup directory area is writable")

    log("===========================")
    run_df("Space before backups:", config.data("backup_dir"))

    if int(config.data("pool_db_backup")):
        log("*** begin backup_pool_metadata ***")
        if not backup_pool_metadata(server_name):
            error_cnt += 1

    ######################################################################
    # Iterate through all vdi-export= in cfg
    log("************ vdi-export= ***************")
    for vm_parm in config.data("vdi-export"):
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

        vm_backup_dir = os.path.join(config.data("backup_dir"), vm_name)
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
        check_if_vm_is_running(vm_name)

        # list the vdi we will backup
        cmd = f"vdi-list uuid={xvda_uuid}"
        log(f"1.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") != 0:
            log(f"ERROR xe {cmd}")
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
            f"vdi-list name-label='{snap_vdi_name_label}' params=uuid |"
            + " /bin/awk -F': ' '{print $2}' | /bin/grep '-'"
        )
        old_snap_vdi_uuid = run_xe(cmd)
        if old_snap_vdi_uuid != "":
            log(f"cleanup old-snap-vdi-uuid: {old_snap_vdi_uuid}")
            # vdi-destroy old vdi-snapshot
            this_status = destroy_vdi_snapshot(old_snap_vdi_uuid)

        # === pre_cleanup code goes in here ===
        if arg.is_pre_clean():
            pre_cleanup(vm_backup_dir, vm_max_backups)

        # take a vdi-snapshot of this vm
        cmd = f"vdi-snapshot uuid={xvda_uuid}"
        log(f"2.cmd: xe {cmd}")
        snap_vdi_uuid = run_xe(cmd)
        log(f"snap-uuid: {snap_vdi_uuid}")
        if snap_vdi_uuid == "":
            log(f"ERROR xe {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-SNAPSHOT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # change vdi-snapshot to unique name-label for easy id and cleanup
        cmd = f'vdi-param-set uuid={snap_vdi_uuid} name-label="{snap_vdi_name_label}"'
        log(f"3.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") != 0:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-PARAM-SET-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # actual-backup: vdi-export vdi-snapshot
        cmd = (
            f"vdi-export format={config.data('vdi_export_format')} uuid={snap_vdi_uuid}"
        )
        full_path_backup_file = os.path.join(
            full_backup_dir, vm_name + f'.config["vdi_export_format"]'
        )
        cmd = f'{cmd} filename="{full_path_backup_file}"'
        log(f"4.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") == 0:
            log("vdi-export success")
        else:
            log(f"ERROR xe {cmd}")
            if config_specified:
                status_log_vdi_export_end(server_name, f"VDI-EXPORT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # cleanup: vdi-destroy vdi-snapshot
        this_status = destroy_vdi_snapshot(snap_vdi_uuid, log_prefix="5.cmd")

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
    for vm_parm in config.data("vm-export"):
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

        vm_backup_dir = os.path.join(config.data("backup_dir"), vm_name)
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
        check_if_vm_is_running(vm_name)

        # check for old vm-snapshot for this vm
        snap_name = f"RESTORE_{vm_name}"
        log(f"check for prev-vm-snapshot: {snap_name}")
        cmd = (
            f"vm-list name-label='{snap_name}' params=uuid | "
            + "/bin/awk -F': ' '{print $2}' | /bin/grep '-'"
        )
        old_snap_vm_uuid = run_xe(cmd)
        if old_snap_vm_uuid != "":
            log(f"cleanup old-snap-vm-uuid: {old_snap_vm_uuid}")
            # vm-uninstall old vm-snapshot
            cmd = f"vm-uninstall uuid={old_snap_vm_uuid} force=true"
            log(f"cmd: xe {cmd}")
            if run_xe(cmd, out_format="rc") != 0:
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
        cmd = f'vm-snapshot vm={vm_uuid} new-name-label="{snap_name}"'
        log(f"1.cmd: xe {cmd}")
        snap_vm_uuid = run_xe(cmd)
        log(f"snap-uuid: {snap_vm_uuid}")
        if snap_vm_uuid == "":
            log(f"ERROR xe {cmd}")
            if config_specified:
                status_log_vm_export_end(server_name, f"SNAPSHOT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # change vm-snapshot so that it can be referenced by vm-export
        cmd = f"template-param-set is-a-template=false ha-always-run=false uuid={snap_vm_uuid}"
        log(f"2.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") != 0:
            log(f"ERROR {cmd}")
            if config_specified:
                status_log_vm_export_end(
                    server_name, f"TEMPLATE-PARAM-SET-FAIL {vm_name}"
                )
            error_cnt += 1
            # next vm
            continue

        # vm-export vm-snapshot
        cmd = f"vm-export uuid={snap_vm_uuid}"
        if arg.is_compress():
            full_path_backup_file = os.path.join(full_backup_dir, vm_name + ".xva.gz")
            cmd = f'{cmd} filename="{full_path_backup_file}" compress=true'
        else:
            full_path_backup_file = os.path.join(full_backup_dir, vm_name + ".xva")
            cmd = f'{cmd} filename="{full_path_backup_file}"'
        log(f"3.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") == 0:
            log("vm-export success")
        else:
            log(f"ERROR xe {cmd}")
            if config_specified:
                status_log_vm_export_end(server_name, f"VM-EXPORT-FAIL {vm_name}")
            error_cnt += 1
            # next vm
            continue

        # vm-uninstall vm-snapshot
        cmd = f"vm-uninstall uuid={snap_vm_uuid} force=true"
        log(f"4.cmd: xe {cmd}")
        if run_xe(cmd, out_format="rc") != 0:
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
    run_df("Space status:", config.data("backup_dir"))

    # gather a final VmBackup.py status
    summary = f"S:{success_cnt} W:{warning_cnt} E:{error_cnt}"
    status_log = config.data("status_log")
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
        else int(config.data("max_backups"))
    )


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
        f"vm-export metadata=true uuid={vm_uuid} filename= "
        + '| tar -xOf - | /usr/bin/xmllint -format - > "{tmp_full_backup_dir}/vm-metadata.xml"'
    )
    if run_xe(cmd, out_format="rc") != 0:
        log(f"WARNING xe {cmd}")
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

    metadata_base = os.path.join(config.data("backup_dir"), "METADATA_" + svr_name)
    metadata_file = get_meta_path(metadata_base)

    cmd = f"pool-dump-database file-name='{metadata_file}'"
    log(cmd)
    if run_xe(cmd, out_format="rc") != 0:
        log("ERROR failed to backup pool metadata")
        return False

    return True


def get_os_version(uuid):
    cmd = (
        f"vm-list uuid='{uuid}' params=os-version | /bin/grep 'os-version' | "
        + "/bin/awk -F'name: ' '{print $2}' | /bin/awk -F'|' '{print $1}' | /bin/awk -F';' '{print $1}'"
    )
    return run_xe(cmd)


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

    cmd = f"pool-list params=master --minimal"
    master_uuid = run_xe(cmd)

    hostname = os.uname()[1]
    cmd = f"host-list name-label={hostname} --minimal"
    host_uuid = run_xe(cmd)

    if host_uuid == master_uuid:
        return True

    return False


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
    for key in ["vdi-export", "vm-export"]:
        for vm_parm in config.data(key):
            # verify vm exists
            vm_name_part = get_vm_name(vm_parm)
            if not verify_vm_exist(vm_name_part):
                vm_error += vm_name_part + " "

    return vm_error


def verify_exclude_vms_exist():
    vm_error = ""
    for vm_parm in config.data("exclude"):
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
    cmd = f"vm-list is-control-domain=false is-a-snapshot=false params=name-label --minimal"
    vms = run_xe(cmd)
    return vms.split(",")


def show_vms_not_in_backup():
    # show all vm's not in backup scope
    all_vms = get_all_vms()
    for vm_parm in config.data("vdi-export"):
        # remove from all_vms
        vm_name_part = get_vm_name(vm_parm)
        if vm_name_part in all_vms:
            all_vms.remove(vm_name_part)

    for vm_parm in config.data("vm-export"):
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
    for vdi_parm in config.data("vdi-export"):
        # vdi_parm has form PRD-name or PRD-name:5
        tmp_vdi_parm = get_vm_name(vdi_parm)
        for vm_parm in config.data("vm-export"):
            tmp_vm_parm = get_vm_name(vm_parm)
            if tmp_vm_parm == tmp_vdi_parm:
                log(f"***WARNING vdi-export duplicate - removing vm-export={vm_parm}")
                config.remove_data_list("vm-export", vm_parm)
    # remove duplicates
    config.data("vdi-export", RemoveDup(config.data("vdi-export")))
    config.data("vm-export", RemoveDup(config.data("vm-export")))


def RemoveDup(duplicate):
    # OK, this access to exclude works, good! Can use internally then.

    # This is for debugging and can remove / change the command in production
    print(f'exclude list: {config.data("exclude")} ')

    final_list = []
    for val in duplicate:
        # print(f'===== val: {val}' % )

        # check if version exists and if so, take account of extra versions
        # as well as if a numbered wildcard version already exists!
        versioned = 0
        accounted = 0
        # version flag here for debugging and tracking purposes, only
        if val.find(":") != -1:
            # found version in new VM entry and need to expand
            (valroot, numb) = val.split(":")
            print(f"found version to check: {val} {valroot}")
            versioned = 1
        else:
            versioned = 0
            # set root to be the same
            valroot = val
            print(f"valroot set to be val if simple name: {valroot}")

        # Need to replace old with new if found, Redo list and replace with new value
        # Loop on index, starting with 0 and if root is the same,
        # sub in new value; last index in array is len(array)-1 since len(array)
        # is the number of elements in an array.
        alen = len(final_list)
        i = 0
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
                elen = len(config.data("exclude"))
                while j < elen:
                    eroot = config.data("exclude")[j]
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
        elen = len(config.data("exclude"))
        while j < elen:
            eroot = config.data("exclude")[j]
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


def status_log(server, op="begin", kind="vmbackup.py", status=""):
    date = datetime.datetime.today().strftime("%y%m%d %H:%M:%S")
    message_line = f"{date},{kind},{server},{op},{status}\n"
    open(config.data("status_log"), "a", 0).write(message_line)


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
    config.data("compress", arg.is_compress())
    config.data("ignore_extra_keys", arg.is_ignore_extra_keys())

    cfg_file = arg.args.config_file or ""
    if cfg_file != "":
        config.filename(cfg_file)
        if not config.load():
            print(
                "ERROR in reading the configuration file, consider ignore_extra_keys=true"
            )
            sys.exit(1)
        else:
            config_specified = 1
            cleanup_vmexport_vdiexport_dups()

    config.data("all_vms", get_all_vms())

    log(f"VmBackup config loaded from: {cfg_file}")
    config.print()  # show fully loaded config

    warning_match = False
    error_regex = False

    if not config.is_valid():
        log("ERROR in configuration settings...")
        sys.exit(1)

    # acquire a xapi session by logging in
    username = "root"
    try:
        session = XenAPI.Session("http://localhost/")
        # print (f"session is: {session} ")
        session.xenapi.login_with_password(username, password)
    except XenAPI.Failure as e:
        print(e)
        if e.details[0] == "HOST_IS_SLAVE":
            session = XenAPI.Session("http://" + e.details[1])
            session.xenapi.login_with_password(username, password)
        else:
            print("ERROR - XenAPI authentication error")
            sys.exit(1)

    hosts = session.xenapi.host.get_all()

    if arg.is_preview():
        # check for duplicate names
        log("Checking all VMs for duplicate names ...")
        for vm in config.data("all_vms"):
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

    warning = ""
    if warning_match:
        warning = " - WARNINGS found (see above)"
    if error_regex:
        log("ERROR regex errors found (see above)")
        sys.exit(1)
    log(f"SUCCESS preview / check of parameters {warning}")

    if arg.is_preview():
        sys.exit(1)

    try:
        main(session)
        session.logout
    except Exception as e:
        print(e)
        log(f"***ERROR EXCEPTION - {sys.exc_info()[0]}")
        log("***ERROR NOTE: see VmBackup output for details")
        raise
