# -*- coding: utf-8 -*-

import logging
import os
import tarfile

from pathlib import Path
from tempfile import NamedTemporaryFile

from .backend import VhtBackend
from .aws_backend import AwsBackend
from .local_backend import LocalBackend

class VHTClient:
    def __init__(self, backend):
        self.backend_desc = backend.lower()
        logging.info(f"vht:{self.backend_desc} backend selected!")
        self._set_backend()

    def _set_backend(self):
        if self.backend_desc == "aws":
            self.backend = AwsBackend()
        elif self.backend_desc == "local":
            self.backend = LocalBackend()
        else:
            logging.error(f"{self.backend_desc} not supported!")
            raise RuntimeError()

    def create_instance(self):
        return self.backend.create_instance()

    def delete_file_from_cloud(self, key):
        return self.backend.delete_file_from_cloud(key)

    def download_file_from_cloud(self, filename, key):
        return self.backend.download_file_from_cloud(filename, key)

    def get_image_id(self):
        return self.backend.get_image_id()

    def get_instance_state(self):
        return self.backend.get_instance_state()

    def get_process_vht_commands(self):
        return self.backend.get_process_vht_commands()

    def run(self, workdir=os.getcwd()):
        vhtin = None
        vhtout = None
        instance_state = VhtBackend.INSTANCE_INVALID
        try:
            logging.info("Creating/staring instance...")
            instance_state = self.backend.create_or_start_instance()

            logging.info("Preparing instance...")
            self.backend.prepare_instance()

            logging.info("Uploading workspace...")
            vhtin = NamedTemporaryFile(mode='w+b', prefix='vhtin-', suffix='.tbz2', delete=False)
            vhtin.close()
            with tarfile.open(vhtin.name, mode='w:bz2') as archive:
                archive.add(workdir, arcname="./")

            self.backend.upload_workspace(vhtin.name)

            logging.info("Executing...")
            self.backend.run_commands([
                "pip install -r requirements.txt",
                "python build.py cbuild vht"
            ])

            logging.info("Downloading workspace...")
            vhtout = NamedTemporaryFile(mode='r+b', prefix='vhtout-', suffix='.tbz2', delete=False)
            vhtout.close()
            self.backend.download_workspace(vhtout.name)
            with tarfile.open(vhtout.name, mode='r:bz2') as archive:
                archive.extractall(path=workdir)
        finally:
            if vhtin:
                os.remove(vhtin.name)
            if vhtout:
                os.remove(vhtout.name)
            self.backend.cleanup_instance(instance_state)



    def send_remote_command(self, command_list, working_dir, fail_if_unsuccess=True):
        return self.backend.send_remote_command(command_list=command_list,
                                                working_dir=working_dir,
                                                fail_if_unsuccess=fail_if_unsuccess)

    def send_remote_command_batch(self, command_list, working_dir, fail_if_unsuccess=True):
        return self.backend.send_remote_command_batch(
                                                command_list=command_list,
                                                working_dir=working_dir,
                                                fail_if_unsuccess=fail_if_unsuccess)

    def start_instance(self):
        return self.backend.start_instance()

    def stop_instance(self):
        return self.backend.stop_instance()

    def terminate_instance(self):
        return self.backend.terminate_instance()

    def teardown(self):
        return self.backend.teardown()

    def upload_file_to_cloud(self, filename, key):
        return self.backend.upload_file_to_cloud(filename, key)
