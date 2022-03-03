# -*- coding: utf-8 -*-

import logging
import os
import tarfile

from tempfile import NamedTemporaryFile
from typing import List

from .backend import VhtBackend


class VHTClient:
    @staticmethod
    def get_available_backends() -> List[str]:
        backends = VhtBackend.find_implementations()
        return sorted(backends.keys(), key=lambda k: backends[k].priority())

    def __init__(self, backend):
        self.backend_desc = backend.lower()
        logging.info(f"vht:{self.backend_desc} backend selected!")
        self._set_backend()

    def _set_backend(self):
        backends = VhtBackend.find_implementations()
        if self.backend_desc in backends:
            self.backend = backends[self.backend_desc]()
        else:
            logging.error(f"{self.backend_desc} not supported!")
            raise RuntimeError()

    def create_instance(self) -> str:
        """Create a new VHT instance"""
        return self.backend.create_instance()

    def start_instance(self):
        return self.backend.start_instance()

    def stop_instance(self):
        return self.backend.stop_instance()

    def terminate_instance(self):
        return self.backend.terminate_instance()

    def get_instance_state(self):
        return self.backend.get_instance_state()

    def upload_file_to_cloud(self, filename: str, key: str):
        return self.backend.upload_file_to_cloud(filename, key)

    def download_file_from_cloud(self, filename, key):
        return self.backend.download_file_from_cloud(filename, key)

    def delete_file_from_cloud(self, key: str) -> str:
        return self.backend.delete_file_from_cloud(key)

    def run(self, workdir: str = os.getcwd()):
        """Run the VHT job in the given WORKDIR"""
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
            cmds = ["pwd",
                    "pip install -r requirements.txt",
                    "python build.py cbuild vht"]

            self.backend.run_commands(cmds)

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
