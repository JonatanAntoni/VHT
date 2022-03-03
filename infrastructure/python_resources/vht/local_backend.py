# -*- coding: utf-8 -*-

import logging
import subprocess
import tarfile

from tempfile import TemporaryDirectory
from typing import List

from .backend import VhtBackend


class LocalBackend(VhtBackend):

    @staticmethod
    def name() -> str:
        return "local"

    @staticmethod
    def priority() -> int:
        return 50

    def __init__(self):
        self.workdir = TemporaryDirectory(prefix="vhtwork-")

    def create_or_start_instance(self, instance_id: str = None):
        return VhtBackend.INSTANCE_RUNNING

    def cleanup_instance(self, state):
        logging.info("Cleaning up %s", self.workdir.name)
        self.workdir.cleanup()

    def prepare_instance(self):
        pass

    def upload_workspace(self, tarball):
        logging.info("Extracting workspace into %s", self.workdir.name)
        with tarfile.open(tarball, mode='r:bz2') as archive:
            archive.extractall(path=self.workdir.name)

    def run_commands(self, cmds: List[str]):
        for cmd in cmds:
            logging.info("VHT> %s", self.workdir.name)
            subprocess.run(cmd, shell=True, cwd=self.workdir.name)

    def download_workspace(self, tarball):
        logging.info("Archiving workspace from %s", self.workdir.name)
        with tarfile.open(tarball, mode='w:bz2') as archive:
            archive.add(self.workdir.name, arcname="./")
