# -*- coding: utf-8 -*-

import logging
import subprocess
import tarfile
from glob import iglob
from pathlib import Path

from tempfile import TemporaryDirectory
from typing import List, Union

from .backend import VhtBackend, VhtBackendState
from .helper import create_archive


class LocalBackend(VhtBackend):

    @staticmethod
    def name() -> str:
        return "local"

    @staticmethod
    def priority() -> int:
        return 50

    def __init__(self):
        self._workdir = TemporaryDirectory(prefix="vhtwork-")

    def prepare(self) -> VhtBackendState:
        return VhtBackendState.RUNNING

    def cleanup(self, state: VhtBackendState):
        logging.info("Cleaning up %s", self._workdir.name)
        self._workdir.cleanup()

    def upload_workspace(self, tarball: Union[str, Path]):
        logging.info("Extracting workspace into %s", self._workdir.name)
        with tarfile.open(tarball, mode='r:bz2') as archive:
            archive.extractall(path=self._workdir.name)

    def run_commands(self, cmds: List[str]):
        for cmd in cmds:
            logging.info("VHT> %s", cmd)
            subprocess.run(cmd, shell=True, cwd=self._workdir.name)

    def download_workspace(self, tarball: Union[str, Path], globs: List[str] = ['**/*']):
        logging.info("Archiving workspace from %s", self._workdir.name)
        create_archive(tarball, self._workdir.name, globs)
