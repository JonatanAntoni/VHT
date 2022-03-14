# -*- coding: utf-8 -*-

import logging
import subprocess
import tarfile
from pathlib import Path

from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import List, Union

from .avh_backend import AvhBackend, AvhBackendState
from .helper import create_archive


class LocalBackend(AvhBackend):

    @staticmethod
    def name() -> str:
        return "local"

    @staticmethod
    def priority() -> int:
        return 50

    def __init__(self):
        self._workdir = TemporaryDirectory(prefix="avhwork-")

    def prepare(self) -> AvhBackendState:
        return AvhBackendState.RUNNING

    def cleanup(self, state: AvhBackendState):
        logging.info("Cleaning up %s", self._workdir.name)
        self._workdir.cleanup()

    def upload_workspace(self, tarball: Union[str, Path]):
        logging.info("Extracting workspace into %s", self._workdir.name)
        with tarfile.open(tarball, mode='r:bz2') as archive:
            archive.extractall(path=self._workdir.name)

    def run_commands(self, cmds: List[str]):
        shfile = NamedTemporaryFile(prefix="script-", suffix=".sh", dir=self._workdir.name, delete=False)
        with open(shfile.name, mode="w", encoding='UTF-8', newline='\n') as f:
            f.write("#!/bin/bash\n")
            f.write("set +x\n")
            f.write("\n".join(cmds))
            f.write("\n")

        subprocess.run(["bash", shfile.name], shell=True, cwd=self._workdir.name)

    def download_workspace(self, tarball: Union[str, Path], globs: List[str] = ['**/*']):
        logging.info("Archiving workspace from %s", self._workdir.name)
        create_archive(tarball, self._workdir.name, globs)
