# -*- coding: utf-8 -*-

import logging
import os
import tarfile
import yaml

from glob import iglob
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List

from .backend import VhtBackend, VhtBackendState
from .helper import create_archive


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

    def prepare(self):
        return self.backend.prepare()

    def cleanup(self, state: VhtBackendState = VhtBackendState.CREATED):
        return self.backend.cleanup(state)

    def run(self, specfile: Path = Path.cwd().joinpath("vht.yml")):
        """Run the VHT job in the given WORKDIR"""
        vhtin = None
        vhtout = None
        backend_state = VhtBackendState.INVALID

        if not specfile.exists():
            raise RuntimeError from FileNotFoundError(specfile)

        with open(specfile) as file:
            spec = yaml.safe_load(file)

        workdir = specfile.parent.joinpath(spec.get('workdir', '.')).resolve()
        upload = spec.get('upload', ['**/*'])
        steps = spec.get('steps', [])
        download = spec.get('download', ['**/*'])

        try:
            logging.info("Preparing instance...")
            backend_state = self.backend.prepare()

            logging.info("Uploading workspace...")
            vhtin = NamedTemporaryFile(mode='w+b', prefix='vhtin-', suffix='.tbz2', delete=False)
            vhtin.close()
            create_archive(vhtin.name, workdir, upload, verbose=True)

            self.backend.upload_workspace(vhtin.name)

            logging.info("Executing...")
            for step in steps:
                if 'run' in step:
                    cmds = [cmd for cmd in step['run'].split('\n') if cmd]
                    self.backend.run_commands(cmds)

            logging.info("Downloading workspace...")
            vhtout = NamedTemporaryFile(mode='r+b', prefix='vhtout-', suffix='.tbz2', delete=False)
            vhtout.close()
            self.backend.download_workspace(vhtout.name, download)
            with tarfile.open(vhtout.name, mode='r:bz2') as archive:
                archive.list(verbose=False)
                archive.extractall(path=workdir)
        finally:
            if vhtin:
                os.remove(vhtin.name)
            if vhtout:
                os.remove(vhtout.name)
            self.backend.cleanup(backend_state)
