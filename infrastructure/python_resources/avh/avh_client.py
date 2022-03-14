# -*- coding: utf-8 -*-

import logging
import os
import tarfile
import yaml

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List

from .avh_backend import AvhBackend, AvhBackendState
from .helper import create_archive


class AvhClient:
    @staticmethod
    def get_available_backends() -> List[str]:
        backends = AvhBackend.find_implementations()
        return sorted(backends.keys(), key=lambda k: backends[k].priority())

    def __init__(self, backend):
        self.backend_desc = backend.lower()
        logging.info(f"vht:{self.backend_desc} backend selected!")
        self._set_backend()

    def _set_backend(self):
        backends = AvhBackend.find_implementations()
        if self.backend_desc in backends:
            self.backend = backends[self.backend_desc]()
        else:
            logging.error(f"{self.backend_desc} not supported!")
            raise RuntimeError()

    def prepare(self)  -> AvhBackendState:
        """Prepare the backend to execute VHT workload."""
        return self.backend.prepare()

    def cleanup(self, state: AvhBackendState = AvhBackendState.CREATED):
        """Cleanup backend into a former state.
        Args:
            state: The state to turn backend into.
        """
        self.backend.cleanup(state)

    def run(self, specfile: Path = Path("./vht.yml")):
        """Run the VHT job specified by given specfile.
        Args:
            specfile: Path to the YAML specfile.
        """
        vhtin = None
        vhtout = None
        backend_state = AvhBackendState.INVALID

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
