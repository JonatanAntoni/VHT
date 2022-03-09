# -*- coding: utf-8 -*-

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Dict, Type, List, Union


class VhtBackendState(Enum):
    INVALID = 0
    CREATED = 1
    STARTED = 2
    RUNNING = 3


class VhtBackend:
    @staticmethod
    def find_implementations() -> Dict[str, Type[VhtBackend]]:
        return {cls.name(): cls for cls in VhtBackend.__subclasses__()}

    @staticmethod
    def name() -> str:
        """Return the name this backend shall be published as.

        Returns:
            Unique backend identifier.
        """
        raise NotImplementedError()

    @staticmethod
    def priority() -> int:
        """Return a priority fir this backend.
        The priority defines the order different backend implementations are offered to the user.
        The lower the priority value the higher the backend will be listed.

        Returns:
            Priority for this backend
        """
        raise NotImplementedError()

    def prepare(self) -> VhtBackendState:
        """Runs required commands to prepare the backend for VHT workload.

        Returns:
            The BackendState the backend was is before.
         """
        raise NotImplementedError()

    def cleanup(self, state: VhtBackendState):
        """Cleanup the backend.
        The backend is brought back into state before call to prepare.

        Params:
            state - The state returned by prepare
        """
        raise NotImplementedError()

    def upload_workspace(self, tarball: Union[str, Path]):
        """Upload the workspace content from the given tarball.

        Params:
            tarball - The archived workspace.
        """
        raise NotImplementedError()

    def download_workspace(self, tarball: Union[str, Path], globs: List[str] = ['**/*']):
        """Download the workspace content into given tarball.

        Params:
            tarball - The archived workspace.
        """
        raise NotImplementedError()

    def run_commands(self, cmds: List[str]):
        """Execute the given commands on the backend.

        Params:
            cmds - List of command strings
        """
        raise NotImplementedError()

