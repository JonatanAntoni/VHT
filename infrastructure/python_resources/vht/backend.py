# -*- coding: utf-8 -*-

from __future__ import annotations
from typing import Dict, Type, List


class VhtBackend:
    INSTANCE_INVALID = 0
    INSTANCE_CREATED = 1
    INSTANCE_STARTED = 2
    INSTANCE_RUNNING = 3

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

    def create_or_start_instance(self):
        """Create or start a new machine instance

        Returns:
            INSTANCE_CREATED
            INSTANCE_STARTED
            INSTANCE_RUNNING
        """
        raise NotImplementedError()

    def cleanup_instance(self, state):
        """Cleanup the used instance.

        Params:
            state - The instance state returned by create_or_start_instance
        """
        raise NotImplementedError()

    def prepare_instance(self):
        """Runs required commands to prepare the instance for VHT workload.
         """
        raise NotImplementedError()

    def upload_workspace(self, tarball):
        """Upload the workspace content from the given tarball.

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

    def download_workspace(self, tarball):
        """Download the workspace content into given tarball.

        Params:
            tarball - The archived workspace.
        """
        raise NotImplementedError()
