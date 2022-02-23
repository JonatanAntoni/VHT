# -*- coding: utf-8 -*-

class VhtBackend:

    INSTANCE_INVALID = 0
    INSTANCE_CREATED = 1
    INSTANCE_STARTED = 2
    INSTANCE_RUNNING = 3

    def create_or_start_instance(self):
        '''Create or start a new machine instance

        Returns:
            INSTANCE_CREATED
            INSTANCE_STARTED
            INSTANCE_RUNNING
        '''
        raise NotImplementedError()

    def cleanup_instance(self, state):
        '''Cleanup the used instance.

        Params:
            state - The instance state returned by create_or_start_instance
        '''
        raise NotImplementedError()

    def prepare_instance(self):
        '''Runs required commands to prepare the instance for VHT workload.
         '''
        raise NotImplementedError()

    def upload_workspace(self, tarball):
        '''Upload the workspace content from the given tarball.

        Params:
            tarball - The archived workspace.
        '''
        raise NotImplementedError()

    def run_commands(self, cmds):
        commands = [f"runuser -l ubuntu -c 'source vars && {cmd}'" for cmd in cmds]
        self.send_remote_command_batch(commands, working_dir=AWSClient.AMI_WORKDIR)

    def download_workspace(self, tarball):
        '''Download the workspace content into given tarball.

        Params:
            tarball - The archived workspace.
        '''
        raise NotImplementedError()