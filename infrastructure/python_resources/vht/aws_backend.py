# -*- coding: utf-8 -*-
from typing import List

import boto3
import logging
import os
import time

from botocore.exceptions import ClientError
from botocore.exceptions import WaiterError
from pathlib import Path

from .backend import VhtBackend


class AwsBackend(VhtBackend):
    AMI_WORKDIR = '/home/ubuntu'

    @staticmethod
    def name() -> str:
        return "aws"

    @staticmethod
    def priority() -> int:
        return 10

    """
    VHT AWS Backend

    This backend runs in your Amazon account:
     * Creates/starts/setup a [new] VHT EC2 instance.
     * Run VHT-related commands.
     * Get the outputs
     * Terminates/Stops the VHT EC2 instance.

    The AWS credentials key is expected as envs. See _is_aws_credentials_present method.
    Some AWS-related info is expected as envs. See _setup.
    """
    def __init__(self):
        self.ami_id: str = os.environ.get('AWS_AMI_ID', None)
        self.ami_version: str = os.environ.get('AWS_AMI_VERSION', None)
        self.iam_profile: str = os.environ.get('AWS_IAM_PROFILE', None)
        self.instance_id: str = os.environ.get('AWS_INSTANCE_ID', None)
        self.instance_type: str = os.environ.get('AWS_INSTANCE_TYPE', 't2.micro')
        self.key_name: str = os.environ.get('AWS_KEY_NAME', None)
        self.s3_bucket_name: str = os.environ.get('AWS_S3_BUCKET', None)
        self.security_group_id: str = os.environ.get('AWS_SECURITY_GROUP_ID', None)
        self.subnet_id: str = os.environ.get('AWS_SUBNET_ID', None)
        self.keep_ec2_instance: bool = (os.environ.get('AWS_KEEP_EC2_INSTANCES', 'false').lower() == 'true')
        self.s3_keyprefix = os.environ.get('AWS_S3_KEYPREFIX', 'ssm')

    def __repr__(self):
        return (
            f"ami_id={self.ami_id},"
            f"ami_version={self.ami_version},"
            f"iam_profile={self.iam_profile},"
            f"instance_id={self.instance_id},"
            f"instance_type={self.instance_type},"
            f"key_name={self.key_name},"
            f"s3_bucket_name={self.s3_bucket_name},"
            f"security_group_id={self.security_group_id},"
            f"subnet_id={self.subnet_id},"
            f"keep_ec2_instance={self.keep_ec2_instance}"
        )

    def _init(self):
        self._init = lambda: None

        self._is_aws_credentials_present()

        logging.info('aws:Creating EC2 client...')
        self._ec2_client = boto3.client('ec2')

        logging.info('aws:Creating SSM client...')
        self._ssm_client = boto3.client('ssm')

        logging.info('aws:Creating S3 client...')
        self._s3_client = boto3.client('s3')

        logging.info('aws:Creating S3 resource...')
        self._s3_resource = boto3.resource('s3')

        self._setup()

    @staticmethod
    def _check_env(key) -> bool:
        if key in os.environ:
            logging.debug("aws:%s present!", key)
            return True
        logging.info("aws:%s environment variable not present!", key)
        return False

    def _is_aws_credentials_present(self):
        """
            Verifies presence of AWS Credentias as Environment Variables.
            AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are mandatory
            AWS_SESSION_TOKEN is optional for IAM User credentials.
        """
        self._check_env('AWS_ACCESS_KEY_ID')
        self._check_env('AWS_SECRET_ACCESS_KEY')
        if not self._check_env('AWS_SESSION_TOKEN'):
            logging.debug('aws:It is expected for an IAM User')

    def _setup(self):
        """
            Setup AWS object by collecting env vars & preparing AWS instance
        """
        # Initializing None all VHT related variables
        logging.info("aws:setting up aws backend")

        # EC2-related info is not needed if an instance is already created
        if self.instance_id is None:
            if not self.ami_id:
                if not self.ami_version:
                    logging.error("Either `AWS_AMI_ID` or `AWS_AMI_VERSION` should be presented as env var!")
                    raise RuntimeError("Either `AWS_AMI_ID` or `AWS_AMI_VERSION` should be presented as env var!")
                self.ami_id = self.get_image_id()
            if not self.ami_id:
                logging.error('AWS_AMI_ID must not be blank. You should inform either AWS_AMI_ID or provide a valid AWS_AMI_VERSION')
                raise RuntimeError('AWS_AMI_ID must not be blank. You should inform either AWS_AMI_ID or provide a valid AWS_AMI_VERSION')

            if not self.iam_profile:
                logging.error("aws:environment variable `AWS_IAM_PROFILE` needs to be present!")
                raise RuntimeError("aws:environment variable `AWS_IAM_PROFILE` needs to be present!")
            if not self.security_group_id:
                logging.error("aws:environment variable `AWS_SECURITY_GROUP_ID` needs to be present!")
                raise RuntimeError("aws:environment variable `AWS_SECURITY_GROUP_ID` needs to be present!")
            if not self.subnet_id:
                logging.error("aws:environment variable `AWS_SUBNET_ID` needs to be present!")
                raise RuntimeError("aws:environment variable `AWS_SUBNET_ID` needs to be present!")

        if not self.s3_bucket_name:
            logging.error("aws:environment variable `AWS_S3_BUCKET` needs to be present!")
            raise RuntimeError("aws:environment variable `AWS_S3_BUCKET` needs to be present!")

        logging.info(f"aws:aws__repr__:{self.__repr__()}")

    def create_instance(self):
        """
            Create an EC2 Instance. It is a wrapper for create_ec2_instance.
            If key_name is present, it creates a instance with the selected private key.

            This is a mandatory VHT backend method.
        """
        self._init()
        self.instance_id = self.create_ec2_instance(
            ImageId=self.ami_id,
            InstanceType=self.instance_type,
            MaxCount=1,
            MinCount=1,
            KeyName=self.key_name,
            SecurityGroupIds=[self.security_group_id],
            SubnetId=self.subnet_id,
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'VHT_CLI', 'Value': 'true'}]}],
            IamInstanceProfile={'Name': self.iam_profile}
        )

        return self.instance_id

    def create_ec2_instance(self, **kwargs):
        """
        Create a new EC2 Instance

        Parameters
        ----------
        **kwargs: Keyword args associated with run-instances API doc e.g.:
            --create-ec2-instance
                ImageId=ami-0c5eeabe11f3a2685 \
                InstanceType=t2.micro \
                MaxCount=1 \
                MinCount=1 \
                SecurityGroupIds=['sg-04022e04e91197ce3'] \
                SubnetId=subnet-00455495b268076f0  \
                IamInstanceProfile="{'Name': 'Proj-s3-orta-vht-role'}"

        Returns
        -------
        string
            Instance ID

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
        """
        kwargs = {k: v for k, v in kwargs if v}

        logging.debug('aws:DryRun=True to test for permission check')
        logging.debug(f"aws:create_ec2_instance:kwargs:{kwargs}")

        try:
            self._ec2_client.run_instances(**kwargs, DryRun=True)
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise RuntimeError from e

        logging.info('aws:Creating EC2 instance...')
        try:
            response = self._ec2_client.run_instances(**kwargs)
        except ClientError as e:
            raise RuntimeError from e
        logging.debug(response)

        self.instance_id = response['Instances'][0]['InstanceId']
        assert isinstance(self.instance_id, str)
        self.wait_ec2_running()
        self.wait_ec2_status_ok()

        return self.instance_id

    def delete_file_from_cloud(self, key):
        """
        Delete S3 Object

        Parameters
        ----------
        String
            key (s3 path)

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info(f"aws:Delete S3 Object from S3 Bucket {self.s3_bucket_name}, Key {key}")
        try:
            response = self._s3_client.delete_object(
                Bucket=self.s3_bucket_name,
                Key=key
            )
        except ClientError as e:
            raise RuntimeError from e
        logging.debug(response)

    def download_file_from_cloud(self, filename, key):
        """
        Download S3 File

        Parameters
        ----------
        String
            filename (destination local path)
            key (s3 path)

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info("aws:Download S3 File")
        try:
            logging.info(f"Downloading S3 file from bucket `{self.s3_bucket_name}`, key `{key}`, filename `{filename}`")
            self._s3_client.download_file(self.s3_bucket_name, key, filename)
        except ClientError as e:
            if 'HeadObject operation: Not Found' in str(e):
                logging.error("Key '%s' not found on S3 Bucket Name = '%s'", key, self.s3_bucket_name)
            raise RuntimeError from e

    def get_image_id(self):
        """
        Get the VHT AMI ID for the region
        The VHT AMI ID changes for each AWS region

        Return
        ----------
        String
            VHT AMI ID

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images

        This is a mandatory VHT backend method.
        """
        assert self.ami_version is not None, \
            "The variable `ami_version` is not present"

        try:
            response = self._ec2_client.describe_images(
                Filters=[
                    {
                        'Name': 'name',
                        'Values': [
                            f"ArmVirtualHardware-{self.ami_version}*"
                        ]
                    },
                ]
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_vht_ami_id_by_version:{response}")
        self.ami_id = response['Images'][0]['ImageId']
        return self.ami_id

    def get_instance_state(self):
        """
        Get EC2 Instance State

        Return
        ----------
        String
            EC2 Instance State ('pending'|'running'|'shutting-down'|'terminated'|'stopping'|'stopped')

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_instances
        """
        self._init()

        try:
            response = self._ec2_client.describe_instances(
                InstanceIds=[
                    self.instance_id,
                ],
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_instance_state: {response}")
        instance_state = response['Reservations'][0]['Instances'][0]['State']['Name']
        logging.info(f"aws:The EC2 instance state is {instance_state}...")
        return instance_state

    def get_s3_file_content(self, key):
        """
        Get S3 File Content

        Parameters
        ----------
        String
            key (s3 path)

        Return
        ----------
        String
            S3 File Content

        More
        ----
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object
        """
        self._init()
        content = ''
        try:
            content = self._s3_resource.Object(self.s3_bucket_name, key).get()['Body'].read().decode('utf-8')
        except self._s3_client.exceptions.NoSuchKey:
            logging.error(f"aws:Key '%s' not found on S3 bucket '%s'", key, self.s3_bucket_name)
        return content

    def get_s3_ssm_command_id_key(self, command_id, output_type):
        """
        Get calculated S3 SSM Command ID Output Key

        Parameters
        ----------
        String
            command_id (Command ID)
            output_type (`stderr` or `stdout`)

        Return
        ----------
        String
            S3 SSM Command ID Key
        """
        return f"{self.s3_keyprefix}/{command_id}/{self.instance_id}/awsrunShellScript/0.awsrunShellScript/{output_type}"

    def get_ssm_command_id_status(self, command_id):
        """
        Get the Status for a specific command ID and Instance ID.

        Parameters
        ----------
        String
            command_id (Command ID)

        Return
        ----------
        String
            Command ID Status

        More
        ----------
        API Definition:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.list_commands
        """

        try:
            response = self._ssm_client.list_commands(
                CommandId=command_id
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_ssm_command_id_status:{response}")
        command_id_status = response['Commands'][0]['Status']
        logging.info(f"aws:The command_id {command_id} status is {command_id_status}...")
        return command_id_status

    def get_ssm_command_id_status_details(self, command_id):
        """
        Get the Status details for a specific command ID and Instance ID.

        Parameters
        ----------
        String
            command_id (Command ID)

        Return
        ----------
        String
            Command ID Status Details

        More
        ----------
        API Definition:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.get_command_invocation
        """

        try:
            response = self._ssm_client.get_command_invocation(
                CommandId=command_id,
                InstanceId=self.instance_id
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_ssm_command_id_status_details:{response}")
        logging.info(f"aws:The command_id {command_id} status details is {response['StatusDetails']}...")
        return response['StatusDetails']

    def get_ssm_command_id_stdout_url(self, command_id):
        """
        Get the stdout output URL for a specific command ID and Instance ID.

        Parameters
        ----------
        String
            command_id (Command ID)

        Return
        ----------
        String
            Command ID Stdout URL

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.list_command_invocations
        """
        try:
            response = self._ssm_client.list_command_invocations(
                CommandId=command_id,
                InstanceId=self.instance_id
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_ssm_command_id_stdout_url:{response}")
        return response['CommandInvocations'][0]['StandardOutputUrl']

    def get_ssm_command_id_stderr_url(self, command_id):
        """
        Get the stderr output URL for a specific command ID and Instance ID.

        Parameters
        ----------
        String
            command_id (Command ID)

        Return
        ----------
        String
            Command ID Stderr URL

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.list_command_invocations
        """
        try:
            response = self._ssm_client.list_command_invocations(
                CommandId=command_id,
                InstanceId=self.instance_id
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:get_ssm_command_id_stderr_url:{response}")
        return response['CommandInvocations'][0]['StandardErrorUrl']

    def create_or_start_instance(self):
        self._init()
        if self.instance_id:
            state = self.get_instance_state()
            if state == "running":
                logging.info(f"aws:EC2 Instance {self.instance_id} already running!")
                return VhtBackend.INSTANCE_RUNNING
            elif state == "stopped":
                logging.info(f"aws:EC2 Instance {self.instance_id} provided!")
                self.start_instance()
                return VhtBackend.INSTANCE_STARTED
            else:
                logging.warning(f"aws:EC2 Instance {self.instance_id} cannot be reused from state '{state}'!")

        self.create_instance()
        return VhtBackend.INSTANCE_CREATED

    def prepare_instance(self):
        self._init()
        commands = [
            f"runuser -l ubuntu -c 'cat ~/.bashrc | grep export > {self.AMI_WORKDIR}/vars'",
            f"runuser -l ubuntu -c 'rm -rf {self.AMI_WORKDIR}/workspace'",
            f"runuser -l ubuntu -c 'mkdir -p {self.AMI_WORKDIR}/workspace'",
            f"runuser -l ubuntu -c 'mkdir -p {self.AMI_WORKDIR}/packs/.Web'",
            f"runuser -l ubuntu -c 'wget -N https://www.keil.com/pack/index.pidx -O {self.AMI_WORKDIR}/packs/.Web/index.pidx'",
            "apt update",
            "apt install awscli -y"
        ]
        self.send_remote_command_batch(commands, working_dir=self.AMI_WORKDIR)

    def run_commands(self, cmds: List[str]):
        self._init()
        commands = [f"runuser -l ubuntu -c 'source {self.AMI_WORKDIR}/vars && pushd {self.AMI_WORKDIR}/workspace && {cmd}'" for cmd in cmds]
        self.send_remote_command_batch(commands, working_dir=self.AMI_WORKDIR)

    def upload_workspace(self, filename):
        self._init()
        if isinstance(filename, str):
            filename = Path(filename)
        try:
            self.upload_file_to_cloud(str(filename), filename.name)
            commands = [
                f"runuser -l ubuntu -c 'aws s3 cp s3://{self.s3_bucket_name}/{filename.name} {self.AMI_WORKDIR}/{filename.name}'",
                f"runuser -l ubuntu -c 'cd {self.AMI_WORKDIR}/workspace; tar xvf {self.AMI_WORKDIR}/{filename.name}'",
                f"runuser -l ubuntu -c 'rm -f {self.AMI_WORKDIR}/{filename.name}'"
            ]
            self.send_remote_command_batch(commands, working_dir=self.AMI_WORKDIR)
        finally:
            self.delete_file_from_cloud(filename.name)

    def download_workspace(self, filename):
        self._init()
        if isinstance(filename, str):
            filename = Path(filename)
        try:
            commands = [
                f"runuser -l ubuntu -c 'cd {self.AMI_WORKDIR}/workspace; tar cvjf {self.AMI_WORKDIR}/{filename.name} .'",
                f"runuser -l ubuntu -c 'aws s3 cp {self.AMI_WORKDIR}/{filename.name} s3://{self.s3_bucket_name}/{filename.name}'",
                f"runuser -l ubuntu -c 'rm -f {self.AMI_WORKDIR}/{filename.name}'"
            ]
            self.send_remote_command_batch(commands, working_dir=self.AMI_WORKDIR)
            self.download_file_from_cloud(str(filename), filename.name)
        finally:
            self.delete_file_from_cloud(filename.name)

    def upload_file_to_cloud(self, filename, key):
        """
        Upload a file to a S3 Bucket

        Parameters
        ----------
        String
            filename (Local Filename Path)
            Key (Filepath to be stored on S3 Bucket)

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
        """
        self._init()
        logging.info(f"aws:Upload File {filename} to S3 Bucket {self.s3_bucket_name}, Key {key}")
        self._s3_resource.meta.client.upload_file(filename, self.s3_bucket_name, key)

    def send_remote_command(self, command_list, working_dir, fail_if_unsuccess = True):
        """
        Send a remote command to an EC2 Instance.

        Parameters
        ----------
        List
            command_list (List of commands)
        String
            working_dir (Directory where the remote command will be executed)
        Boolean
            fail_if_unsuccess (Fail the method in case the command failed)

        Return
        ------
            JSON data from send_ssm_shell_command method.

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info(f"vht: command_list = {command_list}")
        response = self.send_ssm_shell_command(
            command_list=command_list,
            working_dir=working_dir
        )

        for i in response.keys():
            logging.info(f"vht:{i} = {response[i].strip()}")
        if response['CommandIdStatus'] != 'Success' and fail_if_unsuccess:
            logging.error(f"Command {command_list} failed")
            raise RuntimeError()

        return response

    def send_remote_command_batch(self, command_list, working_dir, fail_if_unsuccess = True):
        """
        Send batch of remote commands to an EC2 Instance.

        Parameters
        ----------
        List
            command_list (List of List of commands)
        String
            working_dir (Directory where the remote command will be executed)
        Boolean
            fail_if_unsuccess (Fail the method in case the command failed - Default: True)

        Return
        ------
            JSON data from send_ssm_shell_command method.

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info(f"vht: command_list = {command_list}")
        all_responses = []

        for command in command_list:
            all_responses.append(
                self.send_remote_command(
                    command_list=command,
                    working_dir=working_dir,
                    fail_if_unsuccess=fail_if_unsuccess
                )
            )
        logging.debug(f"vht: all_responses = {all_responses}")
        return all_responses

    def send_ssm_shell_command(self,
                               command_list,
                               working_dir='/',
                               return_type='all',
                               timeout_seconds=600):
        """
        Send SSM Shell Commands to a EC2 Instance

        Parameters
        ----------
        String
            command_list (List of commands to be executed on the instance_id)
            working_dir (Working directory - Default: '/')
            return_type (
                Method return types:
                    `all`: Return as a dict: 'CommandId', 'CommandIdStatus', 'CommandList', 'StdOut', 'StdErr' - Default
                    `command_id`: Return only the `command_id` as a String
            )
            timeout_seconds (Command Timeout in Seconds - Default: 600)

        Return
        ----------
        Dict
            if return_type == `all` (Default):
                'CommandId', 'CommandIdStatus', 'CommandList', 'StdOut', 'StdErr'
        String
            if return_type == `command_id`:
                command_id

        More
        ----------
        TODO: Use **kwargs

        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.send_command
            https://docs.aws.amazon.com/systems-manager/latest/userguide/ssm-plugins.html#aws-runShellScript
        """

        command_id = ''
        command_id_status = ''
        stdout_key = ''
        stdout_str = ''
        stderr_key = ''
        stderr_str = ''

        logging.info(f"aws:send_ssm_shell_command:{working_dir}:{command_list}")

        try:
            response = self._ssm_client.send_command(
                InstanceIds=[
                    self.instance_id
                ],
                DocumentName='AWS-RunShellScript',
                Parameters={
                    'workingDirectory': [
                        working_dir,
                    ],
                    'commands': [
                        command_list,
                    ]
                },
                OutputS3BucketName=self.s3_bucket_name,
                OutputS3KeyPrefix=self.s3_keyprefix,
                TimeoutSeconds=timeout_seconds,
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:send_ssm_shell_command:{response}")
        command_id = response['Command']['CommandId']
        logging.info(f"aws:command_id = {command_id}")

        # We need a little bit of time to wait for a command
        time.sleep(2)

        logging.info(f"aws:Waiting command id {command_id} to finish")
        self.wait_ssm_command_finished(command_id)

        logging.info(f"aws:Get command id {command_id} status")
        command_id_status = self.get_ssm_command_id_status(command_id)
        logging.info(f"aws:Command id status = {command_id_status}")

        stdout_key = self.get_s3_ssm_command_id_key(command_id, 'stdout')
        stdout_str = self.get_s3_file_content(stdout_key)

        if command_id_status != 'Success':
            stderr_key = self.get_s3_ssm_command_id_key(command_id, 'stderr')
            stderr_str = self.get_s3_file_content(stderr_key)

        if return_type == 'all':
            return {
                'CommandId' : command_id,
                'CommandIdStatus' : command_id_status,
                'CommandList' : command_list,
                'StdOut' : stdout_str,
                'StdErr': stderr_str
            }
        elif return_type == 'command_id':
            return command_id
        else:
            raise AttributeError(f"Output type '{return_type}' invalid. See docs.")

    def start_instance(self):
        """
        Start an Instance and wait it to become running and status OK

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.start_instances

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info(f"aws:Starting EC2 instance {self.instance_id}")

        try:
            response = self._ec2_client.start_instances(
                InstanceIds=[
                    self.instance_id,
                ]
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:start_ec2_instance:{response}")
        self.wait_ec2_running()
        self.wait_ec2_status_ok()

        return self.instance_id

    def stop_instance(self):
        """
        Stop an Instance and wait it becomes stopped.

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.stop_instances

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.info(f"aws:Stopping EC2 instance {self.instance_id}")

        try:
            response = self._ec2_client.stop_instances(
                InstanceIds=[
                    self.instance_id
                ]
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:stop_instance:{response}")
        self.wait_ec2_stopped()

        return self.instance_id

    def wait_ec2_status_ok(self):
        """
        Wait an EC2 instance to have a Status == OK.

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Waiter.InstanceStatusOk
        """
        logging.info(f"aws:Waiting until EC2 instance id {self.instance_id} Status Ok...")

        try:
            waiter = self._ec2_client.get_waiter('instance_status_ok')
            waiter.wait(
                InstanceIds=[
                    self.instance_id
                ]
            )
        except WaiterError as e:
            raise RuntimeError from e

    def wait_ec2_running(self):
        """
        Wait an EC2 instance to be running

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Waiter.InstanceRunning
        """
        logging.info(f"aws:Waiting until EC2 instance id {self.instance_id} is running...")

        try:
            waiter = self._ec2_client.get_waiter('instance_running')
            waiter.wait(
                InstanceIds=[
                    self.instance_id
                ]
            )
        except WaiterError as e:
            raise RuntimeError from e

    def wait_ec2_stopped(self):
        """
        Wait an EC2 instance to stop

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Instance.wait_until_stopped
        """
        logging.info(f"aws:Waiting until EC2 instance id {self.instance_id} is stopped...")
        instance = boto3.resource('ec2').Instance(self.instance_id)
        instance.wait_until_stopped()

    def wait_ec2_terminated(self):
        """
        Wait an EC2 instance to terminate

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Instance.wait_until_terminated
        """
        logging.info(f"aws:Waiting until EC2 instance id {self.instance_id} is terminated...")
        instance = boto3.resource('ec2').Instance(self.instance_id)
        instance.wait_until_terminated()

    def wait_s3_object_exists(self, key, delay=5, max_attempts=120):
        """
        Wait an S3 Object to exists

        Parameters
        ----------
        String
            key (S3 Keypath)
            delay (Retry delay in seconds - Default: 5)
            max_attemps (Max retry - Default: 120)

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectExists
        """
        try:
            waiter = self._s3_client.get_waiter('object_exists')
            waiter.wait(
                Bucket=self.s3_bucket_name,
                Key=key,
                WaiterConfig={
                    'Delay': delay,
                    'MaxAttempts': max_attempts
                }
            )
        except WaiterError as e:
            raise RuntimeError from e

    def cleanup_instance(self, state):
        self._init()
        if state == VhtBackend.INSTANCE_RUNNING:
            pass
        elif (state == VhtBackend.INSTANCE_STARTED) or self.keep_ec2_instance:
            self.stop_instance()
        else:
            self.terminate_instance()

    def wait_ssm_command_finished(self, command_id, delay=5, max_attempts=120):
        """
        Wait the SSM command to reach a terminal status.
        Parameters
        ----------
        String
            command_id (Command ID)
            delay (Retry delay in seconds - Default: 5)
            max_attemps (Max retry - Default: 120)

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Waiter.CommandExecuted
        """
        try:
            waiter = self._ssm_client.get_waiter('command_executed')
            waiter.wait(
                CommandId=command_id,
                InstanceId=self.instance_id,
                WaiterConfig={
                    'Delay': delay,
                    'MaxAttempts': max_attempts
                }
            )
        except WaiterError:
            if "Failed" in str(WaiterError):
                logging.info("aws:Failed status found while wainting for command id")

    def terminate_instance(self):
        """
        Terminate an Instance and wait it to terminated.

        More
        ----------
        API Definition
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.terminate_instances

        This is a mandatory VHT backend method.
        """
        self._init()
        logging.debug('aws:terminate_instance: DryRun=True to test for permission check')
        try:
            self._ec2_client.terminate_instances(
                InstanceIds=[
                    self.instance_id
                ],
                DryRun=True
            )
        except ClientError as e:
            if 'DryRunOperation' not in str(e):
                raise RuntimeError from e

        logging.info('aws:Terminating EC2 instance...')

        try:
            response = self._ec2_client.terminate_instances(
                InstanceIds=[
                    self.instance_id
                ]
            )
        except ClientError as e:
            raise RuntimeError from e

        logging.debug(f"aws:terminate_instance:{response}")

        self.wait_ec2_terminated()
        return response
