#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys

from argparse import ArgumentParser, SUPPRESS, Namespace
from gettext import gettext as _
from inspect import signature, Signature
from itertools import islice
from types import FunctionType

from vht import VHTClient, VhtBackend


class VhtCli:
    def __init__(self):
        parser = self._parser()

        args = parser.parse_known_args()[0]
        if args.verbosity:
            verbosity = args.verbosity
            logging.basicConfig(format='[%(levelname)s]\t%(message)s', level=verbosity)
            logging.debug("Verbosity level is set to %s", verbosity)

        # vht_instance using args.backend
        vht_client = VHTClient(args.backend)

        self._add_commands(parser)
        self._add_backend_args(parser, vht_client.backend)

        args = parser.parse_args()

        self._consume_backend_args(vht_client.backend, args)

        func = VHTClient.__dict__[args.subcmd.replace('-', '_')]
        params = signature(func).parameters
        func_args = [vars(args)[param.replace('-', '_')] for param in islice(params.keys(), 1, None)]
        try:
            func(vht_client, *func_args)
        except RuntimeError as e:
            if e.__cause__:
                logging.error(e.__cause__.__doc__)
                logging.error(e.__cause__)
            if str(e):
                logging.error(e)
            sys.exit(1)

    @staticmethod
    def _parser() -> ArgumentParser:
        parser = ArgumentParser(add_help=False)

        parser.add_argument('-v', '--verbosity',
                            type=str,
                            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                            default='INFO',
                            help='Set the output verbosity. Default: INFO')

        parser.add_argument('-b', '--backend',
                            type=str,
                            choices=VHTClient.get_available_backends(),
                            default='aws',
                            help=f'Select VHT backend to use. Default: {VHTClient.get_available_backends()[0]}')

        return parser

    @staticmethod
    def _add_backend_args(parser: ArgumentParser, backend: VhtBackend):
        group = parser.add_argument_group("Backend properties")
        for m, n in backend.__dict__.items():
            if not m.startswith('_'):
                if type(n) is bool:
                    group.add_argument(f"--{m.replace('_', '-')}", action='store_true')
                else:
                    group.add_argument(f"--{m.replace('_', '-')}", default=n, type=str)

    @staticmethod
    def _consume_backend_args(backend: VhtBackend, args: Namespace):
        args = vars(args)
        for m, n in backend.__dict__.items():
            if not m.startswith('_'):
                if m in args:
                    backend.__dict__[m] = args[m]

    @staticmethod
    def _add_commands(parser: ArgumentParser):
        parser.add_argument('-h', '--help', action='help',
                            default=SUPPRESS, help=_('show this help message and exit'))

        subparsers = parser.add_subparsers(dest='subcmd', required=True, help='sub-command help')

        for m, n in VHTClient.__dict__.items():
            if isinstance(n, FunctionType) and not m.startswith('_'):
                subparser = subparsers.add_parser(m.replace('_', '-'), help=n.__doc__)
                params = signature(n).parameters
                for param in islice(params.items(), 1, None):
                    param_type = param[1].annotation if param[1].annotation != Signature.empty else str
                    param_default = param[1].default if param[1].default != Signature.empty else None
                    subparser.add_argument(f"--{param[0].replace('_', '-')}", type=param_type, default=param_default,
                                           required=(param[1].default == Signature.empty))


if __name__ == '__main__':
    VhtCli()
