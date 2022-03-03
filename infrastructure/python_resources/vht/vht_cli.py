#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging

from inspect import signature, Signature
from itertools import islice
from types import FunctionType

from vht import VHTClient


class VhtCli:
    def __init__(self):
        parser = self._parser()

        parser.parse_args()
        args = parser.parse_args()
        if args.verbosity:
            verbosity = args.verbosity
            logging.basicConfig(format='[%(levelname)s]\t%(message)s', level=verbosity)
            logging.debug("Verbosity level is set to %s", verbosity)

        # vht_instance using args.backend
        vht_client = VHTClient(args.backend)

        func = VHTClient.__dict__[args.subcmd.replace('-', '_')]
        params = signature(func).parameters
        func_args = [vars(args)[param.replace('-', '_')] for param in islice(params.keys(), 1, None)]
        func(vht_client, *func_args)

    @staticmethod
    def _parser():
        parser = argparse.ArgumentParser()

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

        subparsers = parser.add_subparsers(dest='subcmd', required=True, help='sub-command help')

        for m, n in VHTClient.__dict__.items():
            if isinstance(n, FunctionType) and not m.startswith('_'):
                subparser = subparsers.add_parser(m.replace('_', '-'), help=n.__doc__)
                params = signature(n).parameters
                for param in islice(params.items(), 1, None):
                    param_type = param[1].annotation if param[1].annotation != Signature.empty else str
                    param_default = param[1].default if param[1].default != Signature.empty else None
                    subparser.add_argument(f"--{param[0].replace('_', '-')}", type=param_type, default=param_default,
                                           required=param_default is None)

        return parser


if __name__ == '__main__':
    VhtCli()
