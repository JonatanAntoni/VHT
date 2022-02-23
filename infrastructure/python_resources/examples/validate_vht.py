# -*- coding: utf-8 -*-

import logging
from vht import VHTClient

VERBOSITY = 'INFO'
logging.basicConfig(format='[%(levelname)s]\t%(message)s', level=VERBOSITY)
logging.debug("Verbosity level is set to %s", VERBOSITY)

VHTClient("aws").run()
