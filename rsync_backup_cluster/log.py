# -*- coding: utf-8 -*-

# Copyright (C) 2019 Tobias Urdin
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import os


def setup_log(debug, logfile):
    if debug:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    if logfile is not None:
        logfile = os.path.expanduser(logfile)

    log_format = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(format=log_format, level=loglevel, filename=logfile)
