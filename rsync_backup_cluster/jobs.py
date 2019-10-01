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

import os
from rsync_backup_cluster.rq import get_rq
from rsync_backup_cluster.rsync import run_rsync
from rsync_backup_cluster.utils import which


rq = get_rq()


DEFAULT_ALLOWED_RETURNCODES = [0]


@rq.job
def job_destination(destinations):
    for path, uid, gid, mode in destinations:
        if not os.path.isdir(path):
            os.mkdir(path)
            os.chown(path, uid, gid)
            os.chmod(path, mode)


@rq.job
def job_backup(job_data):
    rsync_path = which('rsync')

    if rsync_path is None:
        raise Exception('rsync command not found on worker')

    source = job_data['source']['path']
    dest = job_data['destination']['path']

    exclusions = job_data.get('exclusions', [])
    options = job_data.get('options', [])

    allowed_returncodes = job_data.get(
        'allowed_returncodes', DEFAULT_ALLOWED_RETURNCODES)

    result = run_rsync(rsync_path, source, dest, exclusions,
                       options=options)

    if result not in allowed_returncodes:
        raise Exception('return code %i was not allowed' %
                        result)

    return result
