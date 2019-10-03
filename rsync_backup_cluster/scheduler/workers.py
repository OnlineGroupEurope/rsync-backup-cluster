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

from flask import Blueprint, jsonify
from rsync_backup_cluster.rq import get_rq
import six


bp = Blueprint('workers', __name__)


def all_workers(rq):
    from rq.worker import Worker
    return Worker.all(connection=rq.connection)


def create_worker_obj(worker):
    queues = []
    for q in worker.queues:
        queues.append(q.name)
    return {
        'name': six.text_type(worker.name),
        'state': six.text_type(worker.state),
        'shutdown_requested_date': six.text_type(
            worker.shutdown_requested_date),
        'failed_job_count': worker.failed_job_count,
        'successful_job_count': worker.successful_job_count,
        'queues': queues
    }


@bp.route('/workers')
def get():
    rq = get_rq()
    workers = all_workers(rq)
    result = []
    for worker in workers:
        result.append(create_worker_obj(worker))
    return jsonify(result)
