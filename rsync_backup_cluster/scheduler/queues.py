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
from rq.registry import DeferredJobRegistry


bp = Blueprint('queues', __name__)


def _all_workers(rq):
    from rq.worker import Worker
    return Worker.all(connection=rq.connection)


def _all_queues(rq):
    from rq.queue import Queue
    return Queue.all(connection=rq.connection)


def _get_deferred_count(rq, queue_name='default'):
    reg = DeferredJobRegistry(queue_name,
                              connection=rq.connection)
    return reg.count


@bp.route('/queues')
def get():
    rq = get_rq()
    workers = _all_workers(rq)
    result = []
    queues = {}
    for worker in workers:
        for queue in worker.queues:
            if queue.name not in queues:
                queues[queue.name] = {
                    'workers': 0,
                }
            queues[queue.name]['workers'] += 1
            queues[queue.name]['count'] = queue.count
            for state in ['idle', 'busy', 'suspended']:
                state_key = '%s_workers' % state
                if state_key not in queues[queue.name]:
                    queues[queue.name][state_key] = 0
            state_key = '%s_workers' % worker.state
            if state_key not in queues[queue.name]:
                queues[queue.name][state_key] = 0
            queues[queue.name][state_key] += 1
    all = _all_queues(rq)
    for a in all:
        if a.name not in queues:
            queues[a.name] = {
                'count': a.count,
                'workers': 0
            }
    for q in queues:
        data = {'name': q}
        data.update(queues[q])
        data['deferred'] = _get_deferred_count(rq, q)
        result.append(data)
    return jsonify(result)
