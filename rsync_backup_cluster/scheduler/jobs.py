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

from flask import Blueprint, jsonify, request, make_response
from rsync_backup_cluster.rq import get_rq
from rsync_backup_cluster.scheduler.schema import job_schema
from rsync_backup_cluster.job_factory import JobFactory
from rq.registry import FailedJobRegistry
from rq.queue import Queue
from rq import job as rq_job
import six


bp = Blueprint('jobs', __name__)


def _create_queue_job_obj(job):
    return {
        'id': job.id,
        'source': job.meta['source'],
        'destination': job.meta['destination'],
        'enqueued_at': job.enqueued_at
    }


def _get_queue(queue_name='default'):
    rqc = get_rq()
    queue = rqc.get_queue(name=queue_name)
    jobs = queue.get_jobs()
    result = []
    for job in jobs:
        result.append(_create_queue_job_obj(job))
    return jsonify(result)


def _create_failed_job_obj(j):
    return {
        'source': j.meta['source'],
        'destination': j.meta['destination'],
        'enqueued_at': j.enqueued_at,
        'created_at': j.created_at,
        'started_at': j.started_at,
        'ended_at': j.ended_at,
        'origin': j.origin,
        'status': j._status.decode('utf8'),
        'result': j._result,
        'exc_info': j.exc_info
    }


def _get_failed_jobs(connection):
    queues = Queue.all(connection=connection)
    failed_jobs = []

    for q in queues:
        registry = FailedJobRegistry(q.name,
                                     connection=connection)

        job_ids = registry.get_job_ids()

        for id in job_ids:
            j = rq_job.Job.fetch(id, connection=connection)
            failed_jobs.append(_create_failed_job_obj(j))

    return failed_jobs


@bp.route('/jobs/queue', methods=['GET'])
def jobs_queue_get_default():
    return jsonify(_get_queue())


@bp.route('/jobs/queue/<string:queue_name>', methods=['GET'])
def jobs_queue_get(queue_name):
    return jsonify(_get_queue(queue_name))


@bp.route('/jobs/failed', methods=['GET'])
def jobs_failed_get():
    rqc = get_rq()
    return jsonify(_get_failed_jobs(rqc.connection))


@bp.route('/jobs', methods=['POST'])
def post():
    data = request.get_json()

    try:
        job_schema(data)
    except Exception as exc:
        return make_response(six.text_type(exc), 400)

    factory = JobFactory(data)
    job = factory.process()

    return jsonify(job), 201
