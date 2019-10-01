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

from flask import Flask
from rsync_backup_cluster.rq import get_rq
from rsync_backup_cluster.scheduler.jobs import bp as jobs_bp
from rsync_backup_cluster.scheduler.workers import bp as workers_bp
from rsync_backup_cluster.scheduler.queues import bp as queues_bp


app = Flask(__name__)


@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    return response


def load_app(config):
    app.config['RQ_REDIS_URL'] = config['database']

    rq = get_rq()
    rq.init_app(app)

    app.register_blueprint(jobs_bp)
    app.register_blueprint(workers_bp)
    app.register_blueprint(queues_bp)

    return app
