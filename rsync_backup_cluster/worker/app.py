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

import redis
import rq
import socket
import uuid


def load_app(config):
    database = config.get('database')
    queues = config.get('queues', ['default'])

    redis_conn = redis.from_url(database)

    hostname = socket.gethostname()
    name = '%s-%s' % (hostname, uuid.uuid4().hex)

    with rq.Connection(redis_conn):
        worker = rq.Worker(map(rq.Queue, queues), name=name)

    return worker
