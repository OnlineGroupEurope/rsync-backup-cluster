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
import logging
import six
import sys
import redis
import yaml
from voluptuous import Schema, Invalid


LOG = logging.getLogger(__name__)


def validate_redis(value):
    try:
        rs = redis.Redis().from_url(value)
        rs.ping()
    except Exception as exc:
        raise Invalid(six.text_type(exc))

    return value


def validate_queues(value):
    if not isinstance(value, list):
        raise Invalid('queues must be a list')

    for val in value:
        if not isinstance(val, str):
            raise Invalid('all queues must be strings')

    return value


common_schema = Schema({
    'database': validate_redis,
})


scheduler_schema = common_schema


worker_schema = common_schema.extend({
    'queues': validate_queues
})


def load_config(path, component):
    expanded_path = os.path.expanduser(path)
    if os.path.exists(expanded_path) is False:
        LOG.error('config file %s does not exist' % (
                  expanded_path))
        sys.exit(1)

    try:
        with open(expanded_path) as f:
            data = yaml.safe_load(f)
    except IOError as exc:
        LOG.error('failed to open config file %s: %s' % (
                  expanded_path, six.text_type(exc)))
        sys.exit(1)
    except yaml.YAMLError as exc:
        LOG.error('failed to parse yaml in %s: %s' % (
                  expanded_path, six.text_type(exc)))
        sys.exit(1)
    except Exception as exc:
        LOG.error('failed to load config %s: %s' % (
                  expanded_path, six.text_type(exc)))
        sys.exit(1)

    if component == 'scheduler':
        schema = scheduler_schema
    elif component == 'worker':
        schema = worker_schema
    else:
        raise Exception('invalid component to load_config')

    try:
        schema(data)
    except Exception as exc:
        LOG.error('failed to validate config %s: %s' % (
                  expanded_path, six.text_type(exc)))
        sys.exit(1)

    return data
