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

import copy
import os
import rsync_backup_cluster.jobs as jobs


class ParentJob(object):
    def __init__(self, queue, destinations):
        self.queue = queue
        self.destinations = destinations

    def enqueue(self):
        q = jobs.job_destination.queue(self.destinations,
                                       timeout=-1,
                                       queue=self.queue)

        return {
            'id': q._id,
            'status': 'queued',
            'queue': self.queue,
            'depends_on': None
        }


class Job(object):
    def __init__(self, data, queue):
        self.data = data

        self.source = data['source']['path']
        self.destination = data['destination']['path']

        self.parent_source = data.get('parent_source', None)
        self.parent_destination = data.get('parent_destination', None)

        self.queue = queue

    def process_destinations(self):
        parent_dst_diff = os.path.relpath(self.destination,
                                          self.parent_destination)
        normalized_dst_diff = os.path.normpath(parent_dst_diff)

        all_parts = normalized_dst_diff.split(os.sep)

        previous_dest = self.parent_destination
        previous_src = self.parent_source

        destinations = []

        for part in all_parts:
            part_path = os.path.join(previous_dest, part)

            if not os.path.isdir(part_path):
                source_part = os.path.join(previous_src, part)
                source_part_stat = os.stat(source_part)

                new_dest_path = (part_path, source_part_stat.st_uid,
                                 source_part_stat.st_gid,
                                 source_part_stat.st_mode)
                destinations.append(new_dest_path)

                previous_src = os.path.join(previous_src, part)

            previous_dest = part_path

        return destinations

    def enqueue(self, depends_on=None):
        meta = {
            'source': self.source,
            'destination': self.destination
        }

        q = jobs.job_backup.queue(self.data, timeout=-1,
                                  queue=self.queue,
                                  meta=meta,
                                  depends_on=depends_on)

        return {
            'id': q._id,
            'status': 'queued',
            'queue': self.queue,
            'depends_on': depends_on
        }


class JobFactory(object):
    def __init__(self, data):
        self.data = data

        source = data.get('source')
        dest = data.get('destination')

        self.source = source.get('path')
        self.destination = dest.get('path')

        self.steps = data.get('steps', None)
        self.queue = data.get('queue', 'default')

        self._exploded = False

        self.jobs = []
        self.destinations = []

        if self.steps is not None and self.steps > 0:
            self._explode_source()
            self._exploded = True

    def _explode_source(self):
        src = self.source
        dst = self.destination

        steps = self.steps

        last_dirs = [src]
        new_sources = []

        while (steps > 0):
            new_dirs = []

            for dir in last_dirs:
                objs = os.listdir(dir)

                for obj in objs:
                    obj_path = os.path.join(dir, obj)

                    if not os.path.isdir(obj_path):
                        continue

                    new_dirs.append(obj_path)

            steps -= 1

            if steps == 0:
                new_sources = new_dirs
            else:
                last_dirs = new_dirs

        for new_src in new_sources:
            src_path_diff = os.path.relpath(new_src, src)
            new_dst = os.path.join(dst, src_path_diff)

            new_data = copy.deepcopy(self.data)

            new_data['source']['path'] = new_src
            new_data['destination']['path'] = new_dst
            new_data['parent_source'] = self.source
            new_data['parent_destination'] = self.destination

            job = Job(new_data, self.queue)
            self.destinations += job.process_destinations()
            self.jobs.append(job)

    def process(self):
        result = None

        if self._exploded:
            j = ParentJob(self.queue, self.destinations)
            result = j.enqueue()

            for job in self.jobs:
                job.enqueue(depends_on=result['id'])
        else:
            j = Job(self.data, self.queue)
            result = j.enqueue()

        return result
