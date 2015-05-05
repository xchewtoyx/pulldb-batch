#pylint: disable=missing-docstring
from functools import partial
import json
import logging
import time
from zlib import crc32

from google.appengine.api import search
from google.appengine.ext import ndb

from pulldb.base import create_app, Route, TaskHandler
from pulldb.models.admin import Setting
from pulldb.models import arcs
from pulldb.models import comicvine
from pulldb.models import issues

class QueueArcs(TaskHandler):
    @ndb.tasklet
    def queue_arc(self, arc): # pylint: disable=no-self-use
        if arc.complete:
            arc.complete = False
            arc_key = yield arc.put_async()
            raise ndb.Return(arc_key)

    def shard_filter(self):
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        else:
            shard_count = Setting.query(
                Setting.name == 'arc_shard_count').get()
            current_hour = time.time() // 3600
            logging.info('Current hour: %d, Shard Count: %r',
                         current_hour, shard_count)
            shard = int(time.time() // 3600) % int(shard_count.value)
        logmsg = 'marking arcs in shard %s for refresh'
        logging.info(logmsg, shard)

        return shard

    def get(self):
        shard = self.shard_filter()
        arc_query = arcs.StoryArc.query(arcs.StoryArc.shard == shard)
        results = arc_query.map(self.queue_arc)
        updates = sum(1 for update in results if update)

        status = 'Queued %d of %d arcs in shard %d' % (
            updates, len(results), shard)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))


class RefreshArcs(TaskHandler):
    @ndb.tasklet
    def find_new_issues(self, issue_list):
        new_ids = []
        for issue in issue_list:
            if not issues.issue_key(issue, create=False):
                new_ids.append(issue['id'])
        issue_dicts = cv_api.fetch_issue_batch(issue_ids)
        new_issues = []
        for issue in issue_dicts:
            new_issues.append(
                issues.issue_key(issue, create=True, batch=True))
        issue_keys = yield new_issues
        raise ndb.Return(issue_keys)

    @ndb.tasklet
    def check_arcs(self, arc):
        # pylint: disable=no-self-use,unused-variable
        cv_api = comicvine.load()
        arc_dict = yield cv_api.fetch_story_arc_async(
            int(arc.identifier))
        if not arc_dict:
            arc_updated = False
            logging.warn('Cannot update arc: %r', arc.key)
        else:
            arc_updated, last_update = arc.has_updates(arc_dict)
            if arc_updated:
                logging.debug('StoryArc %r updated', arc.key)
                arc.apply_changes(arc_dict)
            arc.complete = True
            new_issues = self.find_new_issues(arc_dict['issues'])
            yield arc.put_async()
        raise ndb.Return(arc_updated)

    @ndb.toplevel
    def get(self, *args):
        arc_query = arcs.StoryArc.query(
            arcs.StoryArc.complete == False
        )
        incomplete_arcs = arc_query.count_async()
        results = arc_query.map(self.check_arcs, limit=30)
        updates = sum(1 for update in results if update)

        status = 'Updated %d of %d queued arcs' % (
            updates, incomplete_arcs.get_result())
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))


class Reindex(TaskHandler):
    def get(self): #pylint: disable=no-self-use
        query = arcs.StoryArc.query(
            arcs.StoryArc.indexed == False
        )
        # index.put can only update 200 docs at a time
        arcs_future = query.fetch_async(limit=200)
        index = search.Index(name='arcs')
        reindex_list = []
        arc_list = []
        for arc in arcs_future.get_result():
            reindex_list.append(
                arc.index_document(batch=True)
            )
            arc.indexed = True
            arc_list.append(arc)
        logging.info('Reindexing %d arcs', len(arc_list))
        if len(reindex_list):
            try:
                index.put(reindex_list)
            except search.Error as error:
                logging.error('index update failed: %r', error)
                logging.exception(error)
            else:
                ndb.put_multi(arc_list)
        self.response.write(json.dumps({
            'status': 200,
            'message': 'Indexed %d of %d arcs' % (
                len(reindex_list), len(arc_list))
        }))


class ReshardArcs(TaskHandler):
    @ndb.tasklet
    def reshard_task(self, shards, arc): #pylint: disable=no-self-use
        seed = crc32(str(arc.identifier))
        shard = seed % shards
        if arc.shard != shard:
            arc.shard = shard
            result = yield arc.put_async()
            raise ndb.Return(result)

    def get(self):
        shards_key = Setting.query(Setting.name == 'arc_shard_count').get()
        shards = int(shards_key.value)
        callback = partial(self.reshard_task, shards)
        query = arcs.StoryArc.query()
        if not self.request.get('all'):
            query = query.filter(arcs.StoryArc.shard == -1)
        results = query.map(callback)
        logging.info('Resharded %d arcs', len(results))
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d arcs resharded' % len(results),
        }))


app = create_app([ #pylint: disable=invalid-name
    Route('/tasks/arcs/queue', QueueArcs),
    Route(
        '/<:batch|tasks>/arcs/refresh',
        RefreshArcs
    ),
    Route('/tasks/arcs/reindex', Reindex),
    Route('/tasks/arcs/reshard', ReshardArcs),
])
