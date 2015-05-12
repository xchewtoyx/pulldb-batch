# pylint: disable=missing-docstring
from functools import partial
import json
import logging
import time
from zlib import crc32

from google.appengine.ext import ndb

from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import admin
from pulldb.models import issues
from pulldb.models import pulls
from pulldb.models import subscriptions

class QueueActiveCollections(TaskHandler):
    @ndb.tasklet
    def queue_collections(self, watch):
        collection = yield watch.collection.get_async()
        if collection.complete:
            collection.complete = False
            yield collection.put_async()
            raise ndb.Return(collection)

    def active_shard(self, offset=3):
        if self.request.get('shard'):
            return int(self.request.get('shard'))
        shard_key = admin.Setting.query(
            admin.Setting.name == 'watch_shard_count')
        shard_count = int(shard_key.get().value)
        shard = (int(time.time() // 3600) + offset) % shard_count
        return shard

    def get(self):
        shard = self.active_shard()
        query = subscriptions.WatchList.query(
            subscriptions.WatchList.shard == shard,
        )
        count_future = query.count_async()
        queued = query.map(self.queue_collections)
        collection_count = count_future.get_result()
        updated = sum(1 for collection in queued if collection)
        message = 'Updated %d of %d collections in shard %d' % (
            updated, collection_count, shard)
        logging.info(message)
        self.response.write({
            'status': 200,
            'message': message,
            'total': collection_count,
            'queued': updated,
            'shard': shard,
        })


class ReshardWatches(TaskHandler):
    @ndb.tasklet
    def reshard_watch(self, watch): # pylint: disable=no-self-use
        changed = False
        shard_key = admin.Setting.query(
            admin.Setting.name == 'watch_shard_count').get()
        shard_count = int(shard_key.value)
        seed = crc32(watch.key.urlsafe())
        if seed % shard_count != watch.shard:
            watch.shard = seed % shard_count
            changed = True
        if changed:
            update = yield watch.put_async()
            raise ndb.Return(update)

    def get(self):
        query = subscriptions.WatchList.query()
        updates = query.map(self.reshard_watch)
        update_count = sum(1 for update in updates if update)
        message = 'Resharded %d of %d watches' % (
            update_count, len(updates)
        )
        logging.info(message)
        self.response.write(json.dumps({
            'status': 200,
            'message': message,
        }))

class UpdateWatches(TaskHandler):
    @ndb.tasklet
    def issue_context(self, watch, issue): # pylint: disable=no-self-use
        pull_query = pulls.Pull.query(
            pulls.Pull.identifier == issue.identifier,
            ancestor=watch.user
        )
        pull = yield pull_query.get_async()
        raise ndb.Return({
            'pull': pull,
            'issue': issue,
        })

    @ndb.tasklet
    def check_pulls(self, watch):
        issue_query = issues.Issue.query(
            issues.Issue.collection == watch.collection,
            issues.Issue.pubdate > watch.start_date,
        )
        context_callback = partial(self.issue_context, watch)
        issue_contexts = issue_query.map(context_callback)

        new_pulls = []
        for context in issue_contexts:
            if not context['pull']:
                new_pulls.append((context['issue'], watch))
        if new_pulls:
            raise ndb.Return(new_pulls)

    def get(self, request_type=None):
        shard_key = admin.Setting.query(
            admin.Setting.name == 'watch_shard_count').get()
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        else:
            shard = int(time.time() // 3600) % int(shard_key.value)
        logging.info('Updating watch shard %d', shard)
        query = subscriptions.WatchList.query(
            subscriptions.WatchList.shard == shard,
        )
        pull_list = query.map(self.check_pulls)
        candidates = []
        for watch_pulls in pull_list:
            if watch_pulls:
                candidates.extend(watch_pulls)
        logging.info('adding %d pulls', len(candidates))
        new_pulls = []
        for issue, watch in candidates:
            new_pulls.append(
                pulls.pull_key(issue, user=watch.user, create=True, batch=True)
            )
        logging.debug('Adding pulls: %r', new_pulls)
        ndb.put_multi(new_pulls)
        self.response.write(json.dumps({
            'status': 200,
            'updated': len(new_pulls),
        }))


app = create_app([ # pylint: disable=invalid-name
    Route('/tasks/watches/queue/collections', QueueActiveCollections),
    Route('/tasks/watches/reshard', ReshardWatches),
    Route('/<:batch|tasks>/watches/update', UpdateWatches),
])
