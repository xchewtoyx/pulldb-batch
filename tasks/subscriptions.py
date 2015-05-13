from datetime import datetime
from functools import partial
import json
import logging
import time
from zlib import crc32

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import admin
from pulldb.models import issues
from pulldb.models import pulls
from pulldb.models import subscriptions

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class QueueActiveVolumes(TaskHandler):
    @ndb.tasklet
    def queue_collections(self, subscription):
        collection = yield subscription.volume.get_async()
        if collection.complete:
            collection.complete = False
            yield collection.put_async()
            raise ndb.Return(collection)

    def active_shard(self, offset=3):
        if self.request.get('shard'):
            return int(self.request.get('shard'))
        else:
            return current_shard(offset=offset)

    def get(self):
        shard = self.active_shard()
        # TODO(rgh): Should restrict this to fresh once fresh is being set
        #            by the subscription refresh task
        query = subscriptions.Subscription.query(
            subscriptions.Subscription.shard == shard,
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


class RefreshSubscription(TaskHandler):
    @ndb.tasklet
    def check_freshness(self, subscription):
        changed = False
        issue_query = issues.Issue.query(
            issues.Issue.volume == subscription.volume
        ).order(-issues.Issue.pubdate)
        last_issue = yield issue_query.get_async()
        logging.debug('Most recent pull for %r is %r[%s]',
                      subscription.key, last_issue.key, last_issue.pubdate)
        if last_issue:
            issue_age = datetime.now().date() - last_issue.pubdate
            if issue_age.days <= 90 and not subscription.fresh:
                logging.debug('Marking fresh: age:%r fresh:%r',
                              issue_age, subscription.fresh)
                subscription.fresh = True
                changed = True
            if subscription.fresh and issue_age.days > 90:
                logging.debug('Marking stale: age:%r fresh:%r',
                              issue_age, subscription.fresh)
                subscription.fresh = False
                changed = True
        raise ndb.Return(changed)

    @ndb.tasklet
    def refresh_subscription(self, subscription):
        changed = False
        freshness_changed = yield self.check_freshness(subscription)
        if freshness_changed:
            changed = True
        if changed:
            update = yield subscription.put_async()
            raise ndb.Return(update)

    def active_shard(self):
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        else:
            shard = current_shard()
        return shard

    def get(self):
        shard = self.active_shard()
        query = subscriptions.Subscription.query(
            subscriptions.Subscription.shard == shard
        )
        shard_count = query.count_async()
        updates = query.map(self.refresh_subscription)
        update_count = sum(1 for update in updates if update)
        shard_count = shard_count.get_result()
        message = 'updated %d of %d subscriptions in shard %d' % (
            update_count, shard_count, shard)
        logging.info(message)
        self.response.write({
            'status': 200,
            'message': message,
            'total': shard_count,
            'updated': update_count,
            'shard': shard,
        })


class ReshardSubs(TaskHandler):
    @ndb.tasklet
    def reshard_subs(self, subscription):
        changed = False
        if subscription.identifier:
            sub_id = subscription.identifier
        else:
            sub_id = int(subscription.key.id())
            subscription.identifier = sub_id
            changed = True
        seed = crc32(str(sub_id))
        if seed % 24 != subscription.shard:
            subscription.shard = seed % 24
            changed = True
        if changed:
            update = yield subscription.put_async()
            raise ndb.Return(update)

    def get(self):
        query = subscriptions.Subscription.query()
        updates = query.map(self.reshard_subs)
        update_count = sum(1 for update in updates if update)
        message = 'Resharded %d of %d subscriptions' % (
            update_count, len(updates)
        )
        logging.info(message)
        self.response.write(json.dumps({
            'status': 200,
            'message': message,
        }))

class UpdateSubs(TaskHandler):
    @ndb.tasklet
    def check_pulls(self, subscription):
        issue_query = issues.Issue.query(
            issues.Issue.volume == subscription.volume,
            issues.Issue.pubdate > subscription.start_date,
        )
        pull_query = pulls.Pull.query(
            pulls.Pull.subscription == subscription.key,
        )
        issue_list, pull_list = yield (
            issue_query.fetch_async(keys_only=True),
            pull_query.fetch_async(),
        )
        pulled_issues = [pull.issue for pull in pull_list]
        new_pulls = []
        for issue in issue_list:
            if issue not in pulled_issues:
                new_pulls.append((issue, subscription))
        if new_pulls:
            raise ndb.Return(new_pulls)

    def get(self, request_type=None):
        shard = datetime.now().hour
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        logging.info('Updating subscription shard %d' % shard)
        query = subscriptions.Subscription.query(
            subscriptions.Subscription.shard == shard,
        )
        pull_list = query.map(self.check_pulls)
        candidates = []
        for subscription_pulls in pull_list:
            if subscription_pulls:
                candidates.extend(subscription_pulls)
        logging.info('adding %d pulls', len(candidates))
        new_pulls = []
        for issue, subscription in candidates:
            user_key = subscription.key.parent()
            new_pulls.append(
                pulls.pull_key(issue, user=user_key, create=True, batch=True)
            )
        logging.debug('Adding pulls: %r', new_pulls)
        ndb.put_multi(new_pulls)
        self.response.write(json.dumps({
            'status': 200,
            'updated': len(new_pulls),
        }))

class Validate(TaskHandler):
    @ndb.tasklet
    def drop_invalid(self, subscription):
        volume = yield subscription.volume.get_async()
        if not volume:
            yield subscription.key.delete_async()
            raise ndb.Return(True)

    def get(self):
        query = subscriptions.Subscription.query()
        results = query.map(self.drop_invalid)
        deleted = sum(1 for deleted in results if deleted)
        logging.info('Deleted %d invalid entries.', deleted)
        self.response.write(json.dumps({
            'status': 200,
            'seen': len(results),
            'deleted': deleted,
        }))


def current_shard(offset=0):
    shard_key = admin.Setting.query(
        admin.Setting.name == 'watch_shard_count')
    shard_count = int(shard_key.get().value)
    shard = (int(time.time() // 3600) + offset) % shard_count
    return shard

app = create_app([
    Route('/tasks/subscriptions/queue/volumes', QueueActiveVolumes),
    Route('/tasks/subscriptions/refresh', RefreshSubscription),
    Route('/tasks/subscriptions/reshard', ReshardSubs),
    Route('/<:batch|tasks>/subscriptions/update', UpdateSubs),
    Route('/tasks/subscriptions/validate', Validate),
])
