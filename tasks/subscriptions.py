from datetime import datetime
from functools import partial
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import issues
from pulldb.models import pulls
from pulldb.models import subscriptions

# pylint: disable=W0232,C0103,E1101,R0201,R0903

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
        if sub_id % 24 != subscription.shard:
            subscription.shard = sub_id % 24
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

app = create_app([
    Route('/tasks/subscriptions/reshard', ReshardSubs),
    Route('/<:batch|tasks>/subscriptions/update', UpdateSubs),
    Route('/tasks/subscriptions/validate', Validate),
])
