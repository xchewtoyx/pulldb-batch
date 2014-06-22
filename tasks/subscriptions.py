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
    def locate_pulls(self, subscription, issue):
        issue_key = issue.key
        user_key = subscription.key.parent()
        pull_key = pulls.pull_key(issue_key, user=user_key, create=False)
        pull = yield pull_key.get_async()
        if not pull:
            pull = pulls.pull_key(
                    issue_key, user=user_key, create=True, batch=True
            )
            raise ndb.Return(pull)

    @ndb.tasklet
    def check_pulls(self, subscription):
        issue_query = issues.Issue.query(
            issues.Issue.volume == subscription.volume,
            issues.Issue.pubdate > subscription.start_date,
        )
        pull_callback = partial(self.locate_pulls, subscription)
        new_pulls = issue_query.map(pull_callback)
        new_pulls = [pull for pull in new_pulls if pull]
        if new_pulls:
            logging.info('Adding %d new pulls for sub %r',
                         len(new_pulls), subscription.key)
            yield ndb.put_multi_async(new_pulls)
        raise ndb.Return(new_pulls)

    def get(self):
        current_shard = datetime.now().hour
        query = subscriptions.Subscription.query(
            subscriptions.Subscription.shard == current_shard,
        )
        updates = query.map(self.check_pulls)
        self.response.write(json.dumps({
            'status': 200,
            'updated': len(updates),
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
    Route('/tasks/subscriptions/update', UpdateSubs),
    Route('/tasks/subscriptions/validate', Validate),
])
