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

class UpdateSubs(TaskHandler):
    @ndb.tasklet
    def locate_pulls(self, subscription, issue):
        pull_key = pulls.pull_key(
            issue.key.id(), user=subscription.key.parent(), create=False
        )
        pull = yield pull_key.get_async()
        if not pull:
            raise ndb.Return(pull_key)

    @ndb.tasklet
    def check_pulls(self, subscription):
        issue_query = issues.Issue.query(
            issues.Issue.volume == subscription.volume,
            issues.Issue.pubdate > subscription.start_date,
        )
        pull_callback = partial(self.locate_pulls, subscription)
        new_pulls = issue_query.map(pull_callback)
        raise ndb.Return([pull for pull in new_pulls if pull])

    def get(self):
        query = subscriptions.Subscription.query()
        new_pulls = query.map(self.check_pulls)
        pull_count = sum(len(pull) for pull in new_pulls if pull)
        self.response.write(json.dumps({
            'status': 200,
            'seen': len(new_pulls),
            'updated': pull_count,
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
    Route('/tasks/subscriptions/update', UpdateSubs),
    Route('/tasks/subscriptions/validate', Validate),
])
