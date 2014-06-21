import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import pulls
from pulldb.models import subscriptions

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class Validate(TaskHandler):
    @ndb.tasklet
    def drop_invalid(self, pull):
        issue = yield pull.issue.get_async()
        if not issue:
            yield subscription.key.delete_async()
            raise ndb.Return(True)
        if pull.subscription:
            legacy_key = ndb.Key(
                pulls.Pull, pull.key.id(),
                parent=pull.subscription
            )
            legacy_pull = yield legacy_key.get_async()
            if legacy_pull:
                yield legacy_key.delete_async()
                raise ndb.Return(True)

    def get(self):
        query = pulls.Pull.query()
        results = query.map(self.drop_invalid)
        deleted = sum(1 for deleted in results if deleted)
        logging.info('Deleted %d invalid entries.', deleted)
        self.response.write(json.dumps({
            'status': 200,
            'seen': len(results),
            'deleted': deleted,
        }))

app = create_app([
    Route('/tasks/pulls/validate', Validate),
])
