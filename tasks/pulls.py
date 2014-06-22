import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import pulls
from pulldb.models import subscriptions

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class StreamSelect(TaskHandler):
    @ndb.tasklet
    def select_stream(self, pull):
        stream_key = subscriptions.subscription_key(
            'default', user=pull.key.parent()
        )
        publisher_query = streams.Stream.query(
            streams.Stream.publisher == pull.publisher,
            ancestor=pull.key.parent()
        )
        volume_query = streams.Stream.query(
            streams.Stream.volume == pull.volume,
            ancestor=pull.key.parent()
        )
        issue_query = streams.Stream.query(
            streams.Stream.issue == pull.issue,
            ancestor=pull.key.parent()
        )
        pub_stream, vol_stream, issue_stream = yield (
            publisher_query.get_async(),
            volume_query.get_async(),
            issue_query.get_async(),
        )
        if issue_stream:
            stream_key = issue_stream.key
        elif vol_stream:
            stream_key = vol_stream.key
        elif pub_stream:
            stream_key = pub_stream.key
        if pull.stream != stream_key:
            pull.stream = stream_key
            yield pull.put_async()
            raise ndb.Return(True)

    def get(self):
        query = pulls.Pull.query(
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
        )
        pull_list = query.map(self.select_stream)
        updated = sum(1 for pull in pull_list if pull)
        message = 'Updated stream for %d of %d pulls' % (
            updated, len(pull_list),
        )
        logging.info(message)
        self.response.write(json.dumps({
            'status': 200,
            'message': message,
        }))

class Validate(TaskHandler):
    @ndb.tasklet
    def drop_invalid(self, pull):
        issue = yield pull.issue.get_async()
        if not issue:
            yield pull.key.delete_async()
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
    Route('/tasks/pulls/update/streams', StreamSelect),
    Route('/tasks/pulls/validate', Validate),
])
