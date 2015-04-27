from datetime import datetime
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import pulls
from pulldb.models import streams
from pulldb.models import subscriptions
from pulldb.varz import VarzContext

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class StreamTotals(TaskHandler):
    @ndb.tasklet
    def count_unread(self, stream):
        stream_changed = False

        query = pulls.Pull.query(
            pulls.Pull.stream == stream.key,
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
            ancestor=stream.key.parent(),
        ).order(pulls.Pull.pubdate)

        count_pulls = query.count_async()
        fetch_first = query.fetch_async(limit=1)
        pull_count, first_pull = yield count_pulls, fetch_first

        if pull_count:
            first_pull = first_pull[0]

        if pull_count and first_pull.pubdate:
            if first_pull.pubdate != stream.start:
                stream.start = first_pull.pubdate
                stream_changed = True

        if stream.length != pull_count:
            stream.length = pull_count
            stream_changed = True

        if stream_changed:
            yield stream.put_async()
            raise ndb.Return(True)

    @VarzContext('streams')
    def get(self, *args):
        varz = self.varz
        varz.action = 'updatecounts'
        query = streams.Stream.query()
        results = query.map(self.count_unread)
        changed = sum(1 for result in results if result)
        streams_seen = len(results)
        varz.streams = streams_seen
        varz.changed = changed
        logging.info('Saw %d streams, with %d changes' % (
            streams_seen, changed
        ))
        self.response.write(json.dumps({
            'status': 200,
            'seen': streams_seen,
            'changed': changed,
        }))

class StreamWeights(TaskHandler):
    def pull_key(self, pull):
        return pull.pubdate

    @ndb.tasklet
    def update_weight(self, stream):
        stream_pulls = yield pulls.Pull.query(
            pulls.Pull.stream == stream.key,
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
            ancestor=stream.key.parent(),
        ).order(pulls.Pull.pubdate).fetch_async()
        changed = []
        stream_length = max([stream.length, 1])
        for index, pull in enumerate(stream_pulls):
            weight = (index + 0.5) / stream_length
            if pull.weight != weight:
                pull.weight = weight
                changed.append(pull)
        yield ndb.put_multi_async(changed)
        raise ndb.Return(len(changed))

    @VarzContext('streams')
    def get(self, *args):
        varz = self.varz
        varz.action = 'updatecounts'
        query = streams.Stream.query()
        update_list = query.map(self.update_weight)
        updated = sum(count for count in update_list if count)
        streams_seen = len(update_list)
        varz.updated = updated
        varz.streams = streams_seen
        message = 'Updated weights for %d pulls in %d streams' % (
            updated, streams_seen,
        )
        logging.info(message)
        self.response.write(json.dumps({
            'status': 200,
            'message': message,
            'streams': streams_seen,
            'updated': updated,
        }))

app = create_app([
    Route('/<:batch|tasks>/streams/updatecounts', StreamTotals),
    Route('/<:batch|tasks>/streams/updateweights', StreamWeights),
])
