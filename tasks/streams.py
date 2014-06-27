import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models import pulls
from pulldb.models import streams
from pulldb.models import subscriptions

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class StreamTotals(TaskHandler):
    @ndb.tasklet
    def count_unread(self, stream):
        query = pulls.Pull.query(
            pulls.Pull.stream == stream.key,
            pulls.Pull.pulled == True,
            pulls.Pull.read == False,
            ancestor=stream.key.parent(),
        )
        matches = yield query.fetch_async(keys_only=True)
        if not stream.length or stream.length != len(matches):
            stream.length = len(matches)
            yield stream.put_async()
            raise ndb.Return(True)

    def get(self):
        query = streams.Stream.query()
        results = query.map(self.count_unread)
        changed = sum(1 for result in results if result)
        logging.info('Saw %d streams, with %d changes' % (
            len(results), changed,
        ))
        self.response.write(json.dumps({
            'status': 200,
            'seen': len(results),
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
            weight = float(index) / stream_length
            if pull.weight != weight:
                pull.weight = weight
                changed.append(pull)
        yield ndb.put_multi_async(changed)
        raise ndb.Return(len(changed))

    def get(self):
        query = streams.Stream.query()
        update_list = query.map(self.update_weight)
        updated = sum(count for count in update_list if count)
        message = 'Updated weights for %d pulls in %d streams' % (
            updated, len(update_list),
        )
        logging.info(message)
        self.response.write(json.dumps({
            'status': 200,
            'message': message,
        }))

app = create_app([
    Route('/tasks/streams/updatecounts', StreamTotals),
    Route('/tasks/streams/updateweights', StreamWeights),
])
