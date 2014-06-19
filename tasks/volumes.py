from datetime import date, datetime
from functools import partial
import json
import logging
import math

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models.admin import Setting
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import volumes

# pylint: disable=W0232,C0103,E1101,R0201,R0903,W0201

class RefreshVolumes(TaskHandler):
    @ndb.tasklet
    def volume_issues(self, volume):
        volume_issues = yield issues.Issue.query(
            ancestor=volume.key).fetch_async()
        issue_ids = [issue.identifier for issue in volume_issues]
        raise ndb.Return({
            'volume': volume.identifier,
            'volume_key': volume.key,
            'ds_issues': issue_ids,
        })

    def fetch_issues(self, volume, limit=100):
        all_issues = volume.get('issues', [])
        if not all_issues:
            last_page = math.ceil(1.0*volume['count_of_issues']/limit)
            for page in range(int(last_page)):
                issue_page = self.cv.fetch_issue_batch(
                    [volume['id']], filter_attr='volume', page=page)
                all_issues.extend(issue_page)
        return all_issues

    def get(self, shard_count=None, shard=None):
        # TODO(rgh): refactor this method
        # pylint: disable=R0914
        if not shard_count:
            # When run from cron cycle over all issues weekly
            shard_count = 24 * 7
            shard = datetime.today().hour + 24 * date.today().weekday()
        self.cv = comicvine.load()
        query = volumes.Volume.query(
            volumes.Volume.shard == int(shard)
        )
        results = query.map(self.volume_issues)
        sharded_ids = [result['volume'] for result in results]
        volume_detail = {}
        for result in results:
            volume_detail[result['volume']] = result
        cv_volumes = []
        for index in range(0, len(sharded_ids), 100):
            volume_page = sharded_ids[index:min(index+100, len(sharded_ids))]
            cv_volumes.extend(self.cv.fetch_volume_batch(volume_page))
        for comicvine_volume in cv_volumes:
            logging.debug('checking for new issues in %r', comicvine_volume)
            comicvine_id = comicvine_volume['id']
            comicvine_issues = self.fetch_issues(comicvine_volume)
            volume_detail[int(comicvine_id)]['cv_issues'] = comicvine_issues
            volumes.volume_key(comicvine_volume, create=False, reindex=True)
        new_issues = []
        for detail in volume_detail.values():
            for issue in detail['cv_issues']:
                if int(issue['id']) not in detail['ds_issues']:
                    new_issues.append((issue, detail['volume_key']))
        for issue, volume_key in new_issues:
            issues.issue_key(issue, volume_key=volume_key)
        status = 'Updated %d volumes. Found %d new issues' % (
            len(sharded_ids), len(new_issues)
        )
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))

class ReshardVolumes(TaskHandler):
    @ndb.tasklet
    def reshard_task(self, shards, volume):
        volume.shard = volume.identifier % shards
        result = yield volume.put_async()
        raise ndb.Return(result)

    def get(self):
        shards_key = Setting.query(Setting.name == 'update_shards_key').get()
        shards = int(shards_key.value)
        callback = partial(self.reshard_task, shards)
        query = volumes.Volume.query()
        if not self.request.get('all'):
            query = query.filter(volumes.Volume.shard == -1)
        results = query.map(callback)
        logging.info('Resharded %d volumes', len(results))
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d volumes resharded' % len(results),
        }))

class Validate(TaskHandler):
    @ndb.tasklet
    def drop_invalid(self, volume):
        if volume.key.id() != str(volume.identifier):
            yield volume.key.delete_async()
            raise ndb.Return(True)

    def get(self):
        query = volumes.Volume.query()
        results = query.map(self.drop_invalid)
        deleted = sum(1 for deleted in results if deleted)
        self.response.write(json.dumps({
            'status': 200,
            'deleted': deleted,
        }))

app = create_app([
    Route('/tasks/volumes/refresh', RefreshVolumes),
    Route('/tasks/volumes/reshard', ReshardVolumes),
    Route('/tasks/volumes/validate', Validate),
])
