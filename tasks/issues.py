from datetime import datetime, date, timedelta
from functools import partial
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import Route, TaskHandler, create_app
from pulldb.models.admin import Setting
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import volumes

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class FetchNew(TaskHandler):
    def volume_list(self, issue_list):
        volume_keys = [
            self.volume_key(issue['volume']) for issue in issue_list
        ]
        volume_entries = ndb.get_multi(volume_keys)
        return [volume.key.id() for volume in volume_entries if volume]

    def volume_key(self, volume_id):
        return ndb.Key(
            volumes.Volume, str(volume_id)
        )

    def find_candidates(self, new_issues):
        volume_list = self.volume_list(new_issues)
        candidates = [
            (issue, ndb.Key(
                issues.Issue, str(issue['id']),
                parent=self.volume_key(issue['volume'])
            )) for issue in new_issues if str(issue['volume']) in volume_list
        ]
        # Pre-cache issues
        ndb.get_multi_async(candidates)
        return candidates, volume_list

    def get(self):
        cv = comicvine.load()
        today = date.today()
        yesterday = today - timedelta(1)
        new_issues = cv.fetch_issue_batch(
            [yesterday.isoformat(), today.isoformat()],
            filter_attr='date_added',
            deadline=60
        )
        candidates, volume_list = self.find_candidates(new_issues)
        added_issues = []
        skipped_issues = []
        for issue, key in candidates:
            issue = key.get()
            if issue:
                skipped_issues.append(key.id())
                continue
            if key.parent().id() in volume_list:
                key = issues.issue_key(issue, key.parent())
                added_issues.append(key.id())
        status = 'New issues: %d found, %d added, %d skipped' % (
            len(new_issues),
            len(added_issues),
            len(skipped_issues),
        )
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'added': [issue for issue in added_issues],
        }))


class RefreshShard(TaskHandler):
    def get(self, shard_count=None, shard=None):
        if not shard_count:
            # When run from cron cycle over all issues
            shard_count = 24 * 7
            shard = datetime.today().hour + 24 * date.today().weekday()
        cv = comicvine.load()
        query = issues.Issue.query(issues.Issue.shard == int(shard))
        sharded_issues = [issue.key.id() for issue in query.fetch()]
        updated_issues = []
        for index in range(0, len(sharded_issues), 100):
            ids = sharded_issues[index:min([len(sharded_issues), index+100])]
            issue_page = cv.fetch_issue_batch(ids)
            for issue in issue_page:
                updated_issues.append(
                    issues.issue_key(issue, create=False, reindex=True))
        status = 'Updated %d issues' % len(updated_issues)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'updated': [issue.id() for issue in updated_issues],
        }))

class ReshardIssues(TaskHandler):
    @ndb.tasklet
    def reshard_task(self, shards, issue):
        issue.shard = issue.identifier % shards
        result = yield issue.put_async()
        raise ndb.Return(result)

    def get(self):
        shards_key = Setting.query(Setting.name == 'update_shards_key').get()
        shards = int(shards_key.value)
        callback = partial(self.reshard_task, shards)
        query = issues.Issue.query()
        if not self.request.get('all'):
            query = query.filter(issues.Issue.shard == -1)
        results = query.map(callback)
        logging.info('Resharded %d issues', len(results))
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d issues resharded' % len(results),
        }))

class Validate(TaskHandler):
    @ndb.tasklet
    def check_valid(self, issue):
        if issue.key.id() != str(issue.identifier):
            #return value is not used
            yield issue.key.delete_async()
            raise ndb.Return(True)

    def get(self):
        query = issues.Issue.query()
        results = query.map(self.check_valid)
        deleted = sum(1 for deleted in results if deleted)
        self.response.write(json.dumps({
            'status': 200,
            'deleted': deleted,
        }))

app = create_app([
    Route('/tasks/issues/fetchnew', FetchNew),
    Route('/tasks/issues/refresh', RefreshShard),
    Route('/tasks/issues/reshard', ReshardIssues),
    Route('/tasks/issues/validate', Validate),
])
