from datetime import datetime, date
from functools import partial
import json
import logging

from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import Route, TaskHandler, create_app
from pulldb.models.admin import Setting
from pulldb.models import comicvine
from pulldb.models import issues

# pylint: disable=W0232,C0103,E1101,R0201,R0903

class FetchNew(TaskHandler):
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
    Route('/tasks/issues/refresh', RefreshShard),
    Route('/tasks/issues/reshard', ReshardIssues),
    Route('/tasks/issues/validate', Validate),
])
