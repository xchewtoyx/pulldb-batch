from datetime import datetime, date, timedelta
from functools import partial
import json
import logging

from google.appengine.api import search
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
        logging.debug('Fetching volumes for issue list: %r', issue_list)
        volume_keys = []
        for issue in issue_list:
            if not isinstance(issue, dict):
                logging.warning('Invalid issue data: %r', issue)
                continue
            volume_keys.append(
                volumes.volume_key(issue['volume'], create=False)
            )
        volume_entries = ndb.get_multi(volume_keys)
        return [volume.key.id() for volume in volume_entries if volume]

    def volume_key(self, volume_id):
        return ndb.Key(
            volumes.Volume, str(volume_id)
        )

    def find_candidates(self, new_issues):
        volume_list = self.volume_list(new_issues)
        candidates = []
        for issue in new_issues:
            if isinstance(issue, dict):
                candidates.append((
                    issue,
                    ndb.Key(
                        issues.Issue, str(issue['id']),
                        parent=self.volume_key(issue['volume'])
                    ),
                ))

        # Pre-cache issues
        ndb.get_multi_async(key for issue, key in candidates)
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
        # Fixup for sometimes getting 'number_of_page_results' mixed into
        # the results
        new_issues = [
            issue for issue in new_issues if isinstance(issue, dict)
        ]
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
        query = issues.Issue.query(
            issues.Issue.shard == int(shard),
            issues.Issue.volume > None, # issue has volume
        )
        issue_list = query.fetch()
        issue_ids = [issue.key.id() for issue in issue_list]
        issue_map = {issue.key.id(): issue for issue in issue_list}
        updated_issues = []
        for index in range(0, len(issue_ids), 100):
            ids = issue_ids[index:min([len(issue_ids), index+100])]
            issue_details = cv.fetch_issue_batch(ids)
            for issue_dict in issue_details:
                issue = issue_map[issue_dict['id']]
                if issue.has_updates(issue_dict):
                    issue.apply_updates(issue_dict)
                    updated_issues.append(issue)
        status = 'Updated %d issues' % len(updated_issues)
        ndb.put_multi(updated_issues)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'updated': [issue.id() for issue in updated_issues],
        }))

class Reindex(TaskHandler):
    def get(self):
        query = issues.Issue.query(
            issues.Issue.indexed == False,
        )
        # index.put can only handle 200 docs at a time
        issues_future = query.fetch_async(limit=200)
        index = search.Index(name='issues')
        reindex_list = []
        issue_list = []
        for issue in issues_future.get_result():
            reindex_list.append(
                issue.index_document(batch=True)
            )
            issue.indexed=True
            issue_list.append(issue)
        logging.info('Reindexing %d issues', len(issue_list))
        if len(issue_list):
            try:
                index.put(reindex_list)
            except search.Error as error:
                logging.error('index update failed: %r', error)
                logging.exception(error)
            else:
                ndb.put_multi(issue_list)

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

class ValidateShard(TaskHandler):
    @ndb.tasklet
    def check_valid(self, issue):
        if issue.key.id() != str(issue.identifier):
            #return value is not used
            yield issue.key.delete_async()
            raise ndb.Return(True)
        if not issue.volume:
            query = issues.Issue.query(
                issues.Issue.identifier == issue.identifier
            )
            candidates = yield query.fetch_async()
            if len(candidates) > 1:
                yield issue.key.delete_async()
                raise ndb.Return(True)

    def get(self):
        shard = datetime.today().hour + 24 * date.today().weekday()
        query = issues.Issue.query(
            issues.Issue.shard == int(shard)
        )
        results = query.map(self.check_valid)
        deleted = sum(1 for deleted in results if deleted)
        self.response.write(json.dumps({
            'status': 200,
            'deleted': deleted,
        }))

app = create_app([
    Route('/tasks/issues/fetchnew', FetchNew),
    Route('/tasks/issues/refresh', RefreshShard),
    Route('/tasks/issues/reindex', Reindex),
    Route('/tasks/issues/reshard', ReshardIssues),
    Route('/tasks/issues/validate', ValidateShard),
])
