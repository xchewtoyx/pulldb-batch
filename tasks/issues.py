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
        logging.info('Fetching volumes for issue list: %r', issue_list)
        volume_keys = []
        for issue in issue_list:
            if not isinstance(issue, dict):
                logging.warn('Invalid issue data: %r', issue)
                continue
            volume_keys.append(
                volumes.volume_key(issue['volume'], create=False)
            )
        volume_entries = ndb.get_multi(volume_keys)
        return [volume.key.id() for volume in volume_entries if volume]

    def find_candidates(self, new_issues):
        volume_list = self.volume_list(new_issues)
        candidates = []
        for issue in new_issues:
            if isinstance(issue, dict):
                issue_key = issues.issue_key(str(issue['id']), create=False)
                volume_key = volumes.volume_key(issue['volume'], create=False)
                candidates.append(
                    (issue, issue_key, volume_key)
                )

        # Pre-cache issues
        ndb.get_multi_async(key for issue, key, volume in candidates)
        return candidates, volume_list

    @ndb.toplevel
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
        for issue_dict, key, volume in candidates:
            issue = key.get()
            if issue:
                skipped_issues.append(key.id())
                continue
            if volume.id() in volume_list:
                issue = issues.issue_key(
                    issue_dict, volume, create=True, batch=True)
                added_issues.append(issue)
        status = 'New issues: %d found, %d added, %d skipped' % (
            len(new_issues),
            len(added_issues),
            len(skipped_issues),
        )
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))

class RefreshShard(TaskHandler):
    def get(self, *args):
        shard = self.request.get('shard')
        if not shard:
            shard = datetime.today().hour + 24 * date.today().weekday()
        cv = comicvine.load()
        query = issues.Issue.query(
            issues.Issue.shard == int(shard),
            issues.Issue.volume > None, # issue has volume
        )
        issue_list = query.fetch()
        issue_ids = [issue.key.id() for issue in issue_list]
        issue_map = {issue.identifier: issue for issue in issue_list}
        updated_issues = []
        for index in range(0, len(issue_ids), 100):
            ids = issue_ids[index:min([len(issue_ids), index+100])]
            issue_details = cv.fetch_issue_batch(ids)
            for issue_dict in issue_details:
                issue = issue_map[issue_dict['id']]
                issue_updated, last_update = issue.has_updates(issue_dict)
                if issue_updated:
                    issue.apply_changes(issue_dict)
                    updated_issues.append(issue)
        status = 'Updated %d issues' % len(updated_issues)
        ndb.put_multi(updated_issues)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
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

class ConvertIssues(TaskHandler):
    @ndb.tasklet
    def convert_issue(self, issue):
        if issue.json and not issue.volume:
            issue_key = issues.issue_key(issue.json, issue.volume)
            converted = yield issue_key.get_async()
            if converted:
                yield issue.key.delete_async()
                raise ndb.Return(True)

    def get(self):
        query = issues.Issue.query()
        converts = query.map(self.convert_issue, limit=100)
        convert_count = sum(1 for convert in converts if convert)
        self.response.write(json.dumps(
            {'status': 200, 'count': convert_count,}
        ))


class FixVolumeKey(TaskHandler):
    @ndb.tasklet
    def check_type(self, issue):
        if not isinstance(issue.volume.id(), basestring):
            logging.debug('Volume id type: %r', type(issue.volume.id()))
            issue.volume = ndb.Key('Volume', str(issue.volume.id()))
            yield issue.put_async()
            raise ndb.Return(True)

    def get(self):
        shard = int(self.request.get('shard', 0))
        query = issues.Issue.query(issues.Issue.shard == shard)
        fixed = query.map(self.check_type)
        fixed_count = sum([1 for state in fixed if state])
        self.response.write(json.dumps(
            {'status': 200, 'count': fixed_count, 'total': len(fixed)}))


app = create_app([
    Route('/tasks/issues/convert', ConvertIssues),
    Route('/tasks/issues/fetchnew', FetchNew),
    Route('/<:batch|tasks>/issues/refresh', RefreshShard),
    Route('/tasks/issues/reindex', Reindex),
    Route('/tasks/issues/reshard', ReshardIssues),
    Route('/tasks/issues/validate', ValidateShard),
    Route('/tasks/issues/fixvolumekey', FixVolumeKey),
])
