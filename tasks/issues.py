# pylint: disable=missing-docstring, no-self-use
from datetime import datetime, date, timedelta
from functools import partial
import json
import logging
import sys
import time
from zlib import crc32

from google.appengine.api import search
from google.appengine.api.urlfetch_errors import DeadlineExceededError
from google.appengine.ext import ndb
from google.appengine.ext.ndb.tasklets import Future

from pulldb.base import Route, TaskHandler, create_app
from pulldb.models.admin import Setting
from pulldb.models import arcs
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import volumes
from pulldb.varz import VarzContext

class FetchNew(TaskHandler):
    def __init__(self, *args, **kwargs):
        super(FetchNew, self).__init__(*args, **kwargs)
        self.cv_api = comicvine.load()

    @ndb.tasklet
    def check_issue(self, issue):
        issue_key = issues.issue_key(issue, create=False)
        if issue_key and issue_key.get():
            ndb.Return(issue, False)
        volume_key = volumes.volume_key(issue['volume'], create=False)
        if volume_key.get():
            ndb.Return(issue, True)
        try:
            issue_detail = yield self.cv_api.fetch_issue_async(issue['id'])
        except DeadlineExceededError as err:
            logging.warn("Cannot fetch issue detail for %s (%r)",
                         issue['id'], err)
            issue_detail = None
        if issue_detail:
            issue.update(issue_detail)
        arc_list = issue.get('story_arc_credits', [])
        arc_keys = [arcs.arc_key(arc, create=False) for arc in arc_list]
        raise ndb.Return(issue, any(arc_keys))

    def check_batch(self, issue_batch):
        issue_futures = [self.check_issue(issue) for issue in issue_batch]
        added_issues = []
        skipped_issues = []
        for future in issue_futures:
            issue_dict, candidate = future.get_result()
            if candidate:
                issue = issues.issue_key(
                    issue_dict, create=True, batch=True)
                added_issues.append(issue)
            else:
                skipped_issues.append(issue_dict['id'])
        return added_issues, skipped_issues

    @ndb.toplevel
    def get(self):
        today = date.today()
        yesterday = today - timedelta(1)
        try:
            new_issues = self.cv_api.fetch_issue_batch(
                [yesterday.isoformat(), today.isoformat()],
                filter_attr='date_added',
                deadline=60
            )
        except (comicvine.ApiError, DeadlineExceededError) as err:
            logging.warn("Error fetching new issues %r", err)
            new_issues = []
        # Fixup for sometimes getting 'number_of_page_results' mixed into
        # the results
        new_issues = [
            issue for issue in new_issues if isinstance(issue, dict)
        ]
        added_issues = []
        skipped_issues = []
        # Fetch 30 at a time to avoid too many updates in parallel
        for i in range(0, len(new_issues), 30):
            issue_slice = new_issues[i:i+30]
            batch_added, batch_skipped = self.check_batch(issue_slice)
            added_issues += batch_added
            skipped_issues += batch_skipped

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


class RequeueShard(TaskHandler):
    @ndb.tasklet
    def requeue_issue(self, issue):
        if issue.complete:
            issue.complete = False
            yield issue.put_async()
            raise ndb.Return(True)

    def shard_filter(self):
        shard_key = Setting.query(
            Setting.name == 'issue_shard_count').get()
        shard = self.request.get('shard')
        if not shard:
            shard = int(time.time() // 3600) % int(shard_key.value)
        return shard

    def get(self):
        logging.info('Recursion limit: %d', sys.getrecursionlimit())
        shard = self.shard_filter()
        query = issues.Issue.query(
            issues.Issue.shard == int(shard),
        )
        issue_count = query.count_async()
        updates = query.map(self.requeue_issue)
        update_count = sum(1 for updated in updates if updated)
        issue_count = issue_count.get_result()
        status = 'Updated %d of %d issues' % (
            update_count, issue_count)

        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'shard': shard,
            'issues': issue_count,
            'updates': update_count,
        }))

class RefreshBatch(TaskHandler):
    def __init__(self, *args, **kwargs):
        super(RefreshBatch, self).__init__(*args, **kwargs)
        self.cv_api = comicvine.load()
        self.varz = None

    @ndb.tasklet
    def check_volume(self, issue):
        volume_key = issue.volume
        logging.info('Retrieving volume info for %r', volume_key)
        try:
            volume_dict = yield self.cv_api.fetch_volume_async(
                int(volume_key.id()))
        except (DeadlineExceededError, comicvine.ApiError) as err:
            logging.warn('Error fetching volume %r: %r',
                         volume_key, err)
            raise ndb.Return(None)
        new_volume = volumes.volume_key(
            volume_dict, create=True, batch=True)
        if isinstance(new_volume, Future):
            volume_key = yield new_volume
            raise ndb.Return(volume_key)

    @ndb.tasklet
    def refresh_issue(self, issue):
        issue_dict = {}
        try:
            issue_dict = yield self.cv_api.fetch_issue_async(
                int(issue.key.id()))
        except DeadlineExceededError as err:
            logging.warn('Timeout fetching issue %r: %r',
                         issue.key, err)
            raise ndb.Return(None)
        except comicvine.ApiError as err:
            logging.warn('Comicvine Api Error fetching issue %r: %r',
                         issue.key, err)
            raise ndb.Return(None)
        except Exception as err:
            logging.warn('Unknown exception fetching issue %r: %r',
                         issue.key, err)
            raise
        if not issue_dict:
            issue_updated = False
            logging.warn('Cannot update issue: %r', issue_dict)
            raise ndb.Return(None)
        # pylint: disable=unused-variable
        issue_updated, last_update = issue.has_updates(issue_dict)
        if issue_updated:
            logging.debug('Issue %r updated', issue.key)
            issue.apply_changes(issue_dict)
        try:
            volume = yield issue.volume.get_async()
            if not volume:
                volume_key = yield self.check_volume(issue)
                if volume_key:
                    issue.volume = volume_key
        except (DeadlineExceededError, comicvine.ApiError) as err:
            logging.warn('Error checking volume %r for issue %r: %r',
                         issue.volume, issue.key, err)
        issue.complete = True
        yield issue.put_async()
        raise ndb.Return(issue_updated)

    @ndb.tasklet
    def fetch_issue_page(self, batch_size, cursor):
        clean_run = True
        try:
            page_results, cursor, more = yield self.query.fetch_page_async(
                batch_size, start_cursor=cursor)
            if page_results:
                results = yield [
                    self.refresh_issue(issue)
                    for issue in page_results
                ]
        except (DeadlineExceededError, comicvine.ApiError) as err:
            logging.warn('Error while fetching comicvine data: %r', err)
            clean_run = False
            results = []
        raise ndb.Return(results, clean_run, cursor, more)

    @VarzContext('issue_queue')
    def get(self):
        # pylint: disable=unused-variable, attribute-defined-outside-init
        logging.info('Recursion limit: %d', sys.getrecursionlimit())
        self.query = issues.Issue.query(
            issues.Issue.complete == False,
        )
        issue_count = self.query.count_async()
        self.varz.update_count = 0
        limit = int(self.request.get('limit', 150))
        step = int(self.request.get('step', 50))
        cursor = None
        more = None
        for offset in range(0, limit, step):
            logging.info('Fetching issue batch: %d@%d/%d',
                         step, offset, limit)
            batch_limit = min([step, limit - offset])
            batch_future = self.fetch_issue_page(batch_limit, cursor)
            try:
                updates, clean_run, cursor, more = batch_future.get_result()
            except (comicvine.ApiError, DeadlineExceededError) as err:
                logging.warn('Error fetching issue batch at offset %d: %r',
                             offset, err)
                clean_run = False
            else:
                self.varz.update_count += sum(
                    1 for updated in updates if updated)
            if not (clean_run and more):
                break

        self.varz.backlog = issue_count = issue_count.get_result()
        status = 'Updated %d of %d issues' % (
            self.varz.update_count, self.varz.backlog)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'issues': self.varz.backlog,
            'updates': self.varz.update_count,
        }))


class RefreshShard(TaskHandler):
    @ndb.tasklet
    def refresh_issue(self, issue):
        issue_dict = yield self.cv_api.fetch_issue_async(
            int(issue.key.id()))
        issue_changed = False
        issue_updated = False
        if issue_dict:
            # pylint: disable=unused-variable
            issue_changed, last_update = issue.has_updates(issue_dict)
            if issue_changed:
                issue.apply_changes(issue_dict)
                issue_updated = True
            if not issue.complete:
                issue.complete = True
                issue_updated = True
        else:
            logging.warn('Cannot update issue: %r', issue_dict)
        if issue_updated:
            yield issue.put_async()
        logging.debug('Issue %r updated', issue.key)
        raise ndb.Return(issue_changed)

    def shard_filter(self):
        shard_key = Setting.query(
            Setting.name == 'issue_shard_count').get()
        shard = self.request.get('shard')
        if not shard:
            shard = int(time.time() // 3600) % int(shard_key.value)
        return shard

    def get(self, *args): # pylint: disable=unused-argument
        # pylint: disable=unused-variable, attribute-defined-outside-init
        logging.info('Recursion limit: %d', sys.getrecursionlimit())
        self.cv_api = comicvine.load()
        shard = self.shard_filter()
        query = issues.Issue.query(
            issues.Issue.shard == int(shard),
            issues.Issue.complete == False,
        )
        issue_count = query.count()
        update_count = 0
        for offset in range(0, issue_count, 60):
            issue_updates = query.map(
                self.refresh_issue, offset=offset, limit=60)
            update_count += sum(1 for updated in issue_updates if updated)
        status = 'Updated %d of %d issues' % (update_count, issue_count)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
            'shard': shard,
            'issues': issue_count,
            'updates': update_count,
        }))

class Reindex(TaskHandler):
    def get(self):
        query = issues.Issue.query(
            issues.Issue.indexed == False,
        )
        backlog = query.count_async()
        # index.put can only handle 200 docs at a time
        issues_future = query.fetch_async(limit=200)
        index = search.Index(name='issues')
        reindex_list = []
        issue_list = []
        for issue in issues_future.get_result():
            reindex_list.append(
                issue.index_document(batch=True)
            )
            issue.indexed = True
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
        update_count = len(issue_list)
        self.response.write(json.dumps({
            'status': 200,
            'message': 'Reindexed %d of %d outstanding issues' % (
                update_count,
                backlog.get_result(),
            ),
            'backlog': backlog.get_result(),
            'count': update_count,
        }))


class ReshardIssues(TaskHandler):
    @ndb.tasklet
    def reshard_task(self, shard_count, issue):
        seed = crc32(issue.key.urlsafe())
        shard = seed % shard_count
        if issue.shard != shard:
            issue.shard = shard
            result = yield issue.put_async()
            raise ndb.Return(result)

    def get(self):
        shards_key = Setting.query(Setting.name == 'issue_shard_count').get()
        shards = int(shards_key.value)
        callback = partial(self.reshard_task, shards)
        query = issues.Issue.query()
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
            query = query.filter(issues.Issue.shard == shard)
        if self.request.get('all'):
            if self.request.get('shard_start'):
                start = int(self.request.get('shard_start')) - 1
                query = query.filter(issues.Issue.shard >= start)
            if self.request.get('shard_stop'):
                stop = int(self.request.get('shard_stop'))
                query = query.filter(issues.Issue.shard < stop)
        else:
            query = query.filter(issues.Issue.shard == -1)
        issue_count = query.count_async()
        results = query.map(callback)
        update_count = sum(1 for result in results if result)
        logging.info('Resharded %d issues', len(results))
        self.response.write(json.dumps({
            'status': 200,
            'message': '%d of %d issues resharded' % (
                update_count, issue_count.get_result()),
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


app = create_app([ # pylint: disable=invalid-name
    Route('/tasks/issues/convert', ConvertIssues),
    Route('/tasks/issues/fetchnew', FetchNew),
    Route('/tasks/issues/requeue', RequeueShard),
    Route('/tasks/issues/refresh/batch', RefreshBatch),
    Route('/<:batch|tasks>/issues/refresh', RefreshShard),
    Route('/tasks/issues/reindex', Reindex),
    Route('/tasks/issues/reshard', ReshardIssues),
    Route('/tasks/issues/validate', ValidateShard),
    Route('/tasks/issues/fixvolumekey', FixVolumeKey),
])
