# pylint: disable=missing-docstring
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import partial
import json
import logging
import time
from zlib import crc32

from google.appengine.api import search
from google.appengine.api.urlfetch_errors import DeadlineExceededError
from google.appengine.ext import ndb
from google.appengine.ext.ndb.tasklets import Future

from pulldb.base import create_app, Route, TaskHandler
from pulldb.models.admin import Setting
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import volumes
from pulldb.varz import VarzContext

class RequeueVolumes(TaskHandler):
    @ndb.tasklet
    def queue_volume(self, volume): # pylint: disable=no-self-use
        if volume.complete:
            volume.complete = False
            yield volume.put_async()
            raise ndb.Return(True)

    def shard(self):
        # Check weekly for all volumes
        shards_key = Setting.query(
            Setting.name == 'volume_shard_count').get()
        shard = int(time.time() // 3600) % int(shards_key.value)
        logmsg = 'Requeueing all volumes in shard %s'
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        logging.info(logmsg, shard)
        return shard

    @ndb.toplevel
    def get(self):
        # Fetch the volumes due for checking
        shard = self.shard()
        query = volumes.Volume.query(
            volumes.Volume.shard == shard
        )
        volume_count = query.count_async()
        results = query.map(self.queue_volume)
        updated = sum(1 for volume in results if volume)
        status = 'Marking %d of %d issues in shard %d' % (
            updated, volume_count.get_result(), shard)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))


class RefreshBatch(TaskHandler):
    def __init__(self, *args, **kwargs):
        super(RefreshBatch, self).__init__(*args, **kwargs)
        self.cv_api = None
        self.varz = None

    @ndb.tasklet
    def find_new_issues(self, volume):
        pages = yield self.cv_api.fetch_issue_batch_async(
            [int(volume.identifier)], filter_attr='volume')
        issue_dicts = []
        for page in pages:
            issue_dicts.extend(page)
        new_issues = []
        for issue in issue_dicts:
            issue_key = issues.issue_key(issue, create=True, batch=True)
            if isinstance(issue_key, Future):
                new_issues.append(issue_key)
        issue_keys = yield new_issues
        raise ndb.Return(issue_keys)

    @ndb.tasklet
    def check_volumes(self, volume):
        # pylint: disable=no-self-use,unused-variable
        try:
            volume_dict = yield self.cv_api.fetch_volume_async(
                int(volume.identifier))
        except DeadlineExceededError as err:
            logging.error('Timeout fetching volume %d [%r]',
                          int(volume.identifier), err)
            volume_dict = None
        new_issues = []
        if not volume_dict:
            volume_updated = False
            logging.warn('Cannot update volume: %r', volume.key)
        else:
            volume_updated, last_update = volume.has_updates(volume_dict)
            if volume_updated:
                logging.debug('Volume %r updated', volume.key)
                volume.apply_changes(volume_dict)
            volume.complete = True
            new_issues = yield self.find_new_issues(volume)
            yield volume.put_async()
        raise ndb.Return(volume_updated, len(new_issues))

    @VarzContext('volume_queue')
    def get(self):
        self.cv_api = comicvine.load()
        volume_query = volumes.Volume.query(
            volumes.Volume.complete == False
        )
        incomplete_volumes = volume_query.count_async()
        limit = int(self.request.get('limit', 50))
        step = int(self.request.get('step', 10))
        logging.info('Refreshing %d volumes in batches of %d', limit, step)
        updates = 0
        new_issues = 0
        for offset in range(0, limit, step):
            batch_size = min([step, limit-offset])
            results = volume_query.map(
                self.check_volumes, offset=offset, limit=batch_size)
            updates += sum(1 for update, count in results if update)
            new_issues += sum(count for update, count in results)
        self.varz.backlog = incomplete_volumes.get_result()
        self.varz.updates = updates
        self.varz.new_issues = new_issues

        status = 'Updated %d of %d queued volumes (%d new issues)' % (
            updates, self.varz.backlog, new_issues)
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))


class RefreshVolumes(TaskHandler):
    @ndb.tasklet
    def volume_issues(self, volume):
        volume_issues = yield issues.Issue.query(
            issues.Issue.volume == volume.key
        ).fetch_async()
        issue_ids = [
            issue.identifier for issue in volume_issues
        ]
        raise ndb.Return({
            'volume_id': volume.identifier,
            'volume_key': volume.key,
            'issue_ids': issue_ids,
        })

    def fetch_issues(self, volume, limit=100):
        all_issues = volume.get('issues', [])
        if not all_issues:
            try:
                all_issues = self.cv_api.fetch_issue_batch(
                    [volume['id']], filter_attr='volume'
                )
            except Exception as error: #TODO(rgh): Remove blanket catch
                logging.exception(error)
        return all_issues

    def filter_results(self, results, objtype=dict):
        return [result for result in results if isinstance(result, objtype)]

    def shard_filter(self, shard_type):
        if shard_type == 'active':
            # For active volumes check for updates daily
            shard = datetime.today().hour
            # Consider anything with an issue within past 6 months as active
            logmsg = 'Refreshing active volumes shard %s'
        else:
            # Check weekly for all volumes
            shard = datetime.today().hour + 24 * date.today().weekday()
            logmsg = 'Refreshing all volumes shard %s'
        if self.request.get('shard'):
            shard = int(self.request.get('shard'))
        logging.info(logmsg, shard)
        if shard_type == 'active':
            active_threshold = datetime.today() - timedelta(182)
            shard_filter = ndb.AND(
                volumes.Volume.fast_shard == int(shard),
                volumes.Volume.last_issue_date >= active_threshold,
            )
        else:
            shard_filter = volumes.Volume.shard == int(shard)
        return shard_filter

    def fetch_cv_volumes(self, sharded_ids):
        cv_volumes = []
        for index in range(0, len(sharded_ids), 100):
            volume_page = sharded_ids[index:min(index+100, len(sharded_ids))]
            try:
                cv_volumes.extend(self.cv_api.fetch_volume_batch(volume_page))
            except Exception as error:
                logging.exception(error)
        return cv_volumes

    def fetch_cv_details(self, sharded_ids):
        cv_detail = defaultdict(dict)
        for comicvine_volume in self.fetch_cv_volumes(sharded_ids):
            if not comicvine_volume.get('date_last_updated'):
                comicvine_volume['date_last_updated'] = date.today().isoformat()
            logging.debug('checking for new issues in %r', comicvine_volume)
            comicvine_id = comicvine_volume['id']
            comicvine_issues = self.fetch_issues(comicvine_volume)
            comicvine_issues = self.filter_results(comicvine_issues)
            cv_detail[int(comicvine_id)]['cv_issues'] = comicvine_issues
            cv_detail[int(comicvine_id)]['volume_detail'] = comicvine_volume
            # TODO(rgh): Not sure about this next line.  seems to be noop
            volumes.volume_key(comicvine_volume, create=False)
        return cv_detail

    def find_new(self, volume_detail, cv_detail):
        new_issues = []
        for cv_issue in cv_detail['cv_issues']:
            if int(cv_issue['id']) not in volume_detail['issue_ids']:
                new_issues.append((cv_issue, volume_detail['volume_key']))
        return new_issues

    @ndb.toplevel
    def get(self, shard_type='complete', *args):
        # TODO(rgh): refactor this method
        # pylint: disable=R0914
        logging.info('Args: %r %r', shard_type, args)
        self.cv_api = comicvine.load()

        # Fetch the volumes due for checking
        query = volumes.Volume.query(self.shard_filter(shard_type))
        results = query.map(self.volume_issues)

        # Build the list of ids and lookup table of id details
        sharded_ids = [result['volume_id'] for result in results]
        volume_detail = {result['volume_id']: result for result in results}

        # Fill in missing details
        cv_detail = self.fetch_cv_details(sharded_ids)

        # Check for new issues
        new_issues = []
        for  volume_id in sharded_ids:
            new_issues.extend(self.find_new(
                volume_detail[volume_id], cv_detail[volume_id]))

        # Create entries for new issues (async)
        logging.info('Adding %d new issues', len(new_issues))
        logging.debug('Adding new issues: %r', new_issues)
        for issue, volume_key in new_issues:
            issues.issue_key(issue, volume_key=volume_key, batch=True)

        # Update volume entries
        for volume_id in volume_detail:
            volume_key = ndb.Key('Volume', str(volume_id))
            logging.info('updating volume: %r', volume_key)
            volume_data = cv_detail[volume_id]['volume_detail']
            try:
                last_issue_key = ndb.Key(
                    'Issue', str(volume_data['last_issue']['id']))
            except KeyError:
                logging.info(
                    'Cannot update last issue data for %r, %r',
                    volume_id, volume_data)
                continue
            volume = volume_key.get()
            if volume and (volume.last_issue != last_issue_key or
                           not volume.last_issue_date):
                last_issue = last_issue_key.get()
                volume.last_issue = last_issue_key
                try:
                    volume.last_issue_date = last_issue.pubdate
                except AttributeError:
                    logging.warn(
                        'Cannot update volume %r, last issue %r has no pubdate',
                        volume, last_issue)
                volume.put_async()
            else:
                logging.info('not updating %r with %r',
                             volume_key, volume_data)

        status = 'Updated %d volumes. Found %d new issues' % (
            len(sharded_ids), len(new_issues))
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))

class Reindex(TaskHandler):
    def get(self): # pylint: disable=no-self-use
        query = volumes.Volume.query(
            volumes.Volume.indexed == False
        )
        # index.put can only update 200 docs at a time
        volumes_future = query.fetch_async(limit=200)
        index = search.Index(name='volumes')
        reindex_list = []
        volume_list = []
        for volume in volumes_future.get_result():
            reindex_list.append(
                volume.index_document(batch=True)
            )
            volume.indexed = True
            volume_list.append(volume)
        logging.info('Reindexing %d volumes', len(reindex_list))
        if len(reindex_list):
            try:
                index.put(reindex_list)
            except search.Error as error:
                logging.error('index update failed: %r', error)
                logging.exception(error)
            else:
                ndb.put_multi(volume_list)

class ReshardVolumes(TaskHandler):
    @ndb.tasklet
    def reshard_task(self, shards, volume): # pylint: disable=no-self-use
        seed = crc32(volume.key.urlsafe())
        volume.fast_shard = seed % 24
        volume.shard = seed % shards
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


app = create_app([ # pylint: disable=invalid-name
    Route('/tasks/volumes/requeue', RequeueVolumes),
    Route('/tasks/volumes/refresh/batch', RefreshBatch),
    Route(
        '/<:batch|tasks>/volumes/refresh',
        RefreshVolumes
    ),
    Route(
        '/<:batch|tasks>/volumes/refresh/<shard_type:(active)>',
        RefreshVolumes
    ),
    Route('/tasks/volumes/reindex', Reindex),
    Route('/tasks/volumes/reshard', ReshardVolumes),
])
