from datetime import date, datetime, timedelta
from functools import partial
import json
import logging
import math

from google.appengine.api import search
from google.appengine.ext import ndb

# pylint: disable=F0401
from pulldb.base import create_app, Route, TaskHandler
from pulldb.models.admin import Setting
from pulldb.models import comicvine
from pulldb.models import issues
from pulldb.models import subscriptions
from pulldb.models import volumes

# pylint: disable=W0232,C0103,E1101,R0201,R0903,W0201

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
                all_issues = self.cv.fetch_issue_batch(
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

    @ndb.toplevel
    def get(self, shard_type='complete', *args):
        # TODO(rgh): refactor this method
        # pylint: disable=R0914
        logging.info('Args: %r %r', shard_type, args)
        self.cv = comicvine.load()
        query = volumes.Volume.query(self.shard_filter(shard_type))
        results = query.map(self.volume_issues)
        sharded_ids = [result['volume_id'] for result in results]
        volume_detail = {result['volume_id']: result for result in results}
        cv_volumes = []
        for index in range(0, len(sharded_ids), 100):
            volume_page = sharded_ids[index:min(index+100, len(sharded_ids))]
            try:
                cv_volumes.extend(self.cv.fetch_volume_batch(volume_page))
            except Exception as error:
                logging.exception(error)
        for comicvine_volume in cv_volumes:
            if not comicvine_volume.get('date_last_updated'):
                comicvine_volume['date_last_updated'] = date.today().isoformat()
            logging.debug('checking for new issues in %r', comicvine_volume)
            comicvine_id = comicvine_volume['id']
            comicvine_issues = self.fetch_issues(comicvine_volume)
            comicvine_issues = self.filter_results(comicvine_issues)
            volume_detail[int(comicvine_id)]['cv_issues'] = comicvine_issues
            volume_detail[int(comicvine_id)]['volume_detail'] = comicvine_volume
            volumes.volume_key(comicvine_volume, create=False)
        new_issues = []
        for detail in volume_detail.values():
            for issue in detail['cv_issues']:
                if int(issue['id']) not in detail['issue_ids']:
                    new_issues.append((issue, detail['volume_key']))
        for issue, volume_key in new_issues:
            issues.issue_key(issue, volume_key=volume_key)
        for volume_id in volume_detail:
            volume_key = ndb.Key('Volume', str(volume_id))
            logging.info('updating volume: %r' % volume_key)
            volume_data = volume_detail[volume_id]['volume_detail']
            try:
                last_issue_key = ndb.Key(
                    'Issue', str(volume_data['last_issue']['id'])
                )
            except KeyError:
                logging.info(
                    'Cannot update last issue data for %r, %r',
                    volume_id, volume_data
                )
                continue
            volume = volume_key.get()
            if volume and (
                    volume.last_issue != last_issue_key or
                    not volume.last_issue_date):
                last_issue = last_issue_key.get()
                volume.last_issue = last_issue_key
                try:
                    volume.last_issue_date = last_issue.pubdate
                except AttributeError:
                    logging.warn(
                        'Cannot update volume %r, last issue %r has no pubdate',
                        volume, last_issue
                    )
                volume.put_async()
            else:
                logging.info('not updating %r with %r',
                             volume_key, volume_data)
        status = 'Updated %d volumes. Found %d new issues' % (
            len(sharded_ids), len(new_issues)
        )
        logging.info(status)
        self.response.write(json.dumps({
            'status': 200,
            'message': status,
        }))

class Reindex(TaskHandler):
    def get(self):
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
            volume.indexed=True
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
    def reshard_task(self, shards, volume):
        volume.fast_shard = volume.identifier % 24
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


class FixIssueKeys(TaskHandler):
    @ndb.tasklet
    def check_type(self, volume):
        changed = False
        if volume.first_issue and not isinstance(
                volume.first_issue.id(), basestring):
            volume.first_issue = ndb.Key('Issue', str(volume.first_issue.id()))
            changed = True
        if volume.last_issue and not isinstance(
                volume.last_issue.id(), basestring):
            volume.last_issue = ndb.Key('Issue', str(volume.last_issue.id()))
            changed = True
        if changed:
            yield volume.put_async()
        else:
            try:
                logging.debug('unchanged: %r %r',
                              type(volume.first_issue.id()),
                              type(volume.last_issue.id()))
            except:
                pass
        raise ndb.Return(changed)

    def get(self):
        query = volumes.Volume.query()
        fixed = query.map(self.check_type)
        fixed_count = sum([1 for state in fixed if state])
        self.response.write(json.dumps({
            'status': 200,
            'total': len(fixed),
            'fixed': fixed_count}))


app = create_app([
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
    Route('/tasks/volumes/validate', Validate),
    Route('/tasks/volumes/fixissuekeys', FixIssueKeys),
])
