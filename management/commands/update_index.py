# encoding: utf-8
from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
from datetime import timedelta, datetime
from optparse import make_option

from django import db
from django.core.management.base import LabelCommand
from django.db import reset_queries

from sanjab import connections as sanjab_connections
from sanjab.query import SearchQuerySet
from sanjab.utils.app_loading import get_models, load_apps

try:
    from django.utils.encoding import force_text
except ImportError:
    from django.utils.encoding import force_unicode as force_text

try:
    from django.utils.encoding import smart_bytes
except ImportError:
    from django.utils.encoding import smart_str as smart_bytes

try:
    from django.utils.timezone import now
except ImportError:
    from datetime import datetime
    now = datetime.now


DEFAULT_BATCH_SIZE = None
DEFAULT_AGE = None
APP = 'app'
MODEL = 'model'


def worker(bits):
    # We need to reset the connections, otherwise the different processes
    # will try to share the connection, which causes things to blow up.
    from django.db import connections

    for alias, info in connections.databases.items():
        # We need to also tread lightly with SQLite, because blindly wiping
        # out connections (via ``... = {}``) destroys in-memory DBs.
        if 'sqlite3' not in info['ENGINE']:
            try:
                db.close_connection()
                if isinstance(connections._connections, dict):
                    del(connections._connections[alias])
                else:
                    delattr(connections._connections, alias)
            except KeyError:
                pass

    if bits[0] == 'do_update':
        func, model, start, end, total, using, start_date, end_date, verbosity = bits
    elif bits[0] == 'do_remove':
        func, model, pks_seen, start, upper_bound, using, verbosity = bits
    else:
        return

    unified_index = sanjab_connections[using].get_unified_index()
    indexes = unified_index.get_index(model)
    backend = sanjab_connections[using].get_backend()

    if func == 'do_update':
        for type, index in indexes.items():
            qs = index.build_queryset(using=using, start_date=start_date,
                                      end_date=end_date)
            do_update(backend, index, type, qs, start, end, total, verbosity=verbosity)
    elif bits[0] == 'do_remove':
        do_remove(backend, None, model, pks_seen, start, upper_bound, verbosity=verbosity)


def do_update(backend, index, type, qs, start, end, total, verbosity=1):
    # Get a clone of the QuerySet so that the cache doesn't bloat up
    # in memory. Useful when reindexing large amounts of data.
    small_cache_qs = qs.all()
    current_qs = small_cache_qs[start:end]
    if verbosity >= 2:
        if hasattr(os, 'getppid') and os.getpid() == os.getppid():
            print("  indexed %s - %d of %d." % (start + 1, end, total))
        else:
            print("  indexed %s - %d of %d (by %s)." % (start + 1, end, total, os.getpid()))

    # FIXME: Get the right backend.
    backend.update(index, type, current_qs)

    # Clear out the DB connections queries because it bloats up RAM.
    reset_queries()


def do_remove(backend, index, model, pks_seen, start, upper_bound, verbosity=1):
    # Fetch a list of results.
    # Can't do pk range, because id's are strings (thanks comments
    # & UUIDs!).
    stuff_in_the_index = SearchQuerySet(using=backend.connection_alias).models(model)[start:upper_bound]

    # Iterate over those results.
    for result in stuff_in_the_index:
        # Be careful not to hit the DB.
        if not smart_bytes(result.pk) in pks_seen:
            # The id is NOT in the small_cache_qs, issue a delete.
            if verbosity >= 2:
                print("  removing %s." % result.pk)

            backend.remove(".".join([result.app_label, result.model_name, str(result.pk)]))


class Command(LabelCommand):
    help = "Freshens the index for the given app(s)."
    base_options = (
        make_option('-a', '--age', action='store', dest='age',
            default=DEFAULT_AGE, type='int',
            help='Number of hours back to consider objects new.'
        ),
        make_option('-s', '--start', action='store', dest='start_date',
            default=None, type='string',
            help='The start date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-e', '--end', action='store', dest='end_date',
            default=None, type='string',
            help='The end date for indexing within. Can be any dateutil-parsable string, recommended to be YYYY-MM-DDTHH:MM:SS.'
        ),
        make_option('-b', '--batch-size', action='store', dest='batchsize',
            default=None, type='int',
            help='Number of items to index at once.'
        ),
        make_option('-r', '--remove', action='store_true', dest='remove',
            default=False, help='Remove objects from the index that are no longer present in the database.'
        ),
        make_option("-u", "--using", action="append", dest="using",
            default=[],
            help='Update only the named backend (can be used multiple times). '
                 'By default all backends will be updated.'
        ),
        make_option("-d", "--doctype", action="store", dest="doctype",
            default=None,
            help='Update only the given Document type in index. '
                 'By default all doctypes in the index will be updated.'
        ),
        make_option('-k', '--workers', action='store', dest='workers',
            default=0, type='int',
            help='Allows for the use multiple workers to parallelize indexing. Requires multiprocessing.'
        ),
    )
    option_list = LabelCommand.option_list + base_options

    def handle(self, *items, **options):
        self.start_time = datetime.now()
        self.verbosity = int(options.get('verbosity', 1))
        self.batchsize = options.get('batchsize', DEFAULT_BATCH_SIZE)
        self.start_date = None
        self.end_date = None
        self.doctype = None
        self.remove = options.get('remove', False)
        self.workers = int(options.get('workers', 0))

        self.backends = options.get('using')
        if not self.backends:
            self.backends = sanjab_connections.connections_info.keys()

        age = options.get('age', DEFAULT_AGE)
        start_date = options.get('start_date')
        end_date = options.get('end_date')

        if options.get('doctype'):
            self.doctype = options.get('doctype')

        if age is not None:
            self.start_date = now() - timedelta(hours=int(age))

        if start_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.start_date = dateutil_parse(start_date)
            except ValueError:
                pass

        if end_date is not None:
            from dateutil.parser import parse as dateutil_parse

            try:
                self.end_date = dateutil_parse(end_date)
            except ValueError:
                pass

        if not items:
            items = load_apps()

        return super(Command, self).handle(*items, **options)

    def handle_label(self, label, **options):
        for using in self.backends:
            try:
                self.update_backend(label, using)
            except:
                logging.exception("Error updating %s using %s ", label, using)
                raise

    def update_backend(self, label, using):
        from sanjab.exceptions import NotHandled
        backend = sanjab_connections[using].get_backend()
        unified_index = sanjab_connections[using].get_unified_index()
        if self.workers > 0:
            import multiprocessing

        for model in get_models(label):
            try:
                indexes = unified_index.get_index(model)
            except NotHandled:
                if self.verbosity >= 2:
                    print("Skipping '%s' - no index." % model)
                continue

            if self.workers > 0:
                # workers resetting connections leads to references to models / connections getting
                # stale and having their connection disconnected from under them. Resetting before
                # the loop continues and it accesses the ORM makes it better.
                db.close_connection()
            if 'base' in indexes:
                base = indexes.pop('base')


            if self.doctype:
                if self.doctype not in indexes.keys():
                    continue
                indexes = {self.doctype: indexes[self.doctype]}
                print(u"Updating given doctype indexes: %s" % self.doctype)

            for type, index in indexes.items():
                qs = index.build_queryset(using=using, start_date=self.start_date,
                                          end_date=self.end_date)

                total = qs.count()

                if self.verbosity >= 1:
                    print(u"Indexing %d %s" % (total, force_text(model._meta.verbose_name_plural)))

                batch_size = self.batchsize or backend.batch_size

                if self.workers > 0:
                    ghetto_queue = []

                for start in range(0, total, batch_size):
                    end = min(start + batch_size, total)

                    if self.workers == 0:
                        do_update(backend, index, type, qs, start, end, total, self.verbosity)
                    else:
                        ghetto_queue.append(('do_update', model, start, end, total, using, self.start_date, self.end_date, self.verbosity))

                if self.workers > 0:
                    pool = multiprocessing.Pool(self.workers)
                    pool.map(worker, ghetto_queue)
                    pool.terminate()

                if self.remove:
                    if self.start_date or self.end_date or total <= 0:
                        # They're using a reduced set, which may not incorporate
                        # all pks. Rebuild the list with everything.
                        qs = index.index_queryset().values_list('pk', flat=True)
                        pks_seen = set(smart_bytes(pk) for pk in qs)

                        total = len(pks_seen)
                    else:
                        pks_seen = set(smart_bytes(pk) for pk in qs.values_list('pk', flat=True))

                    if self.workers > 0:
                        ghetto_queue = []

                    for start in range(0, total, batch_size):
                        upper_bound = start + batch_size

                        if self.workers == 0:
                            do_remove(backend, index, model, pks_seen, start, upper_bound)
                        else:
                            ghetto_queue.append(('do_remove', model, pks_seen, start, upper_bound, using, self.verbosity))

                    if self.workers > 0:
                        pool = multiprocessing.Pool(self.workers)
                        pool.map(worker, ghetto_queue)
                        pool.terminate()
            delta = (datetime.now() - self.start_time).total_seconds()

            print ("Completed in %s seconds or %s minutes" % (delta, delta/60))