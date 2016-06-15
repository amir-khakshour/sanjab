from six import iteritems, string_types
from elasticsearch.helpers import scan

from django.utils import six

from sanjab.backends import SQ
from sanjab.utils import log as logging
from sanjab.exceptions import NotHandled
from sanjab import connections, connection_router
from sanjab.constants import REPR_OUTPUT_SIZE, ITERATOR_LOAD_PER_QUERY, DEFAULT_OPERATOR

from .query import Q, EMPTY_QUERY, Filtered
from .filter import F, EMPTY_FILTER
from .aggs import A, AggBase
from .utils import DslBase
from .result import Response, Result


class BaseProxy(object):
    """
    Simple proxy around DSL objects (queries and filters) that can be called
    (to add query/filter) and also allows attribute access which is proxied to
    the wrapped query/filter.
    """
    def __init__(self, search, attr_name):
        self._search = search
        self._proxied = self._empty
        self._attr_name = attr_name

    def __nonzero__(self):
        return self._proxied != self._empty
    __bool__ = __nonzero__

    def __call__(self, *args, **kwargs):
        s = self._search._clone()
        getattr(s, self._attr_name)._proxied += self._shortcut(*args, **kwargs)

        # always return search to be chainable
        return s

    def __getattr__(self, attr_name):
        return getattr(self._proxied, attr_name)

    def __setattr__(self, attr_name, value):
        if not attr_name.startswith('_'):
            self._proxied = self._shortcut(self._proxied.to_dict())
            setattr(self._proxied, attr_name, value)
        super(BaseProxy, self).__setattr__(attr_name, value)


class ProxyDescriptor(object):
    """
    Simple descriptor to enable setting of queries and filters as:

        s = Search()
        s.query = Q(...)

    """
    def __init__(self, name):
        # _quer_proxy
        self._attr_name = '_%s_proxy' % name

    def __get__(self, instance, owner):
        return getattr(instance, self._attr_name)

    def __set__(self, instance, value):
        proxy = getattr(instance, self._attr_name)
        proxy._proxied = proxy._shortcut(value)


class ProxyQuery(BaseProxy):
    _empty = EMPTY_QUERY
    _shortcut = staticmethod(Q)


class ProxyFilter(BaseProxy):
    _empty = EMPTY_FILTER
    _shortcut = staticmethod(F)


class AggsProxy(AggBase, DslBase):
    name = 'aggs'
    def __init__(self, search):
        self._base = self._search = search
        self._params = {'aggs': {}}

    def to_dict(self):
        return super(AggsProxy, self).to_dict().get('aggs', {})


class Search(object):
    query = ProxyDescriptor('query')
    filter = ProxyDescriptor('filter')
    post_filter = ProxyDescriptor('post_filter')

    def __init__(self, using=None, query=None, index=None, doc_type=None, extra=None):
        """
        Search request to elasticsearch.

        :arg using: `Elasticsearch` instance to use
        :arg index: limit the search to index
        :arg doc_type: only query this type.

        All the paramters supplied (or omitted) at creation type can be later
        overriden by methods (`using`, `index` and `doc_type` respectively).
        """
        # ``_using`` should only ever be a value other than ``None`` if it's
        # been forced with the ``.using`` method.
        self._using = using
        self._query = query
        self._determine_backend()  # Create link to query instance : ElasticsearchSearchQuerySet

        self._sort = []
        self._result_cache = []
        self._result_count = None
        self._cache_full = False
        self._load_all = False
        self._ignored_result_count = 0
        self.log = logging.getLogger('sanjab')

        self._index = None
        if isinstance(index, (tuple, list)):
            self._index = list(index)
        elif index:
            self._index = [index]

        self.aggs = AggsProxy(self)
        self._extra = extra or {}
        self._params = {}
        self._fields = None
        self._highlight = {}
        self._highlight_opts = {}
        self._suggest = {}

        self._query_proxy = ProxyQuery(self, 'query')
        self._filter_proxy = ProxyFilter(self, 'filter')
        self._post_filter_proxy = ProxyFilter(self, 'post_filter')

        self._doc_type = []
        self._doc_type_map = {}
        if isinstance(doc_type, (tuple, list)):
            for dt in doc_type:
                self._add_doc_type(dt)
        elif isinstance(doc_type, dict):
            self._doc_type.extend(doc_type.keys())
            self._doc_type_map.update(doc_type)
        elif doc_type:
            self.doc_type(doc_type)

    def _determine_backend(self):
        from sanjab import connections
        # A backend has been manually selected. Use it instead.
        if self._using is not None:
            self._query = connections[self._using].get_query()
            return

        # No backend, so rely on the routers to figure out what's right.
        hints = {}

        if self._query:
            hints['models'] = self._query.models

        backend_alias = connection_router.for_read(**hints)

        if isinstance(backend_alias, (list, tuple)) and len(backend_alias):
            # We can only effectively read from one engine.
            backend_alias = backend_alias[0]

        # The ``SearchQuery`` might swap itself out for a different variant
        # here.
        if self._query:
            self._query = self._query.using(backend_alias)
        else:
            self._query = connections[backend_alias].get_query()

    def __getstate__(self):
        """
        For pickling.
        """
        len(self)
        obj_dict = self.__dict__.copy()
        obj_dict['_iter'] = None
        obj_dict['log'] = None
        return obj_dict

    def __setstate__(self, data_dict):
        """
        For unpickling.
        """
        self.__dict__ = data_dict
        self.log = logging.getLogger('sanjab')

    def __repr__(self):
        data = list(self[:REPR_OUTPUT_SIZE])

        if len(self) > REPR_OUTPUT_SIZE:
            data[-1] = "...(remaining elements truncated)..."

        return repr(data)

    def __len__(self):
        """
        Get result count from query instance
        :return:
        """
        if not self._result_count:
            self._query._raw_query = self.to_dict()
            self._result_count = self._query.get_count()

            # Some backends give weird, false-y values here. Convert to zero.
            if not self._result_count:
                self._result_count = 0

        # This needs to return the actual number of hits, not what's in the cache.
        return self._result_count - self._ignored_result_count

    def __iter__(self):
        """
        Lazy access to results
        1st time:
        self._manual_iter() >
            self._fill_cache() >
                self._query.get_results


        """
        if self._cache_is_full():
            # We've got a fully populated cache. Let Python do the hard work.
            return iter(self._result_cache)

        return self._manual_iter()

    def __and__(self, other):
        if isinstance(other, EmptySearchQuerySet):
            return other._clone()
        combined = self._clone()
        combined._query.combine(other._query, SQ.AND)
        return combined

    def __or__(self, other):
        combined = self._clone()
        if isinstance(other, EmptySearchQuerySet):
            return combined
        combined._query.combine(other._query, SQ.OR)
        return combined

    def _cache_is_full(self):
        """
        Determine if result cache is full
        :return:
        """
        if not self._query.has_run():
            return False

        if len(self) <= 0:  # we have no result so we are done.
            return True

        try:
            self._result_cache.index(None)
            return False
        except ValueError:
            # No ``None``s found in the results. Check the length of the cache.
            return len(self._result_cache) > 0

    def _fill_cache(self, start, end, **kwargs):
        # Tell the query where to start from and how many we'd like.
        self._query._reset()
        self._query._raw_query = self.to_dict()
        self._query.set_limits(start, end)
        results = self._query.get_results(**kwargs)
        if results == None or len(results) == 0:
            return False

        # Setup the full cache now that we know how many results there are.
        # We need the ``None``s as placeholders to know what parts of the
        # cache we have/haven't filled.
        # Using ``None`` like this takes up very little memory. In testing,
        # an array of 100,000 ``None``s consumed less than .5 Mb, which ought
        # to be an acceptable loss for consistent and more efficient caching.
        if len(self._result_cache) == 0:
            self._result_cache = [None for i in range(self._query.get_count())]

        if start is None:
            start = 0

        if end is None:
            end = self._query.get_count()

        to_cache = self.post_process_results(results)

        # Assign by slice.
        self._result_cache[start:start + len(to_cache)] = to_cache
        return True

    def post_process_results(self, results):
        to_cache = []

        # Check if we wish to load all objects.
        if self._load_all:
            result_indexes = {}
            models_doc_pks = {}
            loaded_objects = {}

            """
            "Airline":{
                "AirlineAll": {pk1, pk2, ...}
            }
            """
            # Remember the search position for each result so we don't have to resort later.
            for result in results:
                models_doc_pks.setdefault(result.model, {})
                models_doc_pks[result.model].setdefault(result.doc_type, []).append(result.pk)

            # Load the objects for each model in turn.
            for model, modelResults in models_doc_pks.iteritems():
                ui = connections[self._query._using].get_unified_index()
                index = ui.get_index(model)
                loaded_objects[model] = {}
                for doc_type, doc_pks in modelResults.iteritems():
                    model_index = index.get(doc_type, None)
                    if not model_index:
                        self.log.warning("IndexModel was not found for type: %s", doc_type)
                    objects = model_index.read_queryset(using=self._query._using)
                    try:
                        loaded_objects[model].update(objects.in_bulk(doc_pks))
                    except NotHandled:
                        self.log.warning("Model '%s' not handled by the routers", model)
                        # Revert to old behaviour
                        loaded_objects[model].update(model._default_manager.in_bulk(doc_pks))

        for result in results:
            if self._load_all:
                # We have to deal with integer keys being cast from strings
                model_objects = loaded_objects.get(result.model, {})
                if not result.pk in model_objects:
                    try:
                        result.pk = int(result.pk)
                    except ValueError:
                        pass
                try:
                    result._object = model_objects[result.pk]
                except KeyError:
                    # The object was either deleted since we indexed or should
                    # be ignored; fail silently.
                    self._ignored_result_count += 1
                    continue

            to_cache.append(result)

        return to_cache

    def _manual_iter(self):
        # If we're here, our cache isn't fully populated.
        # For efficiency, fill the cache as we go if we run out of results.
        # Also, this can't be part of the __iter__ method due to Python's rules
        # about generator functions.
        current_position = 0
        current_cache_max = 0
        while True:
            if len(self._result_cache) > 0:
                try:
                    current_cache_max = self._result_cache.index(None)
                except ValueError:
                    current_cache_max = len(self._result_cache)

            while current_position < current_cache_max:
                yield self._result_cache[current_position]
                current_position += 1

            if self._cache_is_full():
                raise StopIteration

            # We've run out of results and haven't hit our limit.
            # Fill more of the cache.
            if not self._fill_cache(current_position, current_position + ITERATOR_LOAD_PER_QUERY):
                raise StopIteration

    def __getitem__(self, k):
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice, six.integer_types)):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0))
                or (isinstance(k, slice) and (k.start is None or k.start >= 0)
                    and (k.stop is None or k.stop >= 0))), \
                "Negative indexing is not supported."

        # Remember if it's a slice or not. We're going to treat everything as
        # a slice to simply the logic and will `.pop()` at the end as needed.
        if isinstance(k, slice):
            is_slice = True
            start = k.start

            if k.stop is not None:
                bound = int(k.stop)
            else:
                bound = None
        else:
            is_slice = False
            start = k
            bound = k + 1

        # We need check to see if we need to populate more of the cache.
        if len(self._result_cache) <= 0 or (None in self._result_cache[start:bound] and not self._cache_is_full()):
            try:
                self._fill_cache(start, bound)
            except StopIteration:
                # There's nothing left, even though the bound is higher.
                pass

        # Cache should be full enough for our needs.
        if is_slice:
            return self._result_cache[start:bound]
        else:
            return self._result_cache[start]

    @classmethod
    def from_dict(cls, d):
        """
        Construct a `Search` instance from a raw dict containing the search
        body.
        """
        s = cls()
        s.update_from_dict(d)
        return s

    def _clone(self):
        """
        Return a clone of the current search request. Performs a shallow copy
        of all the underlying objects. Used internally by most state modifying
        APIs.
        """

        query = self._query._clone()
        query._raw_query = self.to_dict()

        s = self.__class__(using=self._using, query=query, index=self._index,
                           doc_type=self._doc_type)
        s._doc_type_map = self._doc_type_map.copy()
        s._sort = self._sort[:]
        s._fields = self._fields[:] if self._fields is not None else None
        s._extra = self._extra.copy()
        s._highlight = self._highlight.copy()
        s._highlight_opts = self._highlight_opts.copy()
        s._suggest = self._suggest.copy()
        s._load_all = self._load_all
        for x in ('query', 'filter', 'post_filter'):
            getattr(s, x)._proxied = getattr(self, x)._proxied

        # copy top-level bucket definitions
        if self.aggs._params.get('aggs'):
            s.aggs._params = {'aggs': self.aggs._params['aggs'].copy()}
        s._params = self._params.copy()
        return s

    def update_from_dict(self, d):
        """
        Apply options from a serialized body to the current instance. Modifies
        the object in-place.
        """
        d = d.copy()
        if 'query' in d:
            self.query._proxied = Q(d.pop('query'))
        if 'post_filter' in d:
            self.post_filter._proxied = F(d.pop('post_filter'))

        if isinstance(self.query._proxied, Filtered):
            self.filter._proxied = self.query._proxied.filter
            self.query._proxied = self.query._proxied.query

        aggs = d.pop('aggs', d.pop('aggregations', {}))
        if aggs:
            self.aggs._params = {
                'aggs': dict(
                    (name, A(value)) for (name, value) in iteritems(aggs))
            }
        if 'sort' in d:
            self._sort = d.pop('sort')
        if 'fields' in d:
            self._fields = d.pop('fields')
        if 'highlight' in d:
            high = d.pop('highlight').copy()
            self._highlight = high.pop('fields')
            self._highlight_opts = high
        if 'suggest' in d:
            self._suggest = d.pop('suggest')
            if 'text' in self._suggest:
                text = self._suggest.pop('text')
                for s in self._suggest.values():
                    s.setdefault('text', text)
        self._extra = d

    def params(self, **kwargs):
        """
        Specify query params to be used when executing the search. All the
        keyword arguments will override the current values.
        """
        s = self._clone()
        s._params.update(kwargs)
        return s

    def extra(self, **kwargs):
        """
        Add extra keys to the request body.
        """
        s = self._clone()
        if 'from_' in kwargs:
            kwargs['from'] = kwargs.pop('from_')
        s._extra.update(kwargs)
        return s

    def fields(self, fields=None):
        """
        Selectively load specific stored fields for each document.

        :arg fields: list of fields to return for each document

        If `fields` is None, the entire document will be returned for
        each hit.  If fields is the empty list, no fields will be
        returned for each hit, just the _id and _type.
        """
        s = self._clone()
        s._fields = fields
        return s

    def sort(self, *keys):
        """
        Add sorting information to the search request. If called without
        arguments it will remove all sort requirements. Otherwise it will
        replace them. Acceptable arguments are::

            'some.field'
            '-some.other.field'
            {'different.field': {'any': 'dict'}}

        so for example::

            s = Search().sort(
                'category',
                '-title',
                {"price" : {"order" : "asc", "mode" : "avg"}}
            )

        will sort by ``category``, ``title`` (in descending order) and
        ``price`` in ascending order using the ``avg`` mode.

        The API returns a copy of the Search object and can thus be chained.
        """
        s = self._clone()
        s._sort = []
        for k in keys:
            if isinstance(k, string_types) and k.startswith('-'):
                k = {k[1:]: {"order": "desc"}}
            s._sort.append(k)
        return s

    def highlight_options(self, **kwargs):
        s = self._clone()
        s._highlight_opts.update(kwargs)
        return s

    def highlight(self, *fields, **kwargs):
        """
        Request highliting of some fields. All keyword arguments passed in will be
        used as parameters. Example::

            Search().highlight('title', 'body', fragment_size=50)

        will produce the equivalent of::

            {
                "highlight": {
                    "fields": {
                        "body": {"fragment_size": 50},
                        "title": {"fragment_size": 50}
                    }
                }
            }

        """
        s = self._clone()
        for f in fields:
            s._highlight[f] = kwargs
        return s

    def suggest(self, name, text, **kwargs):
        s = self._clone()
        s._suggest[name] = {'text': text}
        s._suggest[name].update(kwargs)
        return s

    def index(self, *index):
        """
        Set the index for the search. If called empty it will rmove all information.

        Example:

            s = Search().index('twitter')
        """
        # .index() resets
        s = self._clone()
        if not index:
            s._index = None
        else:
            s._index = (self._index or []) + list(index)
        return s

    def add_doc_type(self, doc_type):
        """Adds the given document type to the list of types to search for in the index."""
        self._query.add_doc_type(doc_type)
        clone = self._clone()
        clone._query.add_doc_type(doc_type)
        return clone

    def doc_type(self, doc_type):
        """Narrows the search by given document type in the index."""
        self._query.set_doc_type(doc_type)
        clone = self._clone()
        clone._query.set_doc_type(doc_type)
        return clone

    def to_dict(self, count=False, **kwargs):
        """
        Serialize the search into the dictionary that will be sent over as the
        request's body.

        :arg count: a flag to specify we are interested in a body for count -
            no aggregations, no pagination bounds etc.

        All additional keyword arguments will be included into the dictionary.
        """
        if self.filter:
            d = {
              "query": {
                "filtered": {
                  "query": self.query.to_dict(),
                  "filter": self.filter.to_dict()
                }
              }
            }
        else:
            d = {"query": self.query.to_dict()}

        if self.post_filter:
            d['post_filter'] = self.post_filter.to_dict()

        # count request doesn't care for sorting and other things
        if not count:
            if self.aggs.aggs:
                d.update(self.aggs.to_dict())

            if self._sort:
                d['sort'] = self._sort

            d.update(self._extra)

            if self._fields is not None:
                d['fields'] = self._fields

            if self._highlight:
                d['highlight'] = {'fields': self._highlight}
                d['highlight'].update(self._highlight_opts)

            if self._suggest:
                d['suggest'] = self._suggest

        d.update(kwargs)
        return d

    def using(self, connection_name):
        """
        Allows switching which connection the ``SearchQuerySet`` uses to
        search in.
        """
        clone = self._clone()
        clone._query = self._query.using(connection_name)
        clone._using = connection_name
        return clone

    def execute(self):
        """
        Execute the search and return an instance of ``Response`` wrapping all
        the data.
        """
        es = connections.get_connection(self._using)

        return Response(
            es.search(
                index=self._index,
                doc_type=self._doc_type,
                body=self.to_dict(),
                **self._params
            ),
            callbacks=self._doc_type_map
        )

    def count(self):
        """Returns the total number of matching results."""
        return len(self)

    def scan(self):
        es = connections.get_connection(self._using)

        for hit in scan(
                es,
                query=self.to_dict(),
                index=self._index,
                doc_type=self._doc_type,
                **self._params
            ):
            yield self._doc_type_map.get(hit['_type'], Result)(hit)

    def best_match(self):
        """Returns the best/top search result that matches the query."""
        return self[0]

    def latest(self, date_field):
        """Returns the most recent search result that matches the query."""
        clone = self._clone()
        clone._query.clear_order_by()
        clone._query.add_order_by("-%s" % date_field)
        return clone.best_match()

    def values(self, *fields):
        """
        Returns a list of dictionaries, each containing the key/value pairs for
        the result, exactly like Django's ``ValuesQuerySet``.
        """
        qs = self._clone(klass=ValuesSearchQuerySet)
        qs._fields.extend(fields)
        return qs

    def values_list(self, *fields, **kwargs):
        """
        Returns a list of field values as tuples, exactly like Django's
        ``QuerySet.values``.

        Optionally accepts a ``flat=True`` kwarg, which in the case of a
        single field being provided, will return a flat list of that field
        rather than a list of tuples.
        """
        flat = kwargs.pop("flat", False)

        if flat and len(fields) > 1:
            raise TypeError("'flat' is not valid when values_list is called with more than one field.")

        qs = self._clone(klass=ValuesListSearchQuerySet)
        qs._fields.extend(fields)
        qs._flat = flat
        return qs

    # Methods that return a SearchQuerySet.
    def all(self):
        """Returns all results for the query."""
        return self._clone()

    def none(self):
        """Returns an empty result list for the query."""
        return self._clone(klass=EmptySearchQuerySet)

    def load_all(self):
        """Efficiently populates the objects in the search results."""
        clone = self._clone()
        clone._load_all = True
        return clone

    def get_results(self, start=0, end=-1):
        if start == 0 and end == -1:
            return [result for result in self.load_all() if result]
        clone = self._clone()
        return [result for result in clone[start:end] if result]

    @property
    def aggregations(self):
        """
        Returns the aggregations found by the query.

        This will cause the query to execute and should generally be used when
        presenting the data.
        """
        if self._query.has_run():
            return self._query.get_aggregations()
        else:
            clone = self._clone()
            return clone._query.get_aggregations()


class EmptySearchQuerySet(Search):
    """
    A stubbed SearchQuerySet that behaves as normal but always returns no
    results.
    """
    def __len__(self):
        return 0

    def _cache_is_full(self):
        # Pretend the cache is always full with no results.
        return True

    def _clone(self, klass=None):
        clone = super(EmptySearchQuerySet, self)._clone(klass=klass)
        clone._result_cache = []
        return clone

    def _fill_cache(self, start, end):
        return False

    def facet_counts(self):
        return {}


class ValuesListSearchQuerySet(Search):
    """
    A ``SearchQuerySet`` which returns a list of field values as tuples, exactly
    like Django's ``ValuesListQuerySet``.
    """
    def __init__(self, *args, **kwargs):
        super(ValuesListSearchQuerySet, self).__init__(*args, **kwargs)
        self._flat = False
        self._fields = []

        # Removing this dependency would require refactoring much of the backend
        # code (_process_results, etc.) and these aren't large enough to make it
        # an immediate priority:
        self._internal_fields = ['id', 'django_ct', 'django_id', 'score']

    def _clone(self, klass=None):
        clone = super(ValuesListSearchQuerySet, self)._clone(klass=klass)
        clone._fields = self._fields
        clone._flat = self._flat
        return clone

    def _fill_cache(self, start, end):
        query_fields = set(self._internal_fields)
        query_fields.update(self._fields)
        kwargs = {
            'fields': query_fields
        }
        return super(ValuesListSearchQuerySet, self)._fill_cache(start, end, **kwargs)

    def post_process_results(self, results):
        to_cache = []

        if self._flat:
            accum = to_cache.extend
        else:
            accum = to_cache.append

        for result in results:
            accum([getattr(result, i, None) for i in self._fields])

        return to_cache


class ValuesSearchQuerySet(ValuesListSearchQuerySet):
    """
    A ``SearchQuerySet`` which returns a list of dictionaries, each containing
    the key/value pairs for the result, exactly like Django's
    ``ValuesQuerySet``.
    """
    def _fill_cache(self, start, end):
        query_fields = set(self._internal_fields)
        query_fields.update(self._fields)
        kwargs = {
            'fields': query_fields
        }
        return super(ValuesListSearchQuerySet, self)._fill_cache(start, end, **kwargs)

    def post_process_results(self, results):
        to_cache = []

        for result in results:
            to_cache.append(dict((i, getattr(result, i, None)) for i in self._fields))

        return to_cache
