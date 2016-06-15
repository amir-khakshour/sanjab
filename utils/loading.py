from __future__ import unicode_literals
import copy
import inspect
import warnings
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.datastructures import SortedDict
from django.utils import importlib
from django.utils.module_loading import module_has_submodule
from sanjab.constants import Indexable, DEFAULT_ALIAS
from sanjab.exceptions import NotHandled, SearchFieldError


def import_class(path):
    path_bits = path.split('.')
    # Cut off the class name at the end.
    class_name = path_bits.pop()
    module_path = '.'.join(path_bits)
    module_itself = importlib.import_module(module_path)

    if not hasattr(module_itself, class_name):
        raise ImportError("The Python module '%s' has no '%s' class." % (module_path, class_name))

    return getattr(module_itself, class_name)


# Load the search backend.
def load_backend(full_backend_path):
    """
    Loads a backend for interacting with the search engine.

    Requires a ``backend_path``. It should be a string resembling a Python
    import path, pointing to a ``BaseEngine`` subclass. The built-in options
    available include::

      * sanjab.backends.solr.SolrEngine
      * sanjab.backends.xapian.XapianEngine (third-party)
      * sanjab.backends.whoosh.WhooshEngine
      * sanjab.backends.simple.SimpleEngine

    If you've implemented a custom backend, you can provide the path to
    your backend & matching ``Engine`` class. For example::

      ``myapp.search_backends.CustomSolrEngine``

    """
    path_bits = full_backend_path.split('.')

    if len(path_bits) < 2:
        raise ImproperlyConfigured("The provided backend '%s' is not a complete Python path to a BaseEngine subclass." % full_backend_path)

    return import_class(full_backend_path)


def load_router(full_router_path):
    """
    Loads a router for choosing which connection to use.

    Requires a ``full_router_path``. It should be a string resembling a Python
    import path, pointing to a ``BaseRouter`` subclass. The built-in options
    available include::

      * sanjab.routers.DefaultRouter

    If you've implemented a custom backend, you can provide the path to
    your backend & matching ``Engine`` class. For example::

      ``myapp.search_routers.MasterSlaveRouter``

    """
    path_bits = full_router_path.split('.')

    if len(path_bits) < 2:
        raise ImproperlyConfigured("The provided router '%s' is not a complete Python path to a BaseRouter subclass." % full_router_path)

    return import_class(full_router_path)


class ConnectionHandler(object):
    def __init__(self, connections_info):
        self.connections_info = connections_info
        self._connections = {}
        self._index = None

    def ensure_defaults(self, alias):
        try:
            conn = self.connections_info[alias]
        except KeyError:
            raise ImproperlyConfigured("The key '%s' isn't an available connection." % alias)

        if not conn.get('ENGINE'):
            conn['ENGINE'] = 'sanjab.backends.simple_backend.SimpleEngine'

    def __getitem__(self, key):
        # Load Engines
        if key in self._connections:
            return self._connections[key]
        self.ensure_defaults(key)
        self._connections[key] = load_backend(self.connections_info[key]['ENGINE'])(using=key)
        return self._connections[key]

    def reload(self, key):
        try:
            del self._connections[key]
        except KeyError:
            pass

        return self.__getitem__(key)

    def all(self):
        return [self[alias] for alias in self.connections_info]


class ConnectionRouter(object):
    def __init__(self, routers_list=None):
        self.routers_list = routers_list
        self.routers = []

        if self.routers_list is None:
            self.routers_list = ['sanjab.routers.DefaultRouter']

        for router_path in self.routers_list:
            router_class = load_router(router_path)
            self.routers.append(router_class())

    def for_action(self, action, **hints):
        conns = []

        for router in self.routers:
            if hasattr(router, action):
                action_callable = getattr(router, action)
                connection_to_use = action_callable(**hints)

                if connection_to_use is not None:
                    conns.append(connection_to_use)

        return conns

    def for_write(self, **hints):
        return self.for_action('for_write', **hints)

    def for_read(self, **hints):
        return self.for_action('for_read', **hints)


class UnifiedIndex(object):
    # Used to collect all the indexes into a cohesive whole.
    def __init__(self, excluded_indexes=None):
        self.indexes = dict()
        self.index_objects = list()
        self.fields = SortedDict()
        self._built = False
        self.excluded_indexes = excluded_indexes or []
        self.excluded_indexes_ids = {}
        self.document_field = getattr(settings, 'SANJAB_DOCUMENT_FIELD', 'text')
        self._fieldnames = {}
        self._facet_fieldnames = {}

    def collect_indexes(self):
        indexes = []

        for app in settings.INSTALLED_APPS:
            try:
                mod = importlib.import_module(app)
            except ImportError:
                warnings.warn('Installed app %s is not an importable Python module and will be ignored' % app)
                continue

            try:
                search_index_module = importlib.import_module("%s.search_indexes" % app)
            except ImportError:
                if module_has_submodule(mod, 'search_indexes'):
                    raise

                continue

            for item_name, item in inspect.getmembers(search_index_module, inspect.isclass):
                if getattr(item, 'sanjab_use_for_indexing', False) and getattr(item, 'get_model', None):
                    # We've got an index. Check if we should be ignoring it.
                    class_path = "%s.search_indexes.%s" % (app, item_name)

                    if class_path in self.excluded_indexes or self.excluded_indexes_ids.get(item_name) == id(item):
                        self.excluded_indexes_ids[str(item_name)] = id(item)
                        continue

                    indexes.append(item())

        self.index_objects = indexes
        return indexes

    def reset(self):
        self.indexes = dict()
        self.index_objects = list()
        self.fields = SortedDict()
        self._built = False
        self._fieldnames = {}
        self._facet_fieldnames = {}

    def all_index_objects(self):
        if not self._built:
            self.build()

        return self.index_objects

    def build(self, indexes=None):
        self.reset()

        if indexes is None:
            indexes = self.collect_indexes()

        for index in indexes:
            model = index.get_model()

            if model in self.indexes:
                self.indexes[model].update({index.get_type(): index})
            else:
                self.indexes[model] = {index.get_type(): index}

            if index.base:
                # if 'base' in self.indexes[model]:
                #     raise ImproperlyConfigured(
                #         "Model '%s' has more than one base index. "
                #         "Please define only %s or %s as your base index." % (
                #             model, self.indexes[model]['base'], index
                #         )
                #     )
                self.indexes[model]['base'] = index

            self.collect_fields(index)

        for model, indexes in self.indexes.iteritems():
            if 'base' not in indexes:
                _index = next(iter(self.indexes[model]))
                self.indexes[model]['base'] = _index

        self._built = True

    def get_index_fields(self, index):
        """
        Get fields of given index
        :param index:
        :return:
        """
        if not self._built:
            self.build()
        i_type = index.get_type()
        if i_type in self.fields:
            return self.fields[i_type]

    def collect_fields(self, index):
        i_type = index.get_type()
        self.fields[i_type] = SortedDict()

        for fieldname, field_obj in index.fields.items():
            if field_obj.document is True:
                if field_obj.index_fieldname != self.document_field:
                    raise SearchFieldError("All 'SearchIndex' classes must use the same '%s' fieldname for the 'document=True' field. Offending index is '%s'." % (self.document_field, index))

            # Stow the index_fieldname so we don't have to get it the hard way again.
            # if fieldname in self._fieldnames and field_obj.index_fieldname != self._fieldnames[fieldname]:
            #     # We've already seen this field in the list. Raise an exception if index_fieldname differs.
            #     raise SearchFieldError("All uses of the '%s' field need to use the same 'index_fieldname' attribute." % fieldname)

            self._fieldnames[fieldname] = field_obj.index_fieldname

            # Stow the facet_fieldname so we don't have to look that up either.
            if hasattr(field_obj, 'facet_for'):
                if field_obj.facet_for:
                    self._facet_fieldnames[i_type][field_obj.facet_for] = fieldname
                else:
                    self._facet_fieldnames[i_type][field_obj.instance_name] = fieldname

            # Copy the field in so we've got a unified schema.
            if field_obj.index_fieldname not in self.fields[i_type]:
                self.fields[i_type][field_obj.index_fieldname] = field_obj
                self.fields[i_type][field_obj.index_fieldname] = copy.copy(field_obj)
            else:
                # If the field types are different, we can mostly
                # safely ignore this. The exception is ``MultiValueField``,
                # in which case we'll use it instead, copying over the
                # values.
                if field_obj.is_multivalued == True:
                    old_field = self.fields[i_type][field_obj.index_fieldname]
                    self.fields[i_type][field_obj.index_fieldname] = field_obj
                    self.fields[i_type][field_obj.index_fieldname] = copy.copy(field_obj)

                    # Switch it so we don't have to dupe the remaining
                    # checks.
                    field_obj = old_field

                # We've already got this field in the list. Ensure that
                # what we hand back is a superset of all options that
                # affect the schema.
                if field_obj.indexed is True:
                    self.fields[i_type][field_obj.index_fieldname].indexed = True

                if field_obj.stored is True:
                    self.fields[i_type][field_obj.index_fieldname].stored = True

                if field_obj.faceted is True:
                    self.fields[i_type][field_obj.index_fieldname].faceted = True

                if field_obj.use_template is True:
                    self.fields[i_type][field_obj.index_fieldname].use_template = True

                if field_obj.null is True:
                    self.fields[i_type][field_obj.index_fieldname].null = True

    def get_indexed_models(self):
        if not self._built:
            self.build()

        return list(self.indexes.keys())

    def get_index_fieldname(self, field):
        if not self._built:
            self.build()

        return self._fieldnames.get(field) or field

    def get_index(self, model_klass):
        if not self._built:
            # Build if not Built
            self.build()

        if model_klass not in self.indexes:
            raise NotHandled('The model %s is not registered' % model_klass.__class__)

        return self.indexes[model_klass]

    def get_facet_fieldname(self, field):
        if not self._built:
            self.build()

        for fieldname, field_obj in self.fields.items():
            if fieldname != field:
                continue

            if hasattr(field_obj, 'facet_for'):
                if field_obj.facet_for:
                    return field_obj.facet_for
                else:
                    return field_obj.instance_name
            else:
                return self._facet_fieldnames.get(field) or field

        return field

    def all_searchfields(self):
        if not self._built:
            self.build()

        return self.fields
