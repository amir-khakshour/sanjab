from __future__ import unicode_literals
import logging
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from sanjab.constants import DEFAULT_ALIAS
from sanjab import signals
from sanjab.utils import loading


__author__ = 'Amir Khakshour'
__version__ = (1, 1, 0, 'dev')


# Setup default logging.
log = logging.getLogger('sanjab')
stream = logging.StreamHandler()
stream.setLevel(logging.INFO)
log.addHandler(stream)


# Help people clean up from 1.X.
if hasattr(settings, 'SANJAB_SITECONF'):
    raise ImproperlyConfigured('The SANJAB_SITECONF setting is no longer used & can be removed.')
if hasattr(settings, 'SANJAB_SEARCH_ENGINE'):
    raise ImproperlyConfigured('The SANJAB_SEARCH_ENGINE setting has been replaced with SANJAB_CONNECTIONS.')
if hasattr(settings, 'SANJAB_ENABLE_REGISTRATIONS'):
    raise ImproperlyConfigured('The SANJAB_ENABLE_REGISTRATIONS setting is no longer used & can be removed.')
if hasattr(settings, 'SANJAB_INCLUDE_SPELLING'):
    raise ImproperlyConfigured('The SANJAB_INCLUDE_SPELLING setting is now a per-backend setting & belongs in SANJAB_CONNECTIONS.')


# Check the 2.X+ bits.
if not hasattr(settings, 'SANJAB_CONNECTIONS'):
    raise ImproperlyConfigured('The SANJAB_CONNECTIONS setting is required.')
if DEFAULT_ALIAS not in settings.SANJAB_CONNECTIONS:
    raise ImproperlyConfigured("The default alias '%s' must be included in the SANJAB_CONNECTIONS setting." % DEFAULT_ALIAS)

# Load the connections.
connections = loading.ConnectionHandler(settings.SANJAB_CONNECTIONS)

# Load the router(s).
connection_router = loading.ConnectionRouter()

if hasattr(settings, 'SANJAB_ROUTERS'):
    if not isinstance(settings.SANJAB_ROUTERS, (list, tuple)):
        raise ImproperlyConfigured("The SANJAB_ROUTERS setting must be either a list or tuple.")

    connection_router = loading.ConnectionRouter(settings.SANJAB_ROUTERS)

# Setup the signal processor.
signal_processor_path = getattr(settings, 'SANJAB_SIGNAL_PROCESSOR', 'sanjab.signals.BaseSignalProcessor')
signal_processor_class = loading.import_class(signal_processor_path)
signal_processor = signal_processor_class(connections, connection_router)
#print "signal_processor_class: %s" % signal_processor_class
#print "signal_processor: %s" % signal_processor


# Per-request, reset the ghetto query log.
# Probably not extraordinarily thread-safe but should only matter when
# DEBUG = True.
def reset_search_queries(**kwargs):
    for conn in connections.all():
        conn.reset_queries()


if settings.DEBUG:
    from django.core import signals as django_signals
    django_signals.request_started.connect(reset_search_queries)

