from __future__ import unicode_literals

try:
    from django.conf.urls import patterns, url
except ImportError:
    from django.conf.urls.defaults import patterns, url

from sanjab.views import SearchView


urlpatterns = patterns('sanjab.views',
    url(r'^$', SearchView(), name='sanjab_search'),
)
