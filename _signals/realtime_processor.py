from django.db import models

from sanjab.signals import BaseSignalProcessor
from sanjab.exceptions import NotHandled
from sanjab.indexes import RealtimeSeachIndex


class RealtimeSignalProcessor(BaseSignalProcessor):
    """
    Allows for observing when saves/deletes fire & automatically updates the
    search engine appropriately.
    """
    def setup(self):
        # Naive (listen to all model saves).
        models.signals.post_save.connect(self.handle_save)
        models.signals.post_delete.connect(self.handle_delete)
        # Efficient would be going through all backends & collecting all models
        # being used, then hooking up signals only for those.

    def teardown(self):
        # Naive (listen to all model saves).
        models.signals.post_save.disconnect(self.handle_save)
        models.signals.post_delete.disconnect(self.handle_delete)
        # Efficient would be going through all backends & collecting all models
        # being used, then disconnecting signals only for those.

    def handle_save(self, sender, instance, **kwargs):
        """
        Given an individual model instance, determine which backends the
        update should be sent to & update the object on those backends.
        """
        using_backends = self.connection_router.for_write(instance=instance)

        for using in using_backends:
            try:
                indexes = self.connections[using].get_unified_index().get_index(sender)
                if 'base' in indexes:
                    base = indexes.pop('base')
                for doc_type, index in indexes.iteritems():
                    if isinstance(index, RealtimeSeachIndex):
                        index.update_object(instance, doc_type=doc_type, using=using, index=index)
            except NotHandled:
                # TODO: Maybe log it or let the exception bubble?
                pass

    def handle_delete(self, sender, instance, **kwargs):
        """
        Given an individual model instance, determine which backends the
        delete should be sent to & delete the object on those backends.
        """
        using_backends = self.connection_router.for_write(instance=instance)

        for using in using_backends:
            try:
                indexes = self.connections[using].get_unified_index().get_index(sender)
                if 'base' in indexes:
                    base = indexes.pop('base')
                for doc_type, index in indexes.iteritems():
                    if isinstance(index, RealtimeSeachIndex):
                        index.remove_object(instance, doc_type=doc_type, using=using)
            except NotHandled:
                # TODO: Maybe log it or let the exception bubble?
                pass