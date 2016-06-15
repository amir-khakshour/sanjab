from __future__ import unicode_literals
from sanjab.exceptions import NotHandled
from django.db import models


class BaseSignalProcessor(object):
    """
    A convenient way to attach Sanjab to Django's signals & cause things to
    index.

    By default, does nothing with signals but provides underlying functionality.
    """
    def __init__(self, connections, connection_router):
        self.connections = connections
        self.connection_router = connection_router
        self.setup()

    def setup(self):
        """
        A hook for setting up anything necessary for
        ``handle_save/handle_delete`` to be executed.

        Default behavior is to do nothing (``pass``).
        """
        # Do nothing.
        pass

    def teardown(self):
        """
        A hook for tearing down anything necessary for
        ``handle_save/handle_delete`` to no longer be executed.

        Default behavior is to do nothing (``pass``).
        """
        # Do nothing.
        pass

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
                    index.remove_object(instance, doc_type=doc_type, using=using)
            except NotHandled:
                # TODO: Maybe log it or let the exception bubble?
                pass



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
                    index.remove_object(instance, doc_type=doc_type, using=using)
            except NotHandled:
                # TODO: Maybe log it or let the exception bubble?
                pass
