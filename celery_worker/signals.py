from django.db.models import signals

from sanjab.signals import BaseSignalProcessor
from sanjab.exceptions import NotHandled

from sanjab.celery_worker.utils import enqueue_task
from sanjab.celery_worker.indexes import CelerySearchIndex


class CelerySignalProcessor(BaseSignalProcessor):

    def setup(self):
        signals.post_save.connect(self.enqueue_save)
        signals.post_delete.connect(self.enqueue_delete)

    def teardown(self):
        signals.post_save.disconnect(self.enqueue_save)
        signals.post_delete.disconnect(self.enqueue_delete)

    def enqueue_save(self, sender, instance, **kwargs):
        return self.enqueue('update', instance, sender, **kwargs)

    def enqueue_delete(self, sender, instance, **kwargs):
        return self.enqueue('delete', instance, sender, **kwargs)

    def enqueue(self, action, instance, sender, **kwargs):
        """
        Given an individual model instance, determine if a backend
        handles the model, check if the index is Celery-enabled and
        enqueue task.
        """
        using_backends = self.connection_router.for_write(instance=instance)

        for using in using_backends:
            try:
                indexes = self.connections[using].get_unified_index().get_index(sender)
                if 'base' in indexes:
                    base = indexes.pop('base')
                for doc_type, index in indexes.iteritems():
                    if isinstance(index, CelerySearchIndex):
                        if action == 'update' and not index.should_update(instance):
                            continue
                        enqueue_task(action, instance)
                        # index.update_object(instance, doc_type=doc_type, using=using, index=index)
            except NotHandled:
                # TODO: Maybe log it or let the exception bubble?
                pass
