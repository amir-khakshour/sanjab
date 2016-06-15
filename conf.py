from django.conf import settings
from sanjab.management.commands import update_index as cmd


#: The default alias to
SANJAB_CELERY_DEFAULT_ALIAS = getattr(settings, 'SANJAB_CELERY_DEFAULT_ALIAS', 'default')
#: The delay (in seconds) before task will be executed (Celery countdown)
SANJAB_SANJAB_CELERY_COUNTDOWN = getattr(settings, 'SANJAB_SANJAB_CELERY_COUNTDOWN', 0)
#: The delay (in seconds) after which a failed index is retried
SANJAB_CELERY_RETRY_DELAY = getattr(settings, 'SANJAB_CELERY_RETRY_DELAY', 5 * 60)
#: The number of retries that are done
SANJAB_CELERY_MAX_RETRIES = getattr(settings, 'SANJAB_CELERY_MAX_RETRIES', 1)
#: The default Celery task class
SANJAB_CELERY_DEFAULT_TASK = getattr(settings, 'SANJAB_CELERY_DEFAULT_TASK', 'sanjab.tasks.CeleryHaystackSignalHandler')
#: The name of the celery queue to use, or None for default
SANJAB_CELERY_QUEUE = getattr(settings, 'SANJAB_CELERY_QUEUE', None)
#: Whether the task should be handled transaction safe
SANJAB_CELERY_TRANSACTION_SAFE = getattr(settings, 'SANJAB_CELERY_TRANSACTION_SAFE', True)

#: The batch size used by the CeleryHaystackUpdateIndex task
SANJAB_CELERY_COMMAND_BATCH_SIZE = getattr(settings, 'SANJAB_CELERY_COMMAND_BATCH_SIZE',
	getattr(cmd, 'DEFAULT_BATCH_SIZE', None))

#: The max age of items used by the CeleryHaystackUpdateIndex task
SANJAB_CELERY_COMMAND_AGE = getattr(settings, 'SANJAB_CELERY_COMMAND_AGE', getattr(cmd, 'DEFAULT_AGE', None))
#: Wehther to remove items from the index that aren't in the DB anymore
SANJAB_CELERY_COMMAND_REMOVE = getattr(settings, 'SANJAB_CELERY_COMMAND_REMOVE', False)
#: The number of multiprocessing workers used by the CeleryHaystackUpdateIndex task
SANJAB_CELERY_COMMAND_WORKERS = getattr(settings, 'SANJAB_CELERY_COMMAND_WORKERS', 0)
#: The names of apps to run update_index for
SANJAB_CELERY_COMMAND_APPS = getattr(settings, 'SANJAB_CELERY_COMMAND_APPS', [])
#: The verbosity level of the update_index call
SANJAB_CELERY_COMMAND_VERBOSITY = getattr(settings, 'SANJAB_CELERY_COMMAND_VERBOSITY', 1)

signal_processor = getattr(settings, 'SANJAB_SIGNAL_PROCESSOR', None)

