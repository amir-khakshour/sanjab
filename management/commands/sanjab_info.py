from __future__ import print_function
from __future__ import unicode_literals
from django.core.management.base import NoArgsCommand


class Command(NoArgsCommand):
    help = "Provides feedback about the current Sanjab setup."

    def handle_noargs(self, **options):
        """Provides feedback about the current Sanjab setup."""
        from sanjab import connections

        unified_index = connections['default'].get_unified_index()
        indexed = unified_index.get_indexed_models()
        index_count = len(indexed)
        print("Number of handled %s index(es)." % index_count)

        for index in indexed:
            print("  - Model: %s by Index: %s" % (index.__name__, unified_index.indexes[index]))
