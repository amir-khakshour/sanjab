from __future__ import unicode_literals


class SanjabError(Exception):
    """A generic exception for all others to extend."""
    pass

class SearchBackendError(SanjabError):
    """Raised when a backend can not be found."""
    pass

class SearchFieldError(SanjabError):
    """Raised when a field encounters an error."""
    pass

class MissingDependency(SanjabError):
    """Raised when a library a backend depends on can not be found."""
    pass

class NotHandled(SanjabError):
    """Raised when a model is not handled by the router setup."""
    pass

class MoreLikeThisError(SanjabError):
    """Raised when a model instance has not been provided for More Like This."""
    pass

class FacetingError(SanjabError):
    """Raised when incorrect arguments have been provided for faceting."""
    pass

class SpatialError(SanjabError):
    """Raised when incorrect arguments have been provided for spatial."""
    pass

class StatsError(SanjabError):
    "Raised when incorrect arguments have been provided for stats"
    pass
