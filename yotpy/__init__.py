"""
Yotpy: An easy-to-use Python wrapper for the Yotpo web API.
"""

__version__ = "0.0.91"

from .core import YotpoAPIWrapper, JSONTransformer
from .exceptions import CustomException, SessionNotCreatedError, FailedToGetTokenError, PreflightException, UploadException, SendException, UserNotFound, AppNotFound
