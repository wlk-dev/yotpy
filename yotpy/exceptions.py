"""
Includes custom exceptions for use with the yotpy package.
"""

class CustomException(Exception):
    """A custom exception to be used with request processing utility methods."""
    pass

class SessionNotCreatedError(CustomException):
    """Raised when the aiohttp.ClientSession has not been created before making a request."""

    def __init__(self, message="Session not created. Please use `async with` context manager to make requests."):
        super().__init__(message)


class FailedToGetTokenError(CustomException):
    """Raised when the token cannot be retrieved."""

    def __init__(self, message="Failed to get token."):
        super().__init__(message)


class PreflightException(CustomException):
    """Raised when the preflight check fails."""

    def __init__(self, method: str, message="Preflight check failed. Disallowed method: {method}."):
        message = message.format(method=method)
        super().__init__(message)


class UserNotFound(CustomException):
    """Raised when the user cannot be retrieved."""

    def __init__(self, message="User not found. Make sure you're using the correct credentials."):
        super().__init__(message)


class AppNotFound(CustomException):
    """Raised when the app data cannot be retrieved."""

    def __init__(self, message="App not found. Make sure you're using the correct credentials."):
        super().__init__(message)


class UploadException(CustomException):
    """Raised when review request file upload fails."""

    def __init__(self, message="Failed to upload file. Check integrity of file and that you have `POST` permission."):
        super().__init__(message)


class SendException(CustomException):
    """Raised when review request send fails."""

    def __init__(self, message="Failed to send review request FormData. Check that you have `POST` permission."):
        super().__init__(message)

