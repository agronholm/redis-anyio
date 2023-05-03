class RedisError(Exception):
    """Base class for all Redis related errors."""


class ResponseError(RedisError):
    """Raised when the server responds to a command with an error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(code, message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return f"{self.code} {self.message}"


class ConnectivityError(RedisError):
    """
    Raised when an operation fails due to a non-retryable server connectivity error.
    """


class ProtocolError(RedisError):
    """Raised when there's a problem decoding incoming data from the server."""
