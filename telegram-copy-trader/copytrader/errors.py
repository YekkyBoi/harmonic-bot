"""Shared exception types."""


class ParserError(Exception):
    """The signal parser could not produce a usable result (API error, refusal, bad output).

    ``str(err)`` is shown to the owner in Turkish. ``transient`` is True for
    network / timeout / rate-limit / 5xx failures that are worth retrying
    (None = unknown; the engine then inspects ``__cause__``)."""

    def __init__(self, message: str = "", *, transient: bool | None = None) -> None:
        super().__init__(message)
        self.transient = transient
