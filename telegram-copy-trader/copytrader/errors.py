"""Shared exception types."""


class ParserError(Exception):
    """The signal parser could not produce a usable result (API error, refusal, bad output).

    ``str(err)`` is shown to the owner in Turkish."""
