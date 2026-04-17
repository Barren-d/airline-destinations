"""PyFly domain exceptions."""


class ScraperError(RuntimeError):
    """Raised when a scraper fails completely (all airports returned 0 results)."""


class AuthError(RuntimeError):
    """Raised when API credentials are missing or rejected."""
