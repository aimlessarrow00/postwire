class PostwireError(Exception):
    pass


class MalformedEvent(PostwireError):
    pass


class RetryableEventError(PostwireError):
    pass


class PermanentEventError(PostwireError):
    pass
