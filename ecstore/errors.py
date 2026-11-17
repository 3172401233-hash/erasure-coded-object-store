class ECStoreError(Exception):
    """Base class for all ecstore errors."""


class NodeTimeout(ECStoreError):
    """A storage node did not answer within its deadline."""


class WriteError(ECStoreError):
    """A put() could not stage enough shards and was aborted."""


class ObjectNotFound(ECStoreError):
    """No committed manifest exists for the object."""


class UnrecoverableError(ECStoreError):
    """Fewer than k valid shards of a single version remain."""


class RepairError(ECStoreError):
    """A repair plan could not be executed."""
