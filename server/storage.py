from meta import ThreadSafeSingletonMeta


class Storage(metaclass=ThreadSafeSingletonMeta):
    _base_dir: str = "/mnt/data"
