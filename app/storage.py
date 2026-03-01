from time import monotonic


class CacheStorage:
    def __init__(self):
        self._storage: dict[str, tuple[any, float | None]] = {}
        
    def set(self, key, value, ttl=None):
        if ttl is not None and ttl <= 0:
            raise ValueError("-ERR invalid expire time in set")
        expire_at = None
        now = monotonic()
        if ttl is not None:
            expire_at = now + ttl
        self._storage[key] = (value, expire_at)
        
    def get(self, key):
        if key not in self._storage:
            return None
            
        value, expire_at = self._storage[key]
        if expire_at is not None and monotonic() > expire_at:
            del self._storage[key]
            return None
        return value
    
_storage_instance = None    
    
def get_storage() -> CacheStorage:
    global _storage_instance
    if _storage_instance is None:
        _storage_instance = CacheStorage()
    return _storage_instance