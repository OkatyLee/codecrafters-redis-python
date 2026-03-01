from unittest.mock import patch

import pytest

from app.storage import CacheStorage


def test_cache_storage_set_and_get_value():
	storage = CacheStorage()

	storage.set("my_key", "my_value")

	assert storage.get("my_key") == "my_value"


def test_cache_storage_get_missing_key_returns_none():
	storage = CacheStorage()

	assert storage.get("missing_key") is None


def test_cache_storage_invalid_ttl_raises_value_error():
	storage = CacheStorage()

	with pytest.raises(ValueError, match="-ERR invalid expire time in set"):
		storage.set("my_key", "my_value", ttl=0)

	with pytest.raises(ValueError, match="-ERR invalid expire time in set"):
		storage.set("my_key", "my_value", ttl=-1.5)


@patch("app.storage.monotonic")
def test_cache_storage_ttl_expires_and_deletes_key(mock_monotonic):
	mock_monotonic.return_value = 100.0
	storage = CacheStorage()
	storage.set("temp_key", "temp_value", ttl=5.0)

	mock_monotonic.return_value = 104.9
	assert storage.get("temp_key") == "temp_value"

	mock_monotonic.return_value = 105.1
	assert storage.get("temp_key") is None
	assert "temp_key" not in storage._storage


def test_get_storage_returns_singleton_instance():
	import app.storage

	original_instance = app.storage._storage_instance
	app.storage._storage_instance = None
	try:
		instance1 = app.storage.get_storage()
		instance2 = app.storage.get_storage()
		assert instance1 is instance2
		assert isinstance(instance1, CacheStorage)
	finally:
		app.storage._storage_instance = original_instance


