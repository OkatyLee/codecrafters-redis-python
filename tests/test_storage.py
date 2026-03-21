from unittest.mock import patch
import os
import tempfile
import time as time_module

import pytest

from app import storage
from app.parser import RESPError
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


# ---------- BLPOP tests ----------

@pytest.mark.asyncio
async def test_blpop_returns_immediately_when_list_has_elements():
	storage = CacheStorage()
	storage.lpush("mylist", b"a", b"b", b"c")

	result = await storage.blpop("mylist", timeout=0)

	assert result == ("mylist", b"c")


@pytest.mark.asyncio
async def test_blpop_returns_from_first_non_empty_key():
	storage = CacheStorage()
	storage.lpush("list2", b"val")

	result = await storage.blpop("list1", "list2", timeout=0)

	assert result == ("list2", b"val")


@pytest.mark.asyncio
async def test_blpop_returns_none_on_timeout():
	storage = CacheStorage()

	result = await storage.blpop("empty", timeout=0.1)

	assert result is None


@pytest.mark.asyncio
async def test_blpop_blocks_until_push():
	import asyncio

	storage = CacheStorage()

	async def push_later():
		await asyncio.sleep(0.05)
		storage.lpush("mylist", b"delayed")

	task = asyncio.create_task(push_later())
	result = await storage.blpop("mylist", timeout=2)

	assert result == ("mylist", b"delayed")
	await task


@pytest.mark.asyncio
async def test_blpop_multiple_keys_blocks_until_push():
	import asyncio

	storage = CacheStorage()

	async def push_later():
		await asyncio.sleep(0.05)
		storage.rpush("key2", b"hello")

	task = asyncio.create_task(push_later())
	result = await storage.blpop("key1", "key2", timeout=2)

	assert result == ("key2", b"hello")
	await task


@pytest.mark.asyncio
async def test_blpop_no_timeout_blocks_until_push():
	"""timeout=0 means block indefinitely; push unblocks it."""
	import asyncio

	storage = CacheStorage()

	async def push_later():
		await asyncio.sleep(0.05)
		storage.lpush("k", b"x")

	task = asyncio.create_task(push_later())
	result = await storage.blpop("k", timeout=0)

	assert result == ("k", b"x")
	await task


def test_lpop_does_not_destroy_remaining_elements():
	"""Regression: lpop must not overwrite the list with popped values."""
	storage = CacheStorage()
	storage.rpush("mylist", b"a", b"b", b"c")

	popped = storage.lpop("mylist")
	assert popped == [b"a"]

	remaining = storage.lrange("mylist", 0, -1)
	assert remaining == [b"b", b"c"]


@patch("app.types.time")
def test_xadd_full_autogen_stream_id(mock_time):
	storage = CacheStorage()
	mock_time.return_value = 1710000000.123

	stream_id = storage.xadd("mystream", "*", [b"field", b"value"])

	assert stream_id == "1710000000123-0"


@patch("app.types.time")
def test_xadd_partial_autogen_stream_id(mock_time):
	storage = CacheStorage()
	mock_time.return_value = 1710000000.123

	first_id = storage.xadd("mystream", "*", [b"field", b"v1"])
	second_id = storage.xadd("mystream", "1710000000123-*", [b"field", b"v2"])

	assert first_id == "1710000000123-0"
	assert second_id == "1710000000123-1"


def test_xadd_appends_entries_and_preserves_history():
	storage = CacheStorage()

	first_id = storage.xadd("mystream", "1-1", [b"f1", b"v1"])
	second_id = storage.xadd("mystream", "1-2", [b"f2", b"v2"])

	assert first_id == "1-1"
	assert second_id == "1-2"

	stream = storage.get("mystream")
	assert stream is not None
	assert stream["ID"] == "1-2"
	assert stream["entries"] == [
		("1-1", {b"f1": b"v1"}),
		("1-2", {b"f2": b"v2"}),
	]


def test_xadd_rejects_equal_or_smaller_id_than_last():
	storage = CacheStorage()
	storage.xadd("mystream", "5-1", [b"field", b"v1"])

	with pytest.raises(RESPError, match="equal or smaller"):
		storage.xadd("mystream", "5-1", [b"field", b"v2"])

	with pytest.raises(RESPError, match="equal or smaller"):
		storage.xadd("mystream", "5-0", [b"field", b"v3"])


def test_xadd_rejects_zero_zero_id():
	storage = CacheStorage()

	with pytest.raises(RESPError, match="greater than 0-0"):
		storage.xadd("mystream", "0-0", [b"field", b"value"])


def test_xadd_rejects_odd_number_of_payload_arguments():
	storage = CacheStorage()

	with pytest.raises(RESPError, match="wrong number of arguments"):
		storage.xadd("mystream", "1-1", [b"field"])


@patch("app.types.time")
def test_xadd_full_autogen_keeps_monotonicity_when_time_goes_back(mock_time):
	storage = CacheStorage()
	mock_time.side_effect = [1710000000.123, 1700000000.000]

	first_id = storage.xadd("mystream", "*", [b"field", b"v1"])
	second_id = storage.xadd("mystream", "*", [b"field", b"v2"])

	assert first_id == "1710000000123-0"
	assert second_id == "1710000000123-1"


def test_xadd_rejects_non_stream_key_type():
	storage = CacheStorage()
	storage.set("mystream", "plain-string")

	with pytest.raises(TypeError, match="dictionary value"):
		storage.xadd("mystream", "1-1", [b"field", b"value"])


# ---------- RDB save / load tests ----------

def test_save_creates_rdb_file():
	storage = CacheStorage()
	storage.set(b"key1", b"value1")
	storage.set(b"key2", b"value2")

	with tempfile.TemporaryDirectory() as tmpdir:
		result = storage.save(tmpdir, "test.rdb")

		assert result is True
		assert os.path.exists(os.path.join(tmpdir, "test.rdb"))


def test_save_and_load_roundtrip():
	storage = CacheStorage()
	storage.set(b"hello", b"world")
	storage.set(b"foo", b"bar")

	with tempfile.TemporaryDirectory() as tmpdir:
		storage.save(tmpdir, "dump.rdb")

		storage2 = CacheStorage()
		result = storage2.load(tmpdir, "dump.rdb")

		assert result is True
		assert storage2.get(b"hello") == b"world"
		assert storage2.get(b"foo") == b"bar"


@patch("app.storage.monotonic")
@patch("app.storage._time_module")
def test_save_and_load_with_expiry(mock_time_module, mock_monotonic):
	mock_monotonic.return_value = 1000.0
	mock_time_module.time.return_value = 2000.0

	storage = CacheStorage()
	storage.set(b"persistent", b"stays")
	storage.set(b"expiring", b"fades", ttl=60.0)

	with tempfile.TemporaryDirectory() as tmpdir:
		storage.save(tmpdir, "dump.rdb")

		# Load when key is still valid (40s have passed)
		mock_monotonic.return_value = 1040.0
		mock_time_module.time.return_value = 2040.0

		storage2 = CacheStorage()
		storage2.load(tmpdir, "dump.rdb")

		assert storage2.get(b"persistent") == b"stays"
		assert storage2.get(b"expiring") == b"fades"


@patch("app.storage.monotonic")
@patch("app.storage._time_module")
def test_load_skips_already_expired_keys(mock_time_module, mock_monotonic):
	mock_monotonic.return_value = 1000.0
	mock_time_module.time.return_value = 2000.0

	storage = CacheStorage()
	storage.set(b"short", b"lived", ttl=10.0)

	with tempfile.TemporaryDirectory() as tmpdir:
		storage.save(tmpdir, "dump.rdb")

		# Load 20s later — key has expired
		mock_monotonic.return_value = 1020.0
		mock_time_module.time.return_value = 2020.0

		storage2 = CacheStorage()
		storage2.load(tmpdir, "dump.rdb")

		assert storage2.get(b"short") is None


def test_load_returns_false_when_file_missing():
	storage = CacheStorage()

	with tempfile.TemporaryDirectory() as tmpdir:
		result = storage.load(tmpdir, "nonexistent.rdb")

	assert result is False


def test_save_does_not_include_expired_keys():
	storage = CacheStorage()
	storage.set(b"alive", b"yes")

	# Manually insert an already-expired record
	from time import monotonic
	storage._storage[b"dead"] = (b"no", monotonic() - 1)

	with tempfile.TemporaryDirectory() as tmpdir:
		storage.save(tmpdir, "dump.rdb")

		storage2 = CacheStorage()
		storage2.load(tmpdir, "dump.rdb")

		assert storage2.get(b"alive") == b"yes"
		assert storage2.get(b"dead") is None


def test_save_overwrites_existing_file():
	storage = CacheStorage()
	storage.set(b"k", b"v1")

	with tempfile.TemporaryDirectory() as tmpdir:
		storage.save(tmpdir, "dump.rdb")
		size1 = os.path.getsize(os.path.join(tmpdir, "dump.rdb"))

		storage.set(b"k2", b"v2")
		storage.save(tmpdir, "dump.rdb")
		size2 = os.path.getsize(os.path.join(tmpdir, "dump.rdb"))

		assert size2 > size1


# ---------- KEYS tests ----------

def test_keys_star_returns_all_keys():
	storage = CacheStorage()
	storage.set(b"hello", b"world")
	storage.set(b"foo", b"bar")
	storage.set(b"foobar", b"baz")

	result = set(storage.keys(b"*"))

	assert result == {b"hello", b"foo", b"foobar"}


def test_keys_prefix_glob():
	storage = CacheStorage()
	storage.set(b"foo", b"1")
	storage.set(b"foobar", b"2")
	storage.set(b"bar", b"3")

	result = set(storage.keys(b"foo*"))

	assert result == {b"foo", b"foobar"}


def test_keys_question_mark_glob():
	storage = CacheStorage()
	storage.set(b"hallo", b"1")
	storage.set(b"hxllo", b"2")
	storage.set(b"hello", b"3")
	storage.set(b"hllo", b"4")

	result = set(storage.keys(b"h?llo"))

	assert result == {b"hallo", b"hxllo", b"hello"}


def test_keys_character_class_glob():
	storage = CacheStorage()
	storage.set(b"hallo", b"1")
	storage.set(b"hello", b"2")
	storage.set(b"hillo", b"3")

	result = set(storage.keys(b"h[ae]llo"))

	assert result == {b"hallo", b"hello"}


def test_keys_excludes_expired():
	from time import monotonic
	storage = CacheStorage()
	storage.set(b"alive", b"yes")
	storage._storage[b"dead"] = (b"no", monotonic() - 1)

	result = storage.keys(b"*")

	assert b"alive" in result
	assert b"dead" not in result


def test_keys_empty_storage():
	storage = CacheStorage()

	assert storage.keys(b"*") == []


def test_keys_no_match_returns_empty():
	storage = CacheStorage()
	storage.set(b"hello", b"world")

	assert storage.keys(b"xyz*") == []


# ----------------- SortedSet Tests ----------------------------

def test_zadd_new_members():
	storage = CacheStorage()

	results = [storage.zadd(b"zset", i, f"member{i}".encode()) for i in range(10)]
	assert isinstance(storage.get(b"zset"), dict)
	assert all(res == False for res in results)
	assert storage.get(b"zset") == {f"member{i}".encode(): float(i) for i in range(10)}


def test_zadd_existed_members():
	storage = CacheStorage()
	res1 = storage.zadd(b"zset", 1.0, b"member1")
	res2 = storage.zadd(b"zset", 2.0, b"member1")
	assert isinstance(storage.get(b"zset"), dict)
	assert storage.get(b"zset") == {b"member1": 2.0}
	assert res1 == False and res2 == True


def test_zrank():
	storage = CacheStorage()

	res1 = storage.zadd(b"zset_key" , 100.0,  b"foo") 
	res2 = storage.zadd(b"zset_key" , 100.0,  b"bar") 
	res3 = storage.zadd(b"zset_key" , 20.0,  b"baz") 
	res4 = storage.zadd(b"zset_key" , 30.1,  b"caz") 
	res5 = storage.zadd(b"zset_key" , 40.2,  b"paz")
	assert all(res == False for res in [res1, res2, res3, res4, res5])
	assert storage.get(b"zset_key") == {b"foo": 100.0, b"bar": 100.0, b"baz": 20.0, b"caz": 30.1, b"paz": 40.2}
	ranks = [storage.zrank(b"zset_key", member) for member in [b"baz", b"caz", b"paz", b"bar", b"foo"]]
	assert ranks == list(range(5))


def test_zrank_no_existing_member():
	storage = CacheStorage()
	res1 = storage.zadd(b"zset_key" , 100.0,  b"foo") 
	assert storage.zrank(b"zset_key", b"nonexistent") is None


def test_zrank_no_existing_key():
	storage = CacheStorage()
	res1 = storage.zadd(b"zset_key" , 100.0,  b"foo") 
	assert storage.zrank(b"noexistingkey", b"foo") is None


def test_zrange():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zrange(b"zset_key", 0, 2)
    assert result == [b"baz", b"caz", b"paz"]
    
    
def test_zrange_unexisted_set():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zrange(b"nonexistent", 0, 2)
    assert result == []
    
    
def test_zrange_out_of_bounds():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zrange(b"zset_key", 10, 12)
    assert result == []
    
    
def test_zrange_right_border_out_of_bounds():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zrange(b"zset_key", 3, 12)
    assert result == [b"bar", b"foo"]
    
    
def test_zrange_left_bound_greater_than_right():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zrange(b"zset_key", 5, 2)
    assert result == []
    
def test_zcard():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")
    storage.zadd(b"zset_key", 30.1, b"caz")
    storage.zadd(b"zset_key", 40.2, b"paz")

    result = storage.zcard(b"zset_key")
    assert result == 5
    
    
def test_zcard_empty_set():
    storage = CacheStorage()
    result = storage.zcard(b"zset_key")
    assert result == 0
    

def test_zscore():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")

    result = storage.zscore(b"zset_key", b"foo")
    assert result == 100.0
    result = storage.zscore(b"zset_key", b"baz")
    assert result == 20.0

    result = storage.zscore(b"zset_key", b"nonexistent")
    assert result is None
    
def test_zrem():
    storage = CacheStorage()
    storage.zadd(b"zset_key", 100.0, b"foo")
    storage.zadd(b"zset_key", 100.0, b"bar")
    storage.zadd(b"zset_key", 20.0, b"baz")

    result = storage.zrem(b"zset_key", [b"foo"])
    assert result == 1
    assert storage.get(b"zset_key") == {b"bar": 100.0, b"baz": 20.0}

    result = storage.zrem(b"zset_key", [b"nonexistent"])
    assert result == 0
    assert storage.get(b"zset_key") == {b"bar": 100.0, b"baz": 20.0}
    
    result = storage.zrem(b"zset_key", [b"bar", b"nonexistent"])
    assert result == 1
    assert storage.get(b"zset_key") == {b"baz": 20.0}
    
    result = storage.zadd(b"zset_key", 30.0, b"qux")
    assert storage.get(b"zset_key") == {b"baz": 20.0, b"qux": 30.0}
    
    result = storage.zrem(b"zset_key", [b"qux", b"baz"])
    assert result == 2
    assert storage.get(b"zset_key") == {}
