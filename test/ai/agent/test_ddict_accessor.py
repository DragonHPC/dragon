"""Tests for DDictAccessor — error-handled DDict wrapper."""

import dragon
import multiprocessing as mp

from unittest import TestCase, main

from dragon.ai.agent.ddict.accessor import DDictAccessor


class TestDDictAccessorGet(TestCase):
    """Verify DDictAccessor.get() and get_or_default() behavior."""

    def test_get_existing_key(self):
        """Existing key returns its value."""
        ddict = {"key1": "value1"}
        acc = DDictAccessor(ddict, agent_id="a", task_id="t")
        self.assertEqual(acc.get("key1"), "value1")

    def test_get_missing_key_raises_key_error(self):
        """Missing key raises KeyError."""
        acc = DDictAccessor({}, agent_id="a", task_id="t")
        with self.assertRaises(KeyError):
            acc.get("missing")

    def test_get_or_default_existing(self):
        """Existing key returns value, not default."""
        acc = DDictAccessor({"k": 42})
        self.assertEqual(acc.get_or_default("k", default=-1), 42)

    def test_get_or_default_missing(self):
        """Missing key returns the specified default."""
        acc = DDictAccessor({})
        self.assertEqual(acc.get_or_default("k", default=-1), -1)


class TestDDictAccessorPut(TestCase):
    """Verify DDictAccessor.put() writes and overwrites."""

    def test_put_writes_value(self):
        """put() stores the value in the underlying dict."""
        ddict = {}
        acc = DDictAccessor(ddict)
        acc.put("key1", "value1")
        self.assertEqual(ddict["key1"], "value1")

    def test_put_overwrites(self):
        """put() overwrites an existing key."""
        ddict = {"key1": "old"}
        acc = DDictAccessor(ddict)
        acc.put("key1", "new")
        self.assertEqual(ddict["key1"], "new")


class TestDDictAccessorDelete(TestCase):
    """Verify DDictAccessor.delete() removes keys safely."""

    def test_delete_existing_key(self):
        """Existing key is removed from the dict."""
        ddict = {"key1": "value1"}
        acc = DDictAccessor(ddict)
        acc.delete("key1")
        self.assertNotIn("key1", ddict)

    def test_delete_missing_key_no_error(self):
        """Deleting a non-existent key is a silent no-op."""
        acc = DDictAccessor({})
        acc.delete("missing")  # should not raise


class TestDDictAccessorGetList(TestCase):
    """Verify DDictAccessor.get_list() returns a defensive copy."""

    def test_get_list_returns_copy(self):
        """Returned list is a copy; mutating it does not affect the original."""
        original = [1, 2, 3]
        ddict = {"items": original}
        acc = DDictAccessor(ddict)
        result = acc.get_list("items")
        self.assertEqual(result, [1, 2, 3])
        result.append(4)
        self.assertEqual(len(original), 3)  # original unchanged


class TestDDictAccessorWriteEvent(TestCase):
    """Verify DDictAccessor.write_event() writes event data and increments count."""

    def test_write_event(self):
        """Event data stored at formatted key; count set to index+1."""
        ddict = {}
        acc = DDictAccessor(ddict)
        acc.write_event(
            key_template="EVT:{task_id}:{agent_id}:{dispatch_id}:{index}",
            count_template="EVT_COUNT:{task_id}:{agent_id}:{dispatch_id}",
            event_data={"iteration": 1, "output": "hello"},
            index=0,
            task_id="t1", agent_id="a1", dispatch_id="d1",
        )
        self.assertEqual(ddict["EVT:t1:a1:d1:0"], {"iteration": 1, "output": "hello"})
        self.assertEqual(ddict["EVT_COUNT:t1:a1:d1"], 1)

    def test_write_event_increments_count(self):
        """Sequential writes increment the count key."""
        ddict = {}
        acc = DDictAccessor(ddict)
        fmt = dict(task_id="t", agent_id="a", dispatch_id="d")
        key_t = "E:{task_id}:{agent_id}:{dispatch_id}:{index}"
        cnt_t = "C:{task_id}:{agent_id}:{dispatch_id}"

        acc.write_event(key_t, cnt_t, "evt0", 0, **fmt)
        acc.write_event(key_t, cnt_t, "evt1", 1, **fmt)
        self.assertEqual(ddict["C:t:a:d"], 2)


class TestDDictAccessorRaw(TestCase):
    """Verify DDictAccessor.raw property returns the underlying object."""

    def test_raw_returns_underlying_ddict(self):
        """raw property returns the exact object passed to constructor."""
        ddict = {"key": "value"}
        acc = DDictAccessor(ddict)
        self.assertIs(acc.raw, ddict)

    def test_raw_allows_direct_mutation(self):
        """Mutations via raw are reflected in accessor.get()."""
        ddict = {}
        acc = DDictAccessor(ddict)
        acc.raw["direct_key"] = "direct_val"
        self.assertEqual(acc.get("direct_key"), "direct_val")


class TestDDictAccessorErrorPaths(TestCase):
    """Verify DDictAccessor error handling for non-KeyError failures."""

    def test_get_non_key_error_reraises(self):
        """Non-KeyError exceptions from DDict reads are logged and re-raised."""

        class BrokenDict:
            def __getitem__(self, key):
                raise RuntimeError("storage failure")

        acc = DDictAccessor(BrokenDict(), agent_id="a", task_id="t1")
        with self.assertRaises(RuntimeError):
            acc.get("any_key")

    def test_put_failure_reraises(self):
        """DDict write failures are logged and re-raised."""

        class ReadOnlyDict:
            def __setitem__(self, key, value):
                raise RuntimeError("read only")

        acc = DDictAccessor(ReadOnlyDict(), agent_id="a", task_id="t1")
        with self.assertRaises(RuntimeError):
            acc.put("key", "val")

    def test_delete_non_key_error_reraises(self):
        """Non-KeyError exceptions from DDict deletes are logged and re-raised."""

        class BrokenDeleteDict(dict):
            def __delitem__(self, key):
                raise RuntimeError("delete failure")

        acc = DDictAccessor(BrokenDeleteDict({"key": "val"}), agent_id="a", task_id="t1")
        with self.assertRaises(RuntimeError):
            acc.delete("key")

    def test_get_or_default_non_key_error_reraises(self):
        """get_or_default only swallows KeyError, not other exceptions."""

        class BrokenDict:
            def __getitem__(self, key):
                raise RuntimeError("storage failure")

        acc = DDictAccessor(BrokenDict(), agent_id="a", task_id="t1")
        with self.assertRaises(RuntimeError):
            acc.get_or_default("key", default="fallback")

    def test_empty_task_id_in_log(self):
        """Accessor with empty task_id does not crash on error path."""

        class BrokenDict:
            def __getitem__(self, key):
                raise RuntimeError("fail")

        acc = DDictAccessor(BrokenDict(), agent_id="a", task_id="")
        with self.assertRaises(RuntimeError):
            acc.get("key")


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
