"""Tests for completion-envelope constants shared between host and watcher."""

import dragon
import multiprocessing as mp

import cloudpickle
from unittest import TestCase, main


# ========================================================================
# StatusConstants
# ========================================================================

class TestStatusConstants(TestCase):
    """Verify status constant values and string comparison."""

    def test_status_done_value(self):
        """STATUS_DONE has the expected string value."""
        from dragon.ai.langgraph.constants import STATUS_DONE
        self.assertEqual(STATUS_DONE, "done")

    def test_status_error_value(self):
        """STATUS_ERROR has the expected string value."""
        from dragon.ai.langgraph.constants import STATUS_ERROR
        self.assertEqual(STATUS_ERROR, "error")

    def test_status_values_distinct(self):
        """STATUS_DONE and STATUS_ERROR are distinct values."""
        from dragon.ai.langgraph.constants import STATUS_DONE, STATUS_ERROR
        self.assertNotEqual(STATUS_DONE, STATUS_ERROR)


# ========================================================================
# EnvelopeFieldConstants
# ========================================================================

class TestEnvelopeFieldConstants(TestCase):
    """Verify envelope field name constants."""

    def test_env_status_value(self):
        """ENV_STATUS has the expected key name."""
        from dragon.ai.langgraph.constants import ENV_STATUS
        self.assertEqual(ENV_STATUS, "status")

    def test_env_payload_value(self):
        """ENV_PAYLOAD has the expected key name."""
        from dragon.ai.langgraph.constants import ENV_PAYLOAD
        self.assertEqual(ENV_PAYLOAD, "payload")

    def test_all_field_names_distinct(self):
        """All envelope field names are distinct."""
        from dragon.ai.langgraph.constants import ENV_STATUS, ENV_PAYLOAD
        fields = [ENV_STATUS, ENV_PAYLOAD]
        self.assertEqual(len(fields), len(set(fields)))


# ========================================================================
# EnvelopeFraming
# ========================================================================

class TestEnvelopeFraming(TestCase):
    """Verify the task_id framing that rides outside the pickled body."""

    def test_round_trip(self):
        """pack_envelope/unpack_envelope round-trip task_id and body intact."""
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        task_id, body = unpack_envelope(pack_envelope("task_123", b"body-bytes"))
        self.assertEqual(task_id, "task_123")
        self.assertEqual(body, b"body-bytes")

    def test_empty_body(self):
        """An empty body round-trips (the watcher's stop sentinel uses one)."""
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        task_id, body = unpack_envelope(pack_envelope("__stop__", b""))
        self.assertEqual(task_id, "__stop__")
        self.assertEqual(body, b"")

    def test_body_is_never_interpreted(self):
        """An undecodable body still yields its task_id — the whole point of
        framing, since it lets a decode failure be blamed on the right task."""
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        task_id, body = unpack_envelope(pack_envelope("task_9", b"not-a-pickle"))
        self.assertEqual(task_id, "task_9")
        self.assertEqual(body, b"not-a-pickle")

    def test_uuid_task_id(self):
        """A UUID task_id (what the watcher actually generates) round-trips."""
        import uuid
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        expected = str(uuid.uuid4())
        task_id, _ = unpack_envelope(pack_envelope(expected, b"x"))
        self.assertEqual(task_id, expected)

    def test_non_ascii_task_id(self):
        """A multi-byte task_id round-trips (length is bytes, not characters)."""
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        task_id, body = unpack_envelope(pack_envelope("tâche-\u4efb\u52d9", b"x"))
        self.assertEqual(task_id, "tâche-\u4efb\u52d9")
        self.assertEqual(body, b"x")

    def test_body_offset_is_length_prefixed(self):
        """A body that itself looks like a header does not shift the split."""
        from dragon.ai.langgraph.constants import pack_envelope, unpack_envelope
        tricky = b"\x00\x05abcde"
        task_id, body = unpack_envelope(pack_envelope("t", tricky))
        self.assertEqual(task_id, "t")
        self.assertEqual(body, tricky)

    def test_empty_message_rejected(self):
        """A message with no header at all raises ValueError."""
        from dragon.ai.langgraph.constants import unpack_envelope
        with self.assertRaises(ValueError):
            unpack_envelope(b"")

    def test_partial_header_rejected(self):
        """A message with a half-written header raises ValueError."""
        from dragon.ai.langgraph.constants import unpack_envelope
        with self.assertRaises(ValueError):
            unpack_envelope(b"\x00")

    def test_truncated_task_id_rejected(self):
        """A header promising more task_id bytes than exist raises ValueError."""
        from dragon.ai.langgraph.constants import unpack_envelope
        with self.assertRaises(ValueError):
            unpack_envelope(b"\x00\x40abc")


# ========================================================================
# EnvelopePickler
# ========================================================================

class _HostileError(Exception):
    """A pickling failure whose own ``str()`` raises, forcing dump's last resort."""

    def __str__(self):
        raise RuntimeError("str() is broken too")


class _FakeFile:
    """Stands in for the Queue's pickle adapter: records writes, replays reads."""

    def __init__(self, data: bytes = b"") -> None:
        self.data = data
        self.writes: list[bytes] = []

    def write(self, chunk: bytes) -> None:
        self.writes.append(chunk)

    def read(self, size: int = -1) -> bytes:
        return self.data


class TestEnvelopePickler(TestCase):
    """Verify the codec the completion Queue is constructed with."""

    def _round_trip(self, task_id: str, obj) -> tuple:
        from dragon.ai.langgraph.constants import EnvelopePickler
        pickler = EnvelopePickler()
        out = _FakeFile()
        pickler.dump((task_id, obj), out)
        return pickler.load(_FakeFile(b"".join(out.writes)))

    def test_round_trip(self):
        """dump/load round-trip the task_id and the decoded body."""
        from dragon.ai.langgraph.constants import (
            ENV_STATUS, ENV_PAYLOAD, STATUS_DONE,
        )
        payload = {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: {"messages": ["hi"]}}
        task_id, envelope, exc = self._round_trip("task_123", payload)

        self.assertEqual(task_id, "task_123")
        self.assertEqual(envelope, payload)
        self.assertIsNone(exc)

    def test_dump_issues_exactly_one_write(self):
        """The buffered FLI mallocs and copies once per write(), so a multi-write
        dump would multiply allocations on every completion."""
        from dragon.ai.langgraph.constants import EnvelopePickler
        out = _FakeFile()
        EnvelopePickler().dump(("t", {"a": 1}), out)
        self.assertEqual(len(out.writes), 1)

    def test_unpicklable_body_becomes_an_error_envelope(self):
        """dump must not raise: Queue.put closes its send handle either way, and
        a handle closed with nothing buffered emits an unattributable message
        that would leave the task hanging until its own timeout."""
        from dragon.ai.langgraph.constants import (
            ENV_STATUS, ENV_PAYLOAD, STATUS_DONE, STATUS_ERROR,
        )

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("nope, cannot pickle this")

        with self.assertLogs("LANGGRAPH.envelope", level="ERROR") as logs:
            task_id, envelope, exc = self._round_trip(
                "task_bad",
                {ENV_STATUS: STATUS_DONE, ENV_PAYLOAD: _Unpicklable()},
            )

        self.assertEqual(task_id, "task_bad")
        self.assertIsNone(exc)
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], RuntimeError)
        self.assertIn("nope, cannot pickle this", str(envelope[ENV_PAYLOAD]))
        self.assertIn("task_bad", "\n".join(logs.output))

    def test_substituted_envelope_is_still_a_single_write(self):
        """The fallback path shares the one write(), so it costs the FLI no more
        allocations than a normal completion."""
        from dragon.ai.langgraph.constants import EnvelopePickler

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("nope")

        out = _FakeFile()
        with self.assertLogs("LANGGRAPH.envelope", level="ERROR"):
            EnvelopePickler().dump(("t", _Unpicklable()), out)
        self.assertEqual(len(out.writes), 1)

    def test_body_whose_str_also_fails_still_reaches_the_wire(self):
        """Even a pathological payload cannot make dump raise, which would put a
        zero-byte, unattributable message on the queue."""
        from dragon.ai.langgraph.constants import (
            ENV_STATUS, ENV_PAYLOAD, STATUS_ERROR,
        )

        class _Hostile:
            def __reduce__(self):
                raise _HostileError()

        with self.assertLogs("LANGGRAPH.envelope", level="ERROR"):
            task_id, envelope, exc = self._round_trip("task_hostile", _Hostile())

        self.assertEqual(task_id, "task_hostile")
        self.assertIsNone(exc)
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], RuntimeError)

    def test_load_returns_decode_error_instead_of_raising(self):
        """An undecodable body must still surface its task_id, so the watcher can
        fail that Future rather than leaking its slot forever."""
        from dragon.ai.langgraph.constants import EnvelopePickler, pack_envelope
        task_id, envelope, exc = EnvelopePickler().load(
            _FakeFile(pack_envelope("task_9", b"not-a-pickle"))
        )

        self.assertEqual(task_id, "task_9")
        self.assertIsNone(envelope)
        self.assertIsInstance(exc, Exception)

    def test_unframable_message_raises(self):
        """A message with no readable header has no task to blame, so it
        propagates rather than being silently attributed."""
        from dragon.ai.langgraph.constants import EnvelopePickler
        with self.assertRaises(ValueError):
            EnvelopePickler().load(_FakeFile(b"\x00"))

    def test_stop_sentinel_shape(self):
        """The watcher's stop sentinel survives dump/load with an empty body."""
        task_id, envelope, exc = self._round_trip("__dragon_watcher_stop__", None)

        self.assertEqual(task_id, "__dragon_watcher_stop__")
        self.assertIsNone(envelope)
        self.assertIsNone(exc)

    def test_pickler_is_serializable(self):
        """Queue.__getstate__ cloudpickles its pickler, so the codec must
        survive being shipped to a remote host alongside the descriptor."""
        from dragon.ai.langgraph.constants import EnvelopePickler
        restored = cloudpickle.loads(cloudpickle.dumps(EnvelopePickler()))
        self.assertIsInstance(restored, EnvelopePickler)


# ========================================================================
# EnvelopeConstruction
# ========================================================================

class TestEnvelopeConstruction(TestCase):
    """Verify envelope dict construction using constants."""

    def test_done_envelope_structure(self):
        """A success envelope contains all required fields."""
        from dragon.ai.langgraph.constants import (
            ENV_STATUS, ENV_PAYLOAD, STATUS_DONE,
        )
        envelope = {
            ENV_STATUS: STATUS_DONE,
            ENV_PAYLOAD: {"result": "success"},
        }
        self.assertIn(ENV_STATUS, envelope)
        self.assertIn(ENV_PAYLOAD, envelope)
        self.assertEqual(envelope[ENV_STATUS], STATUS_DONE)

    def test_error_envelope_structure(self):
        """An error envelope contains all required fields."""
        from dragon.ai.langgraph.constants import (
            ENV_STATUS, ENV_PAYLOAD, STATUS_ERROR,
        )
        envelope = {
            ENV_STATUS: STATUS_ERROR,
            ENV_PAYLOAD: RuntimeError("test failure"),
        }
        self.assertIn(ENV_STATUS, envelope)
        self.assertIn(ENV_PAYLOAD, envelope)
        self.assertEqual(envelope[ENV_STATUS], STATUS_ERROR)
        self.assertIsInstance(envelope[ENV_PAYLOAD], Exception)


if __name__ == "__main__":
    mp.set_start_method("dragon")
    main()
