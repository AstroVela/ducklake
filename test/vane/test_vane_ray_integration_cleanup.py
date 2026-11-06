"""Check that Ray integration teardown preserves the query failure."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import unittest
from pathlib import Path

path = Path(__file__).with_name("test_vane_wheel_ray_ducklake.py")
specification = importlib.util.spec_from_file_location("ray_integration", path)
assert specification is not None and specification.loader is not None
integration = importlib.util.module_from_spec(specification)
specification.loader.exec_module(integration)


class CleanupTests(unittest.TestCase):
    def test_query_failure_survives_multiple_cleanup_failures(self) -> None:
        calls = []

        def fail(name: str) -> None:
            calls.append(name)
            raise RuntimeError(name)

        primary = ValueError("query failed")
        output = io.StringIO()
        with contextlib.redirect_stderr(output), self.assertRaises(ValueError) as caught:
            try:
                raise primary
            finally:
                integration.cleanup_integration(
                    [("connection", lambda: fail("connection")), ("cluster", lambda: fail("cluster"))], primary
                )
        self.assertIs(caught.exception, primary)
        self.assertEqual(calls, ["connection", "cluster"])
        self.assertIn("cluster", output.getvalue())
        self.assertIn("connection", primary.__notes__[0])

    def test_cleanup_failure_fails_an_otherwise_successful_run(self) -> None:
        calls = []
        failure = OSError("close failed")

        def fail() -> None:
            raise failure

        with (
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaisesRegex(RuntimeError, "integration cleanup failed") as caught,
        ):
            integration.cleanup_integration([("close", fail), ("shutdown", lambda: calls.append("shutdown"))], None)
        self.assertIs(caught.exception.__cause__, failure)
        self.assertEqual(calls, ["shutdown"])

    def test_successful_cleanup_does_not_mask_a_query_failure(self) -> None:
        primary = ValueError("query failed")
        with self.assertRaises(ValueError) as caught:
            try:
                raise primary
            finally:
                integration.cleanup_integration([("close", lambda: None)], primary)
        self.assertIs(caught.exception, primary)


if __name__ == "__main__":
    unittest.main()
