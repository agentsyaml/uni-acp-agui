#!/usr/bin/env python3
"""Small regression checks for release artifact rejection paths."""

import hashlib
import os
import tempfile
import unittest
import warnings
import zipfile
from pathlib import Path

import release


class ReleaseChecks(unittest.TestCase):
    def test_inventory_rejects_missing_assets(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / next(iter(release.expected("0.1.0")))).touch()
            with self.assertRaises(ValueError):
                release.inventory(directory, "0.1.0")

    def test_checksum_detects_tampering(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            name = next(iter(release.expected("0.1.0")))
            for asset in release.expected("0.1.0"):
                data = b"original"
                (directory / asset).write_bytes(data)
                (directory / (asset + ".sha256")).write_text(
                    f"{hashlib.sha256(data).hexdigest()}  {asset}\n")
            (directory / name).write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "checksum"):
                release.check(directory, "0.1.0")

    def test_asset_list_contains_all_archives_and_checksums(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            for name in release.expected("0.1.0"):
                (directory / name).touch()
                (directory / (name + ".sha256")).touch()
            assets = release.asset_paths(directory, "0.1.0")
            self.assertEqual(len(assets), 12)
            self.assertEqual(sum(path.suffix == ".sha256" for path in assets), 6)

    def test_package_preserves_unix_executable_mode_and_checksum(self):
        if os.name == "nt":
            self.skipTest("Unix executable-bit packaging")
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            binary = directory / "agui-acp-bridge"
            binary.write_text(
                "#!/bin/sh\ncase \"$1\" in\n"
                "  --version) printf 'agui-acp-bridge 0.1.0\\n' ;;\n"
                "  --help) exit 0 ;;\nesac\n"
            )
            binary.chmod(0o755)
            release.package(binary, "x86_64-unknown-linux-gnu", "0.1.0", directory / "out")
            archive = directory / "out" / "agui-acp-bridge-0.1.0-x86_64-unknown-linux-gnu.tar.gz"
            _, modes = release.members(archive)
            self.assertTrue(modes["agui-acp-bridge"] & 0o111)
            digest, recorded_name = (directory / "out" / (archive.name + ".sha256")).read_text().split()
            self.assertEqual(digest, hashlib.sha256(archive.read_bytes()).hexdigest())
            self.assertEqual(recorded_name, archive.name)

    def test_package_rejects_unsafe_version_before_paths(self):
        with self.assertRaisesRegex(ValueError, "version"):
            release.package("../binary", "x86_64-unknown-linux-gnu", "01.0.0", ".")

    def test_archive_rejects_duplicate_members(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "bad.zip"
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(path, "w") as archive:
                    archive.writestr("agui-acp-bridge.exe", b"one")
                    archive.writestr("agui-acp-bridge.exe", b"two")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                release.members(path)


if __name__ == "__main__":
    unittest.main()
