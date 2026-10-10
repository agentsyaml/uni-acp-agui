#!/usr/bin/env python3
"""Create and validate the small, platform-specific CLI release artifacts."""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from pathlib import Path

TARGETS = {
    "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin", "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc", "aarch64-pc-windows-msvc",
}


def expected(version):
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version):
        raise ValueError(f"invalid release version: {version}")
    return {f"agui-acp-bridge-{version}-{t}.{ 'zip' if 'windows' in t else 'tar.gz'}"
            for t in TARGETS}


def inventory(directory, version):
    names = [p.name for p in Path(directory).iterdir() if p.is_file()]
    archives = [n for n in names if n.endswith((".zip", ".tar.gz"))]
    wanted = expected(version)
    if len(archives) != len(wanted) or set(archives) != wanted:
        raise ValueError(f"expected exactly {sorted(wanted)}, got {sorted(archives)}")
    checksums = {n for n in names if n.endswith(".sha256")}
    if len(checksums) != len(wanted) or checksums != {n + ".sha256" for n in wanted}:
        raise ValueError("missing, duplicate, or unexpected checksum files")
    if set(names) != wanted | checksums:
        raise ValueError("unexpected files in artifact directory")
    return wanted


def asset_paths(directory, version):
    archives = inventory(directory, version)
    names = sorted(archives | {name + ".sha256" for name in archives})
    return [Path(directory) / name for name in names]


def members(path):
    if path.name.endswith(".zip"):
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) != len(set(names)):
                raise ValueError(f"duplicate archive member: {path.name}")
            files = {name: archive.read(name) for name in names}
            modes = {name: archive.getinfo(name).external_attr >> 16 for name in names}
            return files, modes
    with tarfile.open(path, "r:gz") as archive:
        items = archive.getmembers()
        names = [item.name for item in items]
        if len(names) != len(set(names)) or any(not item.isfile() for item in items):
            raise ValueError(f"invalid archive members: {path.name}")
        result = {}
        for item in items:
            stream = archive.extractfile(item)
            if stream is None:
                raise ValueError(f"unreadable archive member: {item.name}")
            result[item.name] = stream.read()
        return result, {item.name: item.mode for item in items}


def check(directory, version):
    for name in inventory(directory, version):
        path = Path(directory) / name
        checksum = (Path(directory) / (name + ".sha256")).read_text().split()
        if len(checksum) != 2 or checksum[1] != name or checksum[0] != hashlib.sha256(path.read_bytes()).hexdigest():
            raise ValueError(f"checksum mismatch: {name}")
        files, modes = members(path)
        binary = "agui-acp-bridge.exe" if name.endswith(".zip") else "agui-acp-bridge"
        if set(files) != {binary, "README.md", "LICENSE-NOTICE"}:
            raise ValueError(f"unexpected package contents: {name}: {sorted(files)}")
        if name.endswith(".tar.gz") and not modes[binary] & 0o111:
            raise ValueError(f"archive binary is not executable: {name}")


def package(binary, target, version, output):
    expected(version)
    if target not in TARGETS:
        raise ValueError(f"unsupported target: {target}")
    binary = Path(binary)
    expected_version = f"agui-acp-bridge {version}"
    if binary.name != ("agui-acp-bridge.exe" if "windows" in target else "agui-acp-bridge"):
        raise ValueError(f"binary name does not match target: {binary.name}")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    filename = f"agui-acp-bridge-{version}-{target}.{ 'zip' if 'windows' in target else 'tar.gz'}"
    license_text = (
        "License metadata: MIT OR Apache-2.0 (from Cargo.toml).\n"
        "This notice is not the license text. Consult the repository for terms: "
        "https://github.com/agentsyaml/uni-acp-agui\n"
    )
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        (root / "LICENSE-NOTICE").write_text(license_text)
        (root / "README.md").write_bytes(Path("README.md").read_bytes())
        staged_binary = root / binary.name
        shutil.copy2(binary, staged_binary)
        if "windows" not in target:
            staged_binary.chmod(binary.stat().st_mode | 0o111)
        destination = output / filename
        if filename.endswith(".zip"):
            with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
                for item in root.iterdir():
                    archive.write(item, item.name)
        else:
            with tarfile.open(destination, "w:gz") as archive:
                for item in root.iterdir():
                    archive.add(item, arcname=item.name)
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    (output / (filename + ".sha256")).write_text(f"{digest}  {filename}\n")
    with tempfile.TemporaryDirectory() as temp:
        content, modes = members(destination)
        extracted = Path(temp) / binary.name
        extracted.write_bytes(content[binary.name])
        if os.name != "nt":
            extracted.chmod(modes[binary.name])
        for flag in ("--version", "--help"):
            result = subprocess.run([str(extracted), flag], check=True, text=True, capture_output=True)
            if flag == "--version" and result.stdout.strip() != expected_version:
                raise ValueError(f"binary version output was not exactly {expected_version!r}")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    pack = sub.add_parser("package")
    pack.add_argument("--binary", required=True)
    pack.add_argument("--target", required=True)
    pack.add_argument("--version", required=True)
    pack.add_argument("--output", required=True)
    verify = sub.add_parser("check")
    verify.add_argument("--directory", required=True)
    verify.add_argument("--version", required=True)
    assets = sub.add_parser("assets")
    assets.add_argument("--directory", required=True)
    assets.add_argument("--version", required=True)
    args = parser.parse_args()
    if args.command == "package":
        package(args.binary, args.target, args.version, args.output)
    elif args.command == "check":
        check(args.directory, args.version)
    else:
        for path in asset_paths(args.directory, args.version):
            print(path)


if __name__ == "__main__":
    main()
