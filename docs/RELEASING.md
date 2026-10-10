# Releasing

Releases are created only by pushing an authorized `vMAJOR.MINOR.PATCH` tag
whose version exactly matches the workspace version in `Cargo.toml`. For the
current workspace version, the future tag would be `v0.1.0`; no tag or release
has been created by this setup. A tag run first calls the complete CI workflow,
including Rust feature-matrix tests, formatting, Clippy, frontend checks,
audits, and native builds. Publishing runs only after every check succeeds.

The release contains only the CLI binary packages for six native targets.
Each archive includes the executable, this repository's README, and a clearly
labeled `LICENSE-NOTICE` containing only the license metadata declared in
`Cargo.toml`; it is not a license text and asserts no copyright. Canonical
MIT/Apache license texts must be settled and included before an actual release.
Each archive has a SHA-256 sidecar. The binaries are unsigned; users should
verify the published checksums. Linux GNU binaries are built on Ubuntu 24.04
and therefore require a compatible glibc; no broader Linux portability is
promised. Fixture and example binaries are not packaged.

Windows ARM64 requires the hosted `windows-11-arm` runner and a native ARM64
Rust toolchain; an unavailable runner or mismatched host blocks publication.

To prepare a release, update the workspace version, merge and validate the
change, then have an authorized maintainer create and push the matching tag.
Never reuse a tag. Check the resulting GitHub Release and its six archives and
six checksums before announcing it.
