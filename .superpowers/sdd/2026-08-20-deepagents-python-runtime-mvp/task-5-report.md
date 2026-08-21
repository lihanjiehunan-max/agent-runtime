# Task 5 Report: Verified MinIO Package Cache and Digest Singleflight

## Status

Task 5 is complete. The implementation adds a narrow MinIO object download boundary, a
tar-package source, verified atomic local package population, warm-cache outage behavior,
authoritative digest tamper rejection, incomplete-marker handling, and a digest-keyed
singleflight definition cache with bounded LRU size and idle expiry.

The implementation does not cache Session state, messages, principals, or mutable Agent
instances. Disk entries contain package bytes only after Task 4 validation, and the in-memory
cache accepts only immutable `FrozenContract` definitions. No live MinIO service is required by
the deterministic tests; the live integration is explicitly skipped unless configured.

## Commit

- `feat: add verified package cache and cold-load singleflight` (this Task 5 commit)
- No push was performed.

## Files changed

- `packages/object_store/client.py` (created)
- `packages/package_loader/minio_source.py` (created)
- `packages/package_loader/cache.py` (created)
- `tests/integration/test_minio_package_source.py` (created)
- `tests/concurrency/test_package_singleflight.py` (created)
- `.superpowers/sdd/2026-08-20-deepagents-python-runtime-mvp/task-5-report.md` (created)

No other parallel-lane file was modified or staged.

## Implementation summary

- Added `ObjectStoreClient`, a minimal structural boundary with one `download_object` operation,
  and `MinioObjectStoreClient`, which adapts `minio.Minio.fget_object` without coupling tests to a
  live service.
- Added `PackageSource` and `MinioPackageSource`. The default immutable package object key is
  `<tenant>/<agent>/<version>/package.tar.gz`; callers can supply an explicit object-key factory.
- MinIO/network/archive failures surface as `PackageSourceError` with stable code
  `DOWNLOAD_FAILED`. The downloaded tar archive is temporary and is never placed inside the
  package directory passed to Task 4 validation.
- Archive extraction rejects absolute/traversing paths through Task 4 path normalization,
  duplicate paths, links and non-file/non-directory members, oversized files, and excessive file
  counts. Extraction does not use unsafe bulk `extractall` behavior.
- Added `PackageCache.load(AgentPackageRef)`. Cache paths are exactly
  `<cache_root>/<digest>`, with package bytes under `package/` and a `.verified` marker outside the
  validator root so the marker cannot weaken Task 4's exact checksum coverage.
- Cold population downloads into a unique `.package-*` directory, runs the authoritative Task 4
  validator exactly once, atomically renames the staging directory to the digest path, and writes
  the digest-bearing success marker last. Failed downloads or validation remove staging data.
- Missing, malformed, wrong, or unreadable markers are treated as incomplete. Incomplete entries
  are never served and are replaced only through a new download and validation.
- Complete warm entries are revalidated against the caller's authoritative `AgentPackageRef`
  before return. Consequently a verified disk cache remains usable during a MinIO outage while
  changed or locally corrupted bytes are not trusted.
- Added `definition_cache_key(digest)` returning exactly `agent-definition:{digest}`.
- Added thread-safe `SingleflightLoader`: concurrent misses for one digest share one package load,
  one validation, and one definition build. Waiters receive the same immutable definition object;
  failures are published to all waiters and do not poison future retries.
- The in-memory definition cache has configurable positive maximum entries and idle TTL, updates
  recency on access, evicts least-recently-used entries at capacity, and rejects a builder result
  that is not an immutable `FrozenContract`.

## TDD evidence

### Initial RED: scoped modules absent

The two required test files were written before Task 5 production code.

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q
```

Result: exit 2 for the intended missing-feature reason.

```text
ModuleNotFoundError: No module named 'packages.object_store.client'
ModuleNotFoundError: No module named 'packages.package_loader.cache'
2 errors in 0.33s
```

### Edge-case RED: malformed marker and mutable definition

After the first green implementation, two additional tests were added before their fixes.

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py::test_cache_ignores_malformed_success_marker tests/concurrency/test_package_singleflight.py::test_singleflight_rejects_mutable_definition -q
```

Result: exit 1 for the intended reasons.

```text
FAILED test_cache_ignores_malformed_success_marker - UnicodeDecodeError
FAILED test_singleflight_rejects_mutable_definition - Failed: DID NOT RAISE TypeError
2 failed in 0.21s
```

The marker check now treats decode errors as incomplete, and the loader performs a runtime
immutable-contract check before publishing a definition. The same two tests then passed:

```text
2 passed in 0.15s
```

## Verification

### Focused Task 5 tests

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q
```

Result: exit 0.

```text
9 passed, 1 skipped in 0.38s
```

The skip is explicit: `RUNTIME_TEST_MINIO_ENDPOINT` is not configured. The deterministic tests
exercise the real package source/cache/singleflight code through a fake object-client boundary.

### Full Python tests

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest -q -rs
```

Result: exit 0.

```text
87 passed, 3 skipped, 1 warning in 3.22s
```

The other two skips are the pre-existing PostgreSQL integration tests requiring
`RUNTIME_TEST_DATABASE_URL`. The warning is the pre-existing FastAPI/Starlette TestClient
deprecation warning about `httpx2`.

### Scoped Ruff

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run ruff check packages/object_store/client.py packages/package_loader/minio_source.py packages/package_loader/cache.py tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py
```

Result: exit 0: `All checks passed!`

### Scoped Pyright

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pyright packages/object_store/client.py packages/package_loader/minio_source.py packages/package_loader/cache.py tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py
```

Result: exit 0: `0 errors, 0 warnings, 0 informations`.

### Lockfile consistency

Command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv lock --check
```

Result: exit 0: `Resolved 82 packages in 0.76ms`.

## Parallel-lane blockers

- Repository-wide Ruff is not clean because the other active lane has unsorted imports in
  `packages/deepagents_adapter/profile.py` and `tests/unit/test_agent_factory.py`. Task 5 scoped
  Ruff is clean; these files were not modified.
- Repository-wide Pyright is not clean because the other active lane has 27 errors in
  `packages/deepagents_adapter/factory.py`, `packages/deepagents_adapter/profile.py`,
  `tests/contract/test_deepagents_077.py`, and `tests/unit/test_agent_factory.py`, including
  missing/unknown third-party types, incompatible test-model overrides, and a currently missing
  `packages.model_gateway.client` import. Task 5 scoped Pyright is clean; these files were not
  modified.
- A live MinIO endpoint was unavailable because `RUNTIME_TEST_MINIO_ENDPOINT` was not set. The
  live integration remains explicit and opt-in rather than silently pretending to exercise a
  service.

## Concerns

- The package archive convention is tar-compatible and defaults to `package.tar.gz`; deployments
  using a different registry key can supply `object_key_factory`, but a different archive format
  would require a separate `PackageSource` implementation.
- Singleflight is process-local. It guarantees one cold load per digest inside one worker process;
  independently started workers may each download and validate once. Atomic digest directories
  and last-written markers keep shared disk entries fail-closed, but distributed coalescing is not
  in Task 5 scope.
- `DOWNLOAD_FAILED` is source-specific because the immutable Task 4 `PackageErrorCode` contract
  does not include that member and Task 5 was prohibited from modifying Task 4 files. Callers can
  distinguish source failures through `PackageSourceError.code` without weakening validator
  errors such as `DIGEST_MISMATCH`.

## Fix round 1: Race-safe publication and bounded archive preflight

### Status

The two Task 5 review findings are fixed in a scoped follow-up. No Task 6 source, test, or report
file was modified by this fix round.

### Cache publication fix

- `PackageCache` now acquires a per-digest advisory filesystem lock before rechecking an
  incomplete/missing entry, deleting a stale incomplete entry, downloading, validating, atomically
  renaming, and publishing the last-written `.verified` marker.
- Lock files live in a sibling lock directory rather than inside the digest entry, so a renamed
  package can remain marker-free during publication without being mistaken for stale data by a
  cooperating cache instance or process.
- A second publisher waits on the same digest lock, rechecks the marker after the first publisher
  releases it, and consumes the completed package without a second download or raw
  `FileExistsError`.
- Process death automatically releases the advisory lock. A later publisher can then remove the
  genuinely stale marker-free digest directory while holding the lock and repopulate it.
- Atomic staging-directory rename and last-written marker semantics remain unchanged.

### Archive resource fix

- Replaced eager `TarFile.getmembers()` with a two-pass streaming archive workflow. The first pass
  validates all metadata without creating package files or directories; extraction starts only
  after preflight succeeds.
- Total archive members are bounded across regular files, directories, and extended-header
  records. The existing regular-file count and per-file byte limits remain enforced.
- UTF-8 member paths are capped before path normalization or directory creation. PAX extended
  header key/value bytes are capped, and directory headers declaring content are rejected.
- Duplicate paths, unsupported member types, links, negative sizes, oversized files, and unsafe
  paths continue to fail with `DOWNLOAD_FAILED` before package validation or cache publication.

### TDD evidence

The marker-window regression paused the first cache after atomic rename and before marker write,
then started a second independent `PackageCache` instance against the same root.

Initial RED command:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/concurrency/test_package_singleflight.py::test_concurrent_package_caches_preserve_active_marker_publication -q
```

Result: exit 1. The first publisher raised `FileNotFoundError` writing `.verified` because the
second cache had removed its renamed digest directory. After the per-digest publication lock:

```text
1 passed in 0.38s
```

The three archive tests cover a 1,024-directory member bomb, an otherwise filesystem-valid
oversized nested path, and a 20,000-byte PAX extended header. The first RED run produced two
intended failures and revealed that the original path fixture exceeded the host filesystem limit;
the path fixture was narrowed before production changes and then failed for the intended missing
archive-policy reason. After the bounded streaming preflight:

```text
3 passed in 0.19s
```

### Verification

Focused Task 5 tests:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q
13 passed, 1 skipped in 0.75s
```

The skip is the explicit live MinIO integration because `RUNTIME_TEST_MINIO_ENDPOINT` is not
configured.

Scoped Ruff:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run ruff check packages/object_store/client.py packages/package_loader/minio_source.py packages/package_loader/cache.py tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py
All checks passed!
```

Scoped Pyright:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pyright packages/object_store/client.py packages/package_loader/minio_source.py packages/package_loader/cache.py tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py
0 errors, 0 warnings, 0 informations
```

An additional full Python run reached `100 passed, 3 skipped` but had two failures in the separate
Task 6 `tests/unit/test_agent_factory.py` lane (profile registration isolation and exception-cause
normalization). Those Task 6 files and the already-modified `task-6-report.md` were left untouched.

### Remaining concerns

- Publication locking uses POSIX `fcntl.flock`, matching the Linux worker/runtime environment and
  providing process-safe coordination for local cache roots. A network filesystem must provide
  correct advisory-lock semantics if operators place the local cache on one.
- Live MinIO behavior remains unexecuted in this environment because the explicit endpoint and
  credentials were not configured; deterministic source/cache tests continue to cover the MinIO
  client boundary without a service.

## Fix round 2: Bound PAX/global headers before parser payload reads

### Status

The scoped re-review finding is fixed without Task 6 changes. Oversized local and global PAX
extended-header declarations are now rejected before Python `tarfile` reads or parses their
payloads.

### Implementation

- Added a bounded `TarInfo` parser hook for both local PAX (`x`) and global PAX (`g`) records.
  Declared extended-header payload sizes above 16 KiB fail immediately with the stable
  `PackageSourceErrorCode.DOWNLOAD_FAILED` source error.
- Added a bounded `TarFile` raw-header counter. It counts ordinary and recursively processed
  extended headers before `tarfile` can yield a logical member, preventing chains of small PAX
  records from bypassing the existing total-member cap.
- Both the metadata preflight and extraction pass use the bounded parser. Existing normalized-path,
  logical-member, directory/header, regular-file count, and per-file byte limits are unchanged.

### TDD evidence

The regression constructs truncated local and global PAX headers that declare a 1,000,000-byte
payload. Before the fix, `tarfile` attempted that payload read and the stable source error had a
chained `ReadError('empty header')` cause.

RED:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py::test_package_source_rejects_extended_header_bomb_before_payload_read -q
2 failed in 0.18s
```

After moving the bound into the parser hook, both variants fail directly at the source boundary
without a parser cause:

```text
2 passed in 0.15s
```

### Verification

Focused Task 5 tests:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q
15 passed, 1 skipped in 0.75s
```

The skip remains the explicit live MinIO integration because `RUNTIME_TEST_MINIO_ENDPOINT` is not
configured.

Scoped Ruff:

```text
All checks passed!
```

Scoped Pyright:

```text
0 errors, 0 warnings, 0 informations
```

### Remaining concern

- The parser hook intentionally uses Python `tarfile`'s private `_proc_pax` extension point because
  the standard library exposes no public pre-payload validation hook. The call is isolated to the
  bounded `TarInfo` subclass and covered by focused local/global PAX regressions.

## Fix round 3: Bound GNU long headers before parser payload reads

### Status

The final scoped review finding is fixed without Task 6 changes. GNU longname (`L`) and longlink
(`K`) header declarations are now bounded before Python `tarfile` reads their payloads.

### Implementation

- Added `_BoundedTarInfo._proc_gnulong` with the same 16 KiB pre-read declared-size ceiling used
  for local/global PAX headers.
- Oversized or negative GNU long-header payload declarations fail directly with
  `PackageSourceErrorCode.DOWNLOAD_FAILED`; `tarfile._proc_gnulong` is never entered for them.
- Accepted GNU longname payloads still pass through the existing 1 KiB UTF-8 member-path limit,
  path normalization, member/file count, and per-file byte checks. GNU links remain unsupported by
  the existing member-type policy.
- PAX/global header guards, raw-header/member limits, two-pass extraction preflight, and the
  race-safe package cache publication protocol are unchanged.

### TDD evidence

The focused regression constructs truncated GNU `L` and `K` headers declaring a 1,000,000-byte
payload. Before the fix, both variants attempted the payload read and returned a source error with
a chained `ReadError('empty header')`.

RED:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py::test_package_source_rejects_gnu_long_header_bomb_before_payload_read -q
2 failed in 0.19s
```

GREEN:

```text
2 passed in 0.18s
```

### Verification

Focused Task 5 tests:

```text
UV_CACHE_DIR=/tmp/uv-cache-agent-runtime-task5 uv run pytest tests/integration/test_minio_package_source.py tests/concurrency/test_package_singleflight.py -q
17 passed, 1 skipped in 0.79s
```

The skip remains the explicit live MinIO integration because `RUNTIME_TEST_MINIO_ENDPOINT` is not
configured.

Scoped Ruff:

```text
All checks passed!
```

Scoped Pyright:

```text
0 errors, 0 warnings, 0 informations
```

### Remaining concern

- The standard library provides no public pre-read hook for GNU long headers, so the guard uses
  the isolated private `_proc_gnulong` extension point alongside `_proc_pax`. Both paths are
  covered by focused regressions for every supported extended-header type code.
