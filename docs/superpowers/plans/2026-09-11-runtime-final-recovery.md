# Runtime final recovery implementation plan

**Goal:** Continue the verified 6e439fd snapshot, close lost-submit-response recovery and bounded child-result delivery, and collect new reproducible acceptance evidence.

**Architecture:** Retain API / Worker / Console and the current fenced SQL checkpoint authority. Submission recovery reuses a logical request key; it is not automatic Execution retry. Child results stay in content-addressed storage, with bounded excerpts and an authorized paging tool.

**Tech stack:** Existing locked DeepAgents 0.7.7 stack; release verification Python 3.12 / Node 24. Local restored environment Python 3.13 / Node 22 is supplementary evidence only.

**Spec:** `docs/operations/distributed-runtime-delivery.md` and the user's approved final-repair scope.

## Constraints

- Do not replace existing features or downgrade to 400a740.
- Do not enable automatic execution retry, bypass UNKNOWN reconciliation, or change identity scope.
- Do not report synthetic tests as live-model / enterprise-tool / multi-host acceptance.
- Preserve the existing dependency locks and operational interfaces.

## Task 1: Recover and prove the baseline

- [x] Verify artifact/source hashes and restore 6e439fd into an isolated local directory.
- [x] Install retained offline wheels, run the existing Python and Node suites, save logs.

## Task 2: Lost-response submission recovery

Files: `apps/runtime_console/src/submission.mjs`, `submission.test.mjs`, `main.tsx`, `package.json`; `scripts/validate_console_browser.py`.

- [ ] Reproduce a POST committed at the actual API whose response is dropped, switch sessions, resend the original message, and assert identical request key / Execution ID.
- [ ] Add page-memory pending submissions by Session, single-flight POST handling, explicit ambiguous-result messaging, and logout cleanup. Clear only confirmed requests or definitive client errors.
- [ ] Guard stale UI updates after navigation; include Node tests in the console test command.

## Task 3: Bounded child-result delivery

Files: `apps/distributed_runtime/child_results.py`, `harness.py`; `tests/distributed_runtime/test_child_results.py`.

- [ ] RED: require the bounded result component and tests for small/large data, paging, sibling/unrelated rejection, lease fencing and integrity.
- [ ] Return metadata + a labelled prefix excerpt; inline only small JSON. Offer `read_child_result` to the delegating Agent, checking direct parent membership and current claim before/after storage reads.
- [ ] Record metadata-only read events; keep full outputs and their digest unchanged.

## Task 4: Verification and delivery

- [ ] Run new tests and full regressions; preserve RED and GREEN logs separately.
- [ ] Push readable source to the existing development branch without forced updates.
- [ ] On the new commit run Python 3.12 / Node 24, Docker fault acceptance, release/proxy smoke and Chromium recovery verification.
- [ ] Download evidence, verify commit/hashes, package source+patch+logs+handoff; explicitly retain blocked live acceptance.
