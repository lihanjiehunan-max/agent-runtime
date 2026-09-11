# Docker Distributed Validation Implementation Plan

> **For agentic workers:** Use executing-plans task by task, with test evidence before completion.

**Goal:** Produce reproducible multi-container integration and fault-injection evidence.
**Architecture:** Two stateless API containers and three independent DeepAgents Workers share PostgreSQL, Redis notifications and MinIO artifacts. A separate deterministic network model/tool fixture replaces only external model/business responses, not runtime coordination.
**Tech Stack:** Python 3.12, DeepAgents 0.7.7, SQLAlchemy, PostgreSQL, Redis, S3, Docker Compose, GitHub Actions.
**Spec:** docs/superpowers/specs/2026-09-11-docker-distributed-validation.md

## Global constraints
- Preserve original validation_runtime entrypoint.
- No automatic retry of interrupted executions or uncertain external effects.
- Fence checkpoint, event and completion writes in the same transaction as ownership checks.
- Run all destructive tests only in the isolated Compose project.
- Never include credentials or external business data in evidence artifacts.

## Tasks
- [x] Recover source snapshot and run existing regression suite.
- [x] Add failing tests for SQL checkpoint persistence/fencing and Harness/Worker/API availability.
- [x] Implement missing checkpoints.py, harness.py, worker.py and api.py; keep state in the Checkpointer only.
- [x] Build a fixture model/tool HTTP service; run actual DeepAgents graph end to end locally.
- [x] Implement deploy/compose.validation.yml and isolated fault-injection script.
- [ ] Push reviewed source; execute Docker build, tests and faults on GitHub Actions.
- [ ] Diagnose failures with raw logs; write regression tests and rerun the complete acceptance suite.
- [ ] Export source/evidence and a report with pass/fail/unverified boundaries; verify branch and commit.

## Verification commands
```
python -m pytest tests -m 'not real_gateway' -q
npm test
docker compose -p dh-validation -f deploy/compose.validation.yml up -d --build --wait
python scripts/validate_docker_cluster.py
```
