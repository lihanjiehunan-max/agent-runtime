# Single-host integration acceptance

Goal: continue ffa3a86, fix the evidenced cold-start race, and verify release-built services through a proxy and two independent API ports with dedicated PostgreSQL, Redis and MinIO.

Scope: one host, two APIs, three DeepAgents Workers, Console/Nginx, isolated backing services, explicit synthetic model/tool fixture. No live credential discovery or fallback; no production data, no automatic Execution retry, no main-branch merge.

1. Restore artifact/source hashes from ffa3a86; preserve earlier evidence. DONE.
2. Reproduce missing infrastructure readiness with deployment tests; add service_healthy dependencies only to the optional local infrastructure overlay. RED/GREEN saved.
3. Add loopback-only API port override. Keep release base and external-infrastructure deployment independent of local services.
4. Extend release smoke with independent API identities, cross-port Session/Thread/Digest continuity, Worker restart, idempotent submission, async/invoke/SSE, API loss/replay, child-task coordination and large-result paging, and a 50-Session load test.
5. Run existing browser regression/negative controls; add a browser attach mode gated on loopback + synthetic profile so the same tests run against the PostgreSQL-backed release stack.
6. On one fresh CI commit run full regression, legacy 9-container fault tests, bounded load, release/multiport/browser acceptance and old-code negative controls. Archive source, ports, container identities, private-configuration-free logs and JSON results.
7. Verify latest remote results, download/check hashes, deliver source/patch/logs/commands. Distinguish local Python 3.13 supplementary results from Python 3.12 / Node 24 CI results. Do not claim live-model, enterprise-data, long-duration or multi-host HA acceptance.

Known prior failure: ffa3a86 / run 34583448009 failed during release up because API started before PostgreSQL listened; api-b unhealthy. Browser stage was skipped. Current sandbox has no Docker/PostgreSQL/Redis/MinIO server binaries and no outbound DNS; its managed Chromium blocks loopback navigation. Full acceptance belongs on the connected GitHub runner, not this sandbox.
