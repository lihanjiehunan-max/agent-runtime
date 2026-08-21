# Frozen runtime compatibility baselines

## Node Runtime 0.2

`node-runtime-0.2/` is an unchanged copy of the persisted Enterprise Agent Runtime Kernel 0.2 source, entrypoints, and tests taken before the Python migration workspace was introduced.

On 2026-08-20, the archived test suite completed with 92 of 92 tests passing. The raw output is preserved at `evidence/node-0.2-tests.txt`.

Run the frozen baseline locally with Node.js 24 or later:

```bash
cd legacy/node-runtime-0.2
npm test
npm run check
```

## Runtime 0.3 behavior evidence

Runtime 0.3 was completed previously but was not persisted as a restorable source archive. It remains compatibility evidence only and included:

- `deepagents.js` with OpenAI-compatible model execution;
- `POST /api/runs/:runId/execute`;
- `GET /api/agent/status`;
- normalized Agent stream events;
- a least-privilege `read_context` tool profile;
- a chat test console; and
- 114 passing tests.

The Python runtime must preserve applicable 0.2 safety properties and treat the documented 0.3 HTTP and event behavior as reference evidence. This directory does not claim that the 0.3 source can be restored.
