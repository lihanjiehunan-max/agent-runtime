process.env.AGENT_RUNTIME_DB ??= './data/runtime.db';
process.env.HOST ??= '127.0.0.1';
process.env.PORT ??= '3000';
process.env.AGENT_RUNTIME_ALLOW_DEV_AUTH = '1';
await import('../src/server.js');
