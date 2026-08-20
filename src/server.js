import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRuntimeAuthenticator } from './auth.js';
import { createApp } from './api.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const dbPath = process.env.AGENT_RUNTIME_DB ?? resolve(root, 'data', 'runtime.db');
mkdirSync(dirname(dbPath), { recursive: true });
const port = Number(process.env.PORT ?? 3000);
const host = process.env.HOST ?? '127.0.0.1';
const authenticator = createRuntimeAuthenticator(process.env);
const app = createApp({ dbPath, authenticator });

app.server.listen(port, host, () => {
  process.stdout.write(`Agent Runtime listening on http://${host}:${port}\n`);
});

function shutdown() {
  app.server.close(() => {
    app.db.close();
    process.exit(0);
  });
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
