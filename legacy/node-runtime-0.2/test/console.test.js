import assert from 'node:assert/strict';
import test from 'node:test';
import { createDevelopmentAuthenticator } from '../src/auth.js';
import { createApp } from '../src/api.js';

test('operator console and assets are served without tenant credentials', async (t) => {
  const app = createApp({ dbPath: ':memory:', authenticator: createDevelopmentAuthenticator() });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  t.after(() => app.server.close());
  const base = `http://127.0.0.1:${app.server.address().port}`;

  const page = await fetch(`${base}/`);
  assert.equal(page.status, 200);
  assert.match(page.headers.get('content-type'), /text\/html/);
  assert.match(await page.text(), /Agent Runtime Control Center/);

  const script = await fetch(`${base}/app.js`);
  assert.equal(script.status, 200);
  assert.match(script.headers.get('content-type'), /javascript/);

  const traversal = await fetch(`${base}/..%2Fsrc%2Fruntime.js`);
  assert.equal(traversal.status, 404);
});
