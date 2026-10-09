import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { spawnSync } from 'node:child_process';
import { once } from 'node:events';
import test from 'node:test';

const rootRequire = createRequire(import.meta.url.replace(/tests\/[^/]+$/, 'package.json'));
const undiciVersion = rootRequire('undici/package.json').version;
assert.equal(undiciVersion, '6.29.0');

const consumers = ['@ai-sdk/google-vertex', '@ai-sdk/openai-compatible'];
const utilities = consumers.map((consumer) => {
  const consumerRequire = createRequire(rootRequire.resolve(`${consumer}/package.json`));
  const packagePath = consumerRequire.resolve('@ai-sdk/provider-utils/package.json');
  const utilityRequire = createRequire(packagePath);
  assert.equal(utilityRequire('undici/package.json').version, undiciVersion);
  return utilityRequire('@ai-sdk/provider-utils');
});

test('default endpoint lookup rejects private-only and mixed public/private DNS answers', () => {
  for (const scenario of ['private', 'mixed']) {
    const result = spawnSync('node', ['tests/undici-dns-child.cjs', scenario], {
      encoding: 'utf8', timeout: 10_000, env: process.env,
    });
    assert.equal(result.error, undefined, result.error?.message);
    assert.equal(result.status, 0, `${scenario}: ${result.stderr || result.stdout}`);
  }
});


test('provider-utils 3 uses Undici 6 Agent for validated redirects and aborts', async () => {
  const { Agent, fetch } = rootRequire('undici');
  const server = createServer((request, response) => {
    if (request.url === '/redirect') {
      response.writeHead(302, { location: '/final' }).end();
    } else if (request.url === '/slow') {
      // Keep this response open until the test aborts its request.
    } else {
      response.writeHead(200, { 'x-local': 'yes' }).end('local body');
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const agent = new Agent();
  const localFetch = (url, init) => fetch(url, { ...init, dispatcher: agent });

  try {
    for (const utility of utilities) {
      const response = await utility.fetchWithValidatedRedirects({
        url: `${origin}/redirect`, fetch: localFetch, trustedOrigin: origin,
      });
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('x-local'), 'yes');
      assert.equal(await response.text(), 'local body');

      const controller = new AbortController();
      const pending = utility.fetchWithValidatedRedirects({
        url: `${origin}/slow`, fetch: localFetch, trustedOrigin: origin,
        abortSignal: controller.signal,
      });
      setTimeout(() => controller.abort(), 20);
      await assert.rejects(pending, { name: 'AbortError' });
    }
  } finally {
    agent.close?.();
    agent.destroy?.();
    server.closeAllConnections();
    server.close();
  }
});
