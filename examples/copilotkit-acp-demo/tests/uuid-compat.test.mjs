import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';

const root = createRequire(import.meta.url.replace(/tests\/[^/]+$/, 'package.json'));
const consumers = ['@copilotkit/runtime', '@copilotkit/shared', '@ag-ui/client', 'mermaid'];

test('UUID 11.1.1 satisfies installed consumer ranges and the runtime UUID API', async () => {
  for (const name of consumers) {
    const parentRequire = createRequire(root.resolve(`${name}/package.json`));
    assert.equal(parentRequire('uuid/package.json').version, '11.1.1', `${name} resolution`);
  }

  const runtimeRequire = createRequire(root.resolve('@copilotkit/runtime/package.json'));
  const shared = runtimeRequire('@copilotkit/shared');
  assert.equal(typeof shared.randomUUID, 'function');
  const first = shared.randomUUID();
  const second = shared.randomUUID();
  assert.match(first, /^[0-9a-f-]{36}$/i);
  assert.notEqual(first, second);

  const mermaidRequire = createRequire(root.resolve('mermaid/package.json'));
  const uuid = mermaidRequire('uuid');
  assert.equal(typeof uuid.v4, 'function');
  assert.match(uuid.v4(), /^[0-9a-f-]{36}$/i);
});
