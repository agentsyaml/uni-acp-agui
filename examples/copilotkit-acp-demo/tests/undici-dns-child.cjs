/* eslint-disable @typescript-eslint/no-require-imports -- mutable node:dns CommonJS object is required for this isolated test seam */
const assert = require('node:assert/strict');
const dns = require('node:dns');
const { syncBuiltinESMExports } = require('node:module');
const { createRequire } = require('node:module');

async function main() {
  const addresses = process.argv[2] === 'mixed'
    ? [{ address: '8.8.8.8', family: 4 }, { address: '10.0.0.1', family: 4 }]
    : [{ address: '10.0.0.1', family: 4 }];
  let called = false;
  let requestedAll = false;
  Object.defineProperty(dns, 'lookup', {
    configurable: true,
    writable: true,
    value(_host, options, callback) {
      called = true;
      requestedAll = options?.all === true;
      if (requestedAll) callback(null, addresses);
      else callback(null, addresses[0].address, addresses[0].family);
    },
  });
  syncBuiltinESMExports();

  const rootRequire = createRequire(`${process.cwd()}/package.json`);
  const vertexRequire = createRequire(rootRequire.resolve('@ai-sdk/google-vertex/package.json'));
  const utilsPath = vertexRequire.resolve('@ai-sdk/provider-utils/package.json');
  const utilsRequire = createRequire(utilsPath);
  const utils = utilsRequire('@ai-sdk/provider-utils');
  let error;
  try {
    await utils.fetchWithValidatedEndpoint({ url: 'http://private-test.invalid/' });
  } catch (caught) {
    error = caught;
  }
  assert.ok(called, 'injected DNS lookup must run');
  assert.ok(requestedAll, 'safe lookup must request all DNS answers');
  assert.ok(error, 'private DNS answer must reject');
  const details = `${error.name}: ${error.message}; ${error.cause?.name}: ${error.cause?.message}`;
  assert.match(details, /private|unsafe|blocked|forbidden|ip.*range/i);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
