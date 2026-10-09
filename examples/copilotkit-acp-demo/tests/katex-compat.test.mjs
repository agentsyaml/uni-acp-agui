import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const copilotRequire = createRequire(require.resolve('@copilotkit/react-core/package.json'));
const katex = copilotRequire('katex');

assert.match(katex.renderToString('x^2', { throwOnError: true }), /katex-html/);
assert.match(katex.renderToString('x^2', { displayMode: true, throwOnError: true }), /katex-display/);
assert.throws(() => katex.renderToString('\\notARealCommand', { throwOnError: true }), /KaTeX parse error/);
const allowHttpLinks = ({ protocol }) => protocol === 'http' || protocol === 'https';
assert.match(katex.renderToString('\\href{https://example.com}{link}', { trust: allowHttpLinks }), /href="https:\/\/example\.com"/);
assert.doesNotMatch(katex.renderToString('\\href{javascript:alert(1)}{link}', { trust: allowHttpLinks }), /href="javascript:/i);
