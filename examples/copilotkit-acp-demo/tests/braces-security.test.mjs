import { describe, expect, test } from 'bun:test';
import braces from 'braces';
import micromatch from 'micromatch';

const nested = depth => '{'.repeat(depth) + 'x' + '}'.repeat(depth);
const deepAst = depth => {
  const root = { type: 'root', nodes: [] };
  let node = root;
  for (let i = 0; i < depth; i++) {
    const child = { type: 'brace', nodes: [] };
    node.nodes.push(child);
    node = child;
  }
  node.nodes.push({ type: 'text', value: 'x' });
  return root;
};

describe('braces nesting limit', () => {
  test('accepts 100 parsed groups and rejects 101 explicitly', () => {
    expect(() => braces.parse(nested(100))).not.toThrow();
    expect(() => braces.parse(nested(101))).toThrow(/maximum nesting depth of 100/);
  });

  test('checks manually supplied ASTs before all recursive walkers', () => {
    const ast = deepAst(103);
    for (const walk of [braces.compile, braces.stringify, braces.expand]) {
      expect(() => walk(ast)).toThrow(/maximum nesting depth of 100/);
    }
  });

  test('rejects cycles without recursing', () => {
    const ast = { type: 'root', nodes: [] };
    ast.nodes.push(ast);
    for (const walk of [braces.compile, braces.stringify, braces.expand]) {
      expect(() => walk(ast)).toThrow(/maximum nesting depth of 100/);
    }
  });

  test('preserves baseline brace, range, literal and extglob behavior', () => {
    expect(braces.expand('{a,{b,c}}')).toEqual(['a', 'b', 'c']);
    expect(braces.expand('{1..3}')).toEqual(['1', '2', '3']);
    expect(braces.expand('\\{a\\}')).toEqual(['{a}']);
    expect(braces.expand('[{a,b}]')).toEqual(['[{a,b}]']);
    expect(micromatch(['a.js', 'b.ts', 'c.js'], ['*.js'])).toEqual(['a.js', 'c.js']);
    expect(micromatch(['a', 'b', 'c'], ['@(a|b)'])).toEqual(['a', 'b']);
  });
});
