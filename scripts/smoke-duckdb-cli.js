'use strict';

const cp = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const DEFAULT_DUCKDB_CLI_PATH_WINDOWS = 'M:\\Programs\\duckdb\\duckdb.exe';
const AUTO_DUCKDB_CLI_CANDIDATES = {
  win32: [
    'duckdb.exe',
    DEFAULT_DUCKDB_CLI_PATH_WINDOWS,
    'C:\\Program Files\\DuckDB\\duckdb.exe',
    'C:\\Program Files (x86)\\DuckDB\\duckdb.exe',
  ],
  darwin: [
    'duckdb',
    '/opt/homebrew/bin/duckdb',
    '/usr/local/bin/duckdb',
    '/usr/bin/duckdb',
  ],
  linux: [
    'duckdb',
    '/usr/local/bin/duckdb',
    '/usr/bin/duckdb',
    '/snap/bin/duckdb',
  ],
};

function isPathLike(value) {
  return /[\\/]/.test(value) || /^[a-zA-Z]:/.test(value);
}

function canExecuteDuckDB(candidate) {
  if (!candidate) return false;
  if (isPathLike(candidate) && !fs.existsSync(candidate)) return false;
  try {
    cp.execFileSync(candidate, ['--version'], {
      encoding: 'utf8',
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return true;
  } catch {
    return false;
  }
}

function resolveDuckDBCliPath() {
  const configured = `${process.env.DUCKDB_CLI_PATH || ''}`.trim();
  if (configured) {
    if (canExecuteDuckDB(configured)) return configured;
    throw new Error(`Invalid DUCKDB_CLI_PATH: "${configured}"`);
  }

  const platformCandidates = AUTO_DUCKDB_CLI_CANDIDATES[process.platform] || ['duckdb'];
  for (const candidate of platformCandidates) {
    if (canExecuteDuckDB(candidate)) return candidate;
  }
  return process.platform === 'win32' ? 'duckdb.exe' : 'duckdb';
}

const duckdbCliPath = resolveDuckDBCliPath();

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sqltools-duckdb-smoke-'));
const databasePath = path.join(tempDir, 'smoke.duckdb');

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

function toErrorMessage(error) {
  const stderr = error && error.stderr ? `${error.stderr}`.trim() : '';
  const stdout = error && error.stdout ? `${error.stdout}`.trim() : '';
  return stderr || stdout || (error && error.message) || `${error}`;
}

function runRaw(args) {
  return cp.execFileSync(duckdbCliPath, args, {
    encoding: 'utf8',
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

function runJson(sql, options = {}) {
  const args = [databasePath];
  if (options.readOnly) {
    args.push('-readonly');
  }
  args.push('-json', '-c', sql);
  const output = runRaw(args).trim();
  if (!output) return [];
  return JSON.parse(output);
}

function runSmoke() {
  const version = runRaw(['--version']).trim();
  assert(version.length > 0, 'duckdb --version returned empty output');
  console.log(`[smoke] CLI: ${duckdbCliPath}`);
  console.log(`[smoke] Version: ${version}`);

  runRaw([databasePath, '-c', "CREATE SCHEMA IF NOT EXISTS s1; CREATE TABLE IF NOT EXISTS s1.t1(id INTEGER PRIMARY KEY, name VARCHAR); DELETE FROM s1.t1; INSERT INTO s1.t1 VALUES (1, 'alpha'), (2, 'beta'); CREATE OR REPLACE VIEW s1.v1 AS SELECT * FROM s1.t1;"]);

  const tables = runJson("SELECT table_name AS label, table_schema AS schema, 'table' AS type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND LOWER(table_type) = 'base table' ORDER BY table_schema, table_name;");
  assert(tables.some(item => item.schema === 's1' && item.label === 't1'), 'Table explorer query did not return s1.t1');

  const views = runJson("SELECT table_name AS label, table_schema AS schema, 'view' AS type FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND LOWER(table_type) = 'view' ORDER BY table_schema, table_name;");
  assert(views.some(item => item.schema === 's1' && item.label === 'v1'), 'View explorer query did not return s1.v1');

  const columns = runJson("SELECT C.column_name AS label, C.column_name AS name, C.ordinal_position - 1 AS cid, C.data_type AS dataType, CASE WHEN C.is_nullable = 'YES' THEN 1 ELSE 0 END AS isNullable, CASE WHEN PK.column_name IS NULL THEN 0 ELSE 1 END AS isPk, 'column' as type FROM information_schema.columns AS C LEFT JOIN ( SELECT KCU.table_schema, KCU.table_name, KCU.column_name FROM information_schema.table_constraints AS TC INNER JOIN information_schema.key_column_usage AS KCU ON TC.constraint_name = KCU.constraint_name AND TC.table_schema = KCU.table_schema AND TC.table_name = KCU.table_name WHERE TC.constraint_type = 'PRIMARY KEY' ) AS PK ON PK.table_schema = C.table_schema AND PK.table_name = C.table_name AND PK.column_name = C.column_name WHERE C.table_schema = 's1' AND C.table_name = 't1' ORDER BY C.ordinal_position ASC;");
  assert(columns.length === 2, `Expected 2 columns for s1.t1, got ${columns.length}`);
  assert(columns[0].label === 'id', 'Expected first column to be id');
  assert(Number(columns[0].isPk) === 1, 'Expected id to be marked as primary key');

  const readOnlyResult = runJson('SELECT COUNT(*) AS total FROM s1.t1;', { readOnly: true });
  assert(readOnlyResult.length === 1, 'Read-only SELECT did not return row count');
  assert(Number(readOnlyResult[0].total) === 2, `Expected row count 2 in read-only mode, got ${readOnlyResult[0].total}`);

  let readOnlyWriteBlocked = false;
  try {
    runJson('CREATE TABLE s1.read_only_should_fail(id INTEGER);', { readOnly: true });
  } catch (error) {
    const message = toErrorMessage(error).toLowerCase();
    readOnlyWriteBlocked = (
      message.includes('read-only') ||
      message.includes('readonly') ||
      message.includes('cannot execute') ||
      message.includes('permission')
    );
  }
  assert(readOnlyWriteBlocked, 'Write query was not blocked in read-only mode');

  const keywords = runJson('SELECT UPPER(keyword_name) AS label FROM duckdb_keywords() ORDER BY keyword_name LIMIT 1;');
  assert(keywords.length > 0, 'duckdb_keywords() returned no rows');

  const functions = runJson("SELECT function_name AS label, function_type AS category, to_json(tags) AS tags FROM duckdb_functions() WHERE function_name IS NOT NULL AND length(trim(function_name)) > 0 LIMIT 20;");
  assert(functions.length > 0, 'duckdb_functions() returned no rows');
  assert(functions.some(item => !!item.label), 'duckdb_functions() returned rows without function_name');

  console.log('[smoke] PASS');
}

try {
  runSmoke();
  process.exitCode = 0;
} catch (error) {
  console.error(`[smoke] FAIL: ${toErrorMessage(error)}`);
  process.exitCode = 1;
} finally {
  fs.rmSync(tempDir, { recursive: true, force: true });
}
