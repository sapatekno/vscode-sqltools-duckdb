import AbstractDriver from '@sqltools/base-driver';
import queries from './queries';
import { dirname } from 'path';
import fs from 'fs';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { IConnectionDriver, MConnectionExplorer, NSDatabase, ContextValue, Arg0 } from '@sqltools/types';
import { randomUUID } from 'crypto';
import keywordsCompletion from './keywords';

const LEGACY_DEFAULT_DUCKDB_CLI_PATH_WINDOWS = 'M:\\Programs\\duckdb\\duckdb.exe';
const DUCKDB_CLI_PATH_ENV = 'DUCKDB_CLI_PATH';
const AUTO_EXECUTABLE_CANDIDATES: Record<string, string[]> = {
  win32: [
    'duckdb.exe',
    LEGACY_DEFAULT_DUCKDB_CLI_PATH_WINDOWS,
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
const PRIORITIZED_SQL_WORDS = new Set(['SELECT', 'CREATE', 'UPDATE', 'DELETE']);

const execFileAsync = promisify(execFile);

const createResultId = () => {
  try {
    return randomUUID();
  } catch {
    return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }
};

const splitSqlStatements = (sqlText: string): string[] => {
  const statements: string[] = [];
  let current = '';
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inLineComment = false;
  let inBlockComment = false;

  for (let i = 0; i < sqlText.length; i++) {
    const ch = sqlText[i];
    const next = sqlText[i + 1];

    if (inLineComment) {
      current += ch;
      if (ch === '\n') inLineComment = false;
      continue;
    }

    if (inBlockComment) {
      current += ch;
      if (ch === '*' && next === '/') {
        current += next;
        i++;
        inBlockComment = false;
      }
      continue;
    }

    if (inSingleQuote) {
      current += ch;
      if (ch === '\'' && next === '\'') {
        current += next;
        i++;
        continue;
      }
      if (ch === '\'') inSingleQuote = false;
      continue;
    }

    if (inDoubleQuote) {
      current += ch;
      if (ch === '"' && next === '"') {
        current += next;
        i++;
        continue;
      }
      if (ch === '"') inDoubleQuote = false;
      continue;
    }

    if (ch === '-' && next === '-') {
      current += ch + next;
      i++;
      inLineComment = true;
      continue;
    }

    if (ch === '/' && next === '*') {
      current += ch + next;
      i++;
      inBlockComment = true;
      continue;
    }

    if (ch === '\'') {
      current += ch;
      inSingleQuote = true;
      continue;
    }

    if (ch === '"') {
      current += ch;
      inDoubleQuote = true;
      continue;
    }

    if (ch === ';') {
      const trimmed = current.trim();
      if (trimmed) statements.push(trimmed);
      current = '';
      continue;
    }

    current += ch;
  }

  const tail = current.trim();
  if (tail) statements.push(tail);
  return statements;
};

type TDuckDBConnection = {
  databasePath: string;
  executablePath: string;
  readOnly: boolean;
};

type TDynamicFunctionRow = {
  label?: string;
  name?: string;
  category?: string;
  functionType?: string;
  type?: string;
  description?: string;
  returnType?: string;
  return_type?: string;
  parameters?: any;
  parameterTypes?: any;
  parameter_types?: any;
  tags?: any;
};

type TFunctionCompletionMeta = {
  categories: Set<string>;
  returnTypes: Set<string>;
  signatures: Set<string>;
  tags: Set<string>;
  description: string;
};

export default class DuckDB extends AbstractDriver<TDuckDBConnection, any> implements IConnectionDriver {
  queries = queries;

  private getDatabase = async () => this.toAbsolutePath(this.credentials.database || ':memory:');

  private isPathLike = (candidate: string) => /[\\/]/.test(candidate) || /^[a-zA-Z]:/.test(candidate);

  private getConfiguredExecutablePath = async () => {
    const configuredPath = `${this.credentials.duckdbCliPath || this.credentials.duckdbPath || ''}`.trim();
    if (!configuredPath) return '';
    if (this.isPathLike(configuredPath)) {
      return this.toAbsolutePath(configuredPath);
    }
    return configuredPath;
  }

  private getEnvironmentExecutablePath = async () => {
    const envPath = `${process.env[DUCKDB_CLI_PATH_ENV] || ''}`.trim();
    if (!envPath) return '';
    if (this.isPathLike(envPath)) {
      return this.toAbsolutePath(envPath);
    }
    return envPath;
  }

  private getAutoExecutableCandidates = () => {
    const platformCandidates = AUTO_EXECUTABLE_CANDIDATES[process.platform] || ['duckdb'];
    const uniqueCandidates = new Set<string>(platformCandidates);
    uniqueCandidates.add(process.platform === 'win32' ? 'duckdb.exe' : 'duckdb');
    return [...uniqueCandidates];
  }

  private validateExecutablePath = async (executablePath: string) => {
    const isPathLike = this.isPathLike(executablePath);
    if (isPathLike && !fs.existsSync(executablePath)) {
      throw new Error(`DuckDB executable not found at "${executablePath}".`);
    }

    try {
      await execFileAsync(executablePath, ['--version'], { windowsHide: true });
    } catch (error) {
      const stderr = typeof error?.stderr === 'string' ? error.stderr.trim() : '';
      const message = stderr || error?.message || 'Failed to execute DuckDB CLI.';
      throw new Error(`DuckDB CLI validation failed. ${message}`);
    }
  }

  private resolveExecutablePath = async () => {
    const configuredPath = await this.getConfiguredExecutablePath();
    if (configuredPath) {
      try {
        await this.validateExecutablePath(configuredPath);
        return configuredPath;
      } catch (error) {
        throw new Error([
          `Invalid "duckdbCliPath" / "duckdbPath": "${configuredPath}".`,
          error?.message || error,
        ].join(' '));
      }
    }

    const environmentPath = await this.getEnvironmentExecutablePath();
    if (environmentPath) {
      try {
        await this.validateExecutablePath(environmentPath);
        return environmentPath;
      } catch (error) {
        throw new Error([
          `Invalid environment variable ${DUCKDB_CLI_PATH_ENV}: "${environmentPath}".`,
          error?.message || error,
        ].join(' '));
      }
    }

    const attempts: string[] = [];
    for (const candidate of this.getAutoExecutableCandidates()) {
      try {
        await this.validateExecutablePath(candidate);
        return candidate;
      } catch (error) {
        attempts.push(`${candidate} -> ${error?.message || error}`);
      }
    }

    const attemptedMessage = attempts.length ? ` Attempted: ${attempts.slice(0, 5).join(' | ')}` : '';
    throw new Error([
      'DuckDB CLI executable was not found.',
      `Set "duckdbCliPath" in the connection settings or add "duckdb" to your PATH.${attemptedMessage}`,
    ].join(' '));
  }

  private isReadOnlyEnabled = () => {
    const { readOnly } = this.credentials;
    if (typeof readOnly === 'boolean') return readOnly;
    if (typeof readOnly === 'string') {
      return ['1', 'true', 'yes', 'enabled', 'on'].indexOf(readOnly.trim().toLowerCase()) >= 0;
    }
    return false;
  }

  createDirIfNotExists = async (database: string, readOnly: boolean) => {
    if (`${database}`.toLowerCase() === ':memory:') return;
    if (readOnly) {
      if (!fs.existsSync(database)) {
        throw new Error(`Database file not found for read-only mode: "${database}"`);
      }
      return;
    }
    fs.mkdirSync(dirname(database), { recursive: true });
  }

  public async open() {
    if (this.connection) {
      return this.connection;
    }

    const executablePath = await this.resolveExecutablePath();
    const databasePath = await this.getDatabase();
    const readOnly = this.isReadOnlyEnabled();
    if (readOnly && `${databasePath}`.toLowerCase() === ':memory:') {
      throw new Error('Read-only mode is not supported with ":memory:" database.');
    }

    await this.createDirIfNotExists(databasePath, readOnly);

    this.connection = Promise.resolve({
      databasePath,
      executablePath,
      readOnly,
    });
    return this.connection;
  }

  public async close() {
    if (!this.connection) return Promise.resolve();
    await this.connection;
    this.connection = null;
  }

  private isResultSetQuery(query: string) {
    return /^(select|with|pragma|show|describe|call|explain|summarize)\b/i.test(query.trim());
  }

  private async runSingleQuery(query: string) {
    const { databasePath, executablePath, readOnly } = await this.open();
    const args = [databasePath];
    if (readOnly) {
      args.push('-readonly');
    }
    args.push('-json', '-c', query);

    try {
      const { stdout } = await execFileAsync(executablePath, args, {
        windowsHide: true,
        maxBuffer: 1024 * 1024 * 50,
      });

      if (!stdout || !stdout.trim()) {
        return [];
      }

      const parsed = JSON.parse(stdout);
      return Array.isArray(parsed) ? parsed : [parsed];
    } catch (error) {
      const stderr = typeof error?.stderr === 'string' ? error.stderr.trim() : '';
      const stdout = typeof error?.stdout === 'string' ? error.stdout.trim() : '';
      const reason = stderr || stdout || error?.message || 'DuckDB CLI query failed.';
      throw new Error(reason);
    }
  }

  public query: (typeof AbstractDriver)['prototype']['query'] = async (query, opt = {}) => {
    const { requestId } = opt;
    const queries = splitSqlStatements(query.toString()).filter(Boolean);
    const resultsAgg: NSDatabase.IResult[] = [];

    for (const q of queries) {
      try {
        const results: any[] = (await this.runSingleQuery(q)) || [];
        const messages = [];
        if (results.length === 0 && !this.isResultSetQuery(q)) {
          messages.push(this.prepareMessage('Statement executed successfully.'));
        }
        resultsAgg.push(<NSDatabase.IResult>{
          requestId,
          resultId: createResultId(),
          connId: this.getId(),
          cols: results && results.length ? Object.keys(results[0]) : [],
          messages,
          query: q,
          results,
        });
      } catch (error) {
        resultsAgg.push(<NSDatabase.IResult>{
          connId: this.getId(),
          requestId,
          resultId: createResultId(),
          cols: [],
          messages: [this.prepareMessage(error?.message || error)],
          error: true,
          rawError: error,
          query: q,
          results: [],
        });
        break;
      }
    }
    return resultsAgg;
  }

  public async testConnection() {
    await this.open();
    await this.query('SELECT 1', {});
  }

  public async getChildrenForItem({ item, parent }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.type) {
      case ContextValue.CONNECTION:
      case ContextValue.CONNECTED_CONNECTION:
        return <MConnectionExplorer.IChildItem[]>[
          { label: 'Tables', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.TABLE },
          { label: 'Views', type: ContextValue.RESOURCE_GROUP, iconId: 'folder', childType: ContextValue.VIEW },
        ];
      case ContextValue.TABLE:
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchColumns(item as NSDatabase.ITable));
      case ContextValue.RESOURCE_GROUP:
        return this.getChildrenForGroup({ item, parent });
    }
    return [];
  }

  private async getChildrenForGroup({ parent, item }: Arg0<IConnectionDriver['getChildrenForItem']>) {
    switch (item.childType) {
      case ContextValue.TABLE:
        return this.queryResults(this.queries.fetchTables(parent as NSDatabase.ISchema));
      case ContextValue.VIEW:
        return this.queryResults(this.queries.fetchViews(parent as NSDatabase.ISchema));
    }
    return [];
  }

  public searchItems(itemType: ContextValue, search: string, extraParams: any = {}): Promise<NSDatabase.SearchableItem[]> {
    switch (itemType) {
      case ContextValue.TABLE:
        return this.queryResults(this.queries.searchTables({ search }));
      case ContextValue.COLUMN:
        return this.queryResults(this.queries.searchColumns({ search, ...extraParams }));
    }
    return Promise.resolve([]);
  }

  private completionsCache: { [w: string]: NSDatabase.IStaticCompletion } = null;

  private normalizeCompletionLabel = (value: any) => {
    const label = `${value || ''}`.trim().toUpperCase();
    if (!label) return null;
    if (!/^[A-Z_][A-Z0-9_]*$/.test(label)) return null;
    return label;
  }

  private toStringArray = (value: any): string[] => {
    if (Array.isArray(value)) {
      return value.map(item => `${item ?? ''}`.trim()).filter(Boolean);
    }
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) return [];
      if (trimmed[0] === '[' && trimmed[trimmed.length - 1] === ']') {
        const inner = trimmed.slice(1, -1).trim();
        if (!inner) return [];
        return inner
          .split(',')
          .map(item => item.trim().replace(/^['"]|['"]$/g, ''))
          .filter(Boolean);
      }
      return [trimmed];
    }
    return [];
  }

  private toTagArray = (value: any): string[] => {
    if (!value) return [];
    if (typeof value === 'string') {
      const trimmed = value.trim();
      if (!trimmed) return [];
      try {
        return this.toTagArray(JSON.parse(trimmed));
      } catch {
        return [trimmed];
      }
    }
    if (typeof value === 'object') {
      const tags: string[] = [];
      Object.keys(value).forEach((key) => {
        const stringVal = `${value[key] ?? ''}`.trim();
        tags.push(stringVal ? `${key}=${stringVal}` : key);
      });
      return tags.filter(Boolean);
    }
    return [];
  }

  private addKeywordCompletions = (items: any[], completions: { [w: string]: NSDatabase.IStaticCompletion }) => {
    items.forEach((item: any) => {
      const label = this.normalizeCompletionLabel(item?.label);
      if (!label) return;
      const category = `${item?.category || 'KEYWORD'}`.trim().toUpperCase();

      completions[label] = {
        label,
        detail: label,
        filterText: label,
        sortText: (PRIORITIZED_SQL_WORDS.has(label) ? '2:' : '4:') + label,
        documentation: {
          value: `\`\`\`yaml\nWORD: ${label}\nTYPE: ${category}\n\`\`\``,
          kind: 'markdown'
        }
      };
    });
  }

  private addFunctionCompletions = (items: TDynamicFunctionRow[], completions: { [w: string]: NSDatabase.IStaticCompletion }) => {
    const functionMetaByLabel = new Map<string, TFunctionCompletionMeta>();

    items.forEach((item) => {
      const label = this.normalizeCompletionLabel(item.label || item.name);
      if (!label) return;

      const category = `${item.category || item.functionType || item.type || 'FUNCTION'}`.trim().toUpperCase() || 'FUNCTION';
      const returnType = `${item.returnType || item.return_type || ''}`.trim();
      const description = `${item.description || ''}`.trim();

      const parameterNames = this.toStringArray(item.parameters);
      const parameterTypes = this.toStringArray(item.parameterTypes || item.parameter_types);
      const tags = this.toTagArray(item.tags);
      const signatureParams = Array.from({ length: Math.max(parameterNames.length, parameterTypes.length) })
        .map((_, index) => {
          const parameterName = parameterNames[index] || '';
          const parameterType = parameterTypes[index] || '';
          if (parameterName && parameterType && parameterName !== parameterType) {
            return `${parameterName}: ${parameterType}`;
          }
          return parameterName || parameterType;
        })
        .filter(Boolean);
      const signature = `${label}(${signatureParams.join(', ')})`;

      const currentMeta = functionMetaByLabel.get(label) || {
        categories: new Set<string>(),
        returnTypes: new Set<string>(),
        signatures: new Set<string>(),
        tags: new Set<string>(),
        description: '',
      };
      currentMeta.categories.add(category);
      if (returnType) currentMeta.returnTypes.add(returnType);
      if (signatureParams.length) currentMeta.signatures.add(signature);
      tags.forEach(tag => currentMeta.tags.add(tag));
      if (!currentMeta.description && description) {
        currentMeta.description = description;
      }
      functionMetaByLabel.set(label, currentMeta);
    });

    functionMetaByLabel.forEach((meta, label) => {
      const categories = [...meta.categories];
      const returnTypes = [...meta.returnTypes].slice(0, 3);
      const tags = [...meta.tags].slice(0, 6);
      const signatures = [...meta.signatures].slice(0, 8);
      const yamlLines = [
        '```yaml',
        `WORD: ${label}`,
        `TYPE: ${categories.join(', ') || 'FUNCTION'}`,
      ];
      if (returnTypes.length) {
        yamlLines.push(`RETURNS: ${returnTypes.join(', ')}`);
      }
      if (tags.length) {
        yamlLines.push(`TAGS: ${tags.join(', ')}`);
      }
      yamlLines.push('```');

      const signatureBlock = signatures.length
        ? `\n\nSignatures:\n${signatures.map(signature => `- \`${signature}\``).join('\n')}`
        : '';
      const descriptionBlock = meta.description ? `\n\n${meta.description}` : '';
      const docValue = `${yamlLines.join('\n')}${signatureBlock}${descriptionBlock}`;

      if (completions[label]) {
        const existingDocumentation = `${completions[label].documentation?.value || ''}`.trim();
        completions[label].documentation = {
          value: existingDocumentation ? `${existingDocumentation}\n\n${docValue}` : docValue,
          kind: 'markdown'
        };
        return;
      }

      completions[label] = {
        label,
        detail: signatures[0] || `${label}(...)`,
        filterText: label,
        sortText: `3:${label}`,
        documentation: {
          value: docValue,
          kind: 'markdown'
        }
      };
    });
  }

  private loadDynamicFunctions = async (): Promise<TDynamicFunctionRow[]> => {
    try {
      return await this.queryResults(`
SELECT
  function_name AS label,
  UPPER(function_type) AS category,
  function_type AS functionType,
  description,
  return_type AS returnType,
  to_json(parameters) AS parameters,
  to_json(parameter_types) AS parameterTypes,
  to_json(tags) AS tags
FROM duckdb_functions()
WHERE function_name IS NOT NULL
  AND length(trim(function_name)) > 0
ORDER BY function_name;
`);
    } catch (error) {
      this.log.warn(`Failed to load duckdb_functions() metadata: ${error?.message || error}`);
    }

    try {
      const rows = await this.queryResults('PRAGMA functions;');
      return rows.map((row: any) => ({
        label: row?.name,
        category: `${row?.type || 'FUNCTION'}`.toUpperCase(),
        functionType: row?.type,
        returnType: row?.return_type,
        parameters: row?.parameters,
      }));
    } catch (error) {
      this.log.warn(`Failed to load PRAGMA functions metadata: ${error?.message || error}`);
      return [];
    }
  }

  public getStaticCompletions = async () => {
    if (this.completionsCache) return this.completionsCache;

    const completions: { [w: string]: NSDatabase.IStaticCompletion } = {};

    try {
      const items = await this.queryResults('SELECT UPPER(keyword_name) AS label, UPPER(keyword_category) AS category FROM duckdb_keywords() ORDER BY keyword_name;');
      this.addKeywordCompletions(items, completions);
    } catch (error) {
      this.log.warn(`Failed to load DuckDB keywords dynamically: ${error?.message || error}`);
      Object.assign(completions, keywordsCompletion);
    }

    if (!Object.keys(completions).length) {
      Object.assign(completions, keywordsCompletion);
    }

    const functionItems = await this.loadDynamicFunctions();
    this.addFunctionCompletions(functionItems, completions);

    if (!Object.keys(completions).length) {
      Object.assign(completions, keywordsCompletion);
    }

    this.completionsCache = completions;
    return this.completionsCache;
  }
}
