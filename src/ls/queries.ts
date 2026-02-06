import { IBaseQueries, ContextValue, NSDatabase } from '@sqltools/types';
import queryFactory from '@sqltools/base-driver/dist/lib/factory';

function escapeIdentifier(identifier: string) {
  return `"${`${identifier}`.replace(/"/g, '""')}"`;
}

function escapeLiteral(value: string) {
  return `${value}`.replace(/'/g, "''");
}

function resolveTableSchema(table: Partial<NSDatabase.ITable>) {
  return `${(table as any).schema || 'main'}`;
}

function resolveTableName(table: Partial<NSDatabase.ITable>) {
  return `${table.label || table.toString()}`;
}

function escapeTableName(table: Partial<NSDatabase.ITable>) {
  const schema = resolveTableSchema(table);
  const tableName = resolveTableName(table);
  return `${escapeIdentifier(schema)}.${escapeIdentifier(tableName)}`;
}

function filterTableList(tables: Partial<NSDatabase.ITable>[] = []) {
  const validTables = tables.filter(t => !!t.label);
  if (!validTables.length) return '';
  return `AND LOWER(C.table_name) IN (${validTables.map(t => `'${escapeLiteral(`${t.label}`.toLowerCase())}'`).join(', ')})`;
}

const describeTable: IBaseQueries['describeTable'] = queryFactory`
SELECT C.*
FROM information_schema.columns AS C
WHERE C.table_schema = '${p => escapeLiteral(resolveTableSchema(p))}'
  AND C.table_name = '${p => escapeLiteral(resolveTableName(p))}'
ORDER BY C.ordinal_position ASC
`;

const fetchColumns: IBaseQueries['fetchColumns'] = queryFactory`
SELECT C.column_name AS label,
  C.column_name AS name,
  C.ordinal_position - 1 AS cid,
  C.data_type AS dataType,
  CASE WHEN C.is_nullable = 'YES' THEN 1 ELSE 0 END AS isNullable,
  CASE WHEN PK.column_name IS NULL THEN 0 ELSE 1 END AS isPk,
  '${ContextValue.COLUMN}' as type
FROM information_schema.columns AS C
LEFT JOIN (
  SELECT KCU.table_schema, KCU.table_name, KCU.column_name
  FROM information_schema.table_constraints AS TC
  INNER JOIN information_schema.key_column_usage AS KCU
    ON TC.constraint_name = KCU.constraint_name
    AND TC.table_schema = KCU.table_schema
    AND TC.table_name = KCU.table_name
  WHERE TC.constraint_type = 'PRIMARY KEY'
) AS PK
  ON PK.table_schema = C.table_schema
  AND PK.table_name = C.table_name
  AND PK.column_name = C.column_name
WHERE C.table_schema = '${p => escapeLiteral(resolveTableSchema(p))}'
  AND C.table_name = '${p => escapeLiteral(resolveTableName(p))}'
ORDER BY C.ordinal_position ASC
`;

const fetchRecords: IBaseQueries['fetchRecords'] = queryFactory`
SELECT *
FROM ${p => escapeTableName(p.table)}
LIMIT ${p => p.limit || 50}
OFFSET ${p => p.offset || 0};
`;

const countRecords: IBaseQueries['countRecords'] = queryFactory`
SELECT count(1) AS total
FROM ${p => escapeTableName(p.table)};
`;

const fetchTablesAndViews = (type: ContextValue, tableType = 'BASE TABLE'): IBaseQueries['fetchTables'] => queryFactory`
SELECT table_name AS label,
  table_schema AS schema,
  '${type}' AS type
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
  AND LOWER(table_type) = '${tableType.toLowerCase()}'
ORDER BY table_schema, table_name
`;

const fetchTables: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.TABLE);
const fetchViews: IBaseQueries['fetchTables'] = fetchTablesAndViews(ContextValue.VIEW, 'VIEW');

const searchTables: IBaseQueries['searchTables'] = queryFactory`
SELECT table_name AS label,
  table_schema AS schema,
  CASE
    WHEN LOWER(table_type) = 'view' THEN '${ContextValue.VIEW}'
    ELSE '${ContextValue.TABLE}'
  END AS type
FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
${p => p.search ? `AND (LOWER(table_name) LIKE '%${escapeLiteral(p.search.toLowerCase())}%' OR LOWER(table_schema) LIKE '%${escapeLiteral(p.search.toLowerCase())}%')` : ''}
ORDER BY table_schema, table_name
`;
const searchColumns: IBaseQueries['searchColumns'] = queryFactory`
SELECT C.column_name AS label,
  C.table_name AS "table",
  C.table_schema AS schema,
  C.data_type AS dataType,
  CASE WHEN C.is_nullable = 'YES' THEN 1 ELSE 0 END AS isNullable,
  CASE WHEN PK.column_name IS NULL THEN 0 ELSE 1 END AS isPk,
  '${ContextValue.COLUMN}' as type
FROM information_schema.columns AS C
LEFT JOIN (
  SELECT KCU.table_schema, KCU.table_name, KCU.column_name
  FROM information_schema.table_constraints AS TC
  INNER JOIN information_schema.key_column_usage AS KCU
    ON TC.constraint_name = KCU.constraint_name
    AND TC.table_schema = KCU.table_schema
    AND TC.table_name = KCU.table_name
  WHERE TC.constraint_type = 'PRIMARY KEY'
) AS PK
  ON PK.table_schema = C.table_schema
  AND PK.table_name = C.table_name
  AND PK.column_name = C.column_name
WHERE C.table_schema NOT IN ('information_schema', 'pg_catalog')
${p => filterTableList(p.tables || [])}
${p => p.search
  ? `AND (
    LOWER(C.table_name || '.' || C.column_name) LIKE '%${escapeLiteral(p.search.toLowerCase())}%'
    OR LOWER(C.column_name) LIKE '%${escapeLiteral(p.search.toLowerCase())}%'
  )`
  : ''
}
ORDER BY C.column_name ASC,
  C.ordinal_position ASC
LIMIT ${p => p.limit || 100}
`;

export default {
  describeTable,
  countRecords,
  fetchColumns,
  fetchRecords,
  fetchTables,
  fetchViews,
  searchTables,
  searchColumns
}
// export default {
//   listFks: `PRAGMA foreign_key_list(\':table\');`
// } as IBaseQueries;
