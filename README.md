# SQLTools DuckDB Driver (CLI)

This package integrates DuckDB support into the [vscode-sqltools](https://vscode-sqltools.mteixeira.dev/?umd_source=repository&utm_medium=readme&utm_campaign=sqlite) extension.

It executes queries via the native DuckDB CLI. Users may configure the executable location using the `duckdbCliPath` connection setting.

## Connection Options

- `database`: Path to the DuckDB database file (supports `:memory:`).
- `duckdbCliPath`: Custom path to the DuckDB CLI executable.
- `readOnly`: Opens the database in read-only mode (applies the `-readonly` flag).

## Changelog

### 0.1.0

- Forked from the official SQLite3 driver for SQLTools 0.60.
- Initial release.

## Contact & Support

- https://www.sapatekno.com
- admin@sapatekno.com