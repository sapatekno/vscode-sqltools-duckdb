# SQLTools DuckDB Driver (CLI)

This package integrates DuckDB support into the [vscode-sqltools](https://vscode-sqltools.mteixeira.dev/) extension.

It executes queries via the native DuckDB CLI. Users may configure the executable location using the `duckdbCliPath` connection setting.

## Prerequisite: DuckDB CLI

Before using this extension, make sure DuckDB CLI is installed on your system.
Follow the official installation guide:

- https://duckdb.org/install/

After installation, ensure the `duckdb` executable is available in your `PATH`, or set it explicitly via `duckdbCliPath`.

## Connection Options

- `database`: Path to the DuckDB database file (supports `:memory:`).
- `duckdbCliPath`: Custom path to the DuckDB CLI executable.
- `readOnly`: Opens the database in read-only mode (applies the `-readonly` flag).

## Testing Status

This extension is currently in a testing phase.

- Tested on Windows.
- Tested on WSL to ensure it runs on Linux environments.
- Not yet tested on macOS, but in theory it should work as long as DuckDB CLI is installed and reachable.

## Changelog

### 0.1.0

- Initial release.

## Contact & Support

- https://www.sapatekno.com
- admin@sapatekno.com
