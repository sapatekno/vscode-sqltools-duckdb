import { ILanguageServerPlugin } from '@sqltools/types';
import DuckDB from './driver';
import { DRIVER_ALIASES } from './../constants';

const DuckDBDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, DuckDB as any);
    });
  }
}

export default DuckDBDriverPlugin;
