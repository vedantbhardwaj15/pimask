import { DatabaseAdapter } from './DatabaseAdapter';
import { PostgresAdapter } from './PostgresAdapter';
import { DatabaseConnection } from '../config';

export function createAdapter(type: string, connection: DatabaseConnection): DatabaseAdapter {
  switch (type) {
    case 'postgres':
      return new PostgresAdapter(connection);
    default:
      throw new Error(`Unsupported database type: "${type}". Supported: postgres`);
  }
}
