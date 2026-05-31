import { Pool, PoolClient } from 'pg';
import QueryStream from 'pg-query-stream';
import { Readable } from 'stream';
import { DatabaseAdapter, ForeignKeyRelation } from './DatabaseAdapter';
import { DatabaseConnection } from '../config';

export class PostgresAdapter implements DatabaseAdapter {
  private pool: Pool;

  constructor(connection: DatabaseConnection) {
    this.pool = new Pool({
      host: connection.host,
      port: connection.port,
      database: connection.database,
      user: connection.user,
      password: connection.password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });
  }

  async connect(): Promise<void> {
    const client = await this.pool.connect();
    client.release();
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  async getTables(): Promise<string[]> {
    const query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE';
    `;
    const res = await this.pool.query(query);
    return res.rows.map((row) => row.table_name);
  }

  async getForeignKeys(): Promise<ForeignKeyRelation[]> {
    const query = `
      SELECT
          tc.table_name AS table,
          kcu.column_name AS column,
          ccu.table_name AS referencedtable,
          ccu.column_name AS referencedcolumn
      FROM
          information_schema.table_constraints AS tc
          JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
    `;
    const res = await this.pool.query(query);
    return res.rows.map((row) => ({
      table: row.table,
      column: row.column,
      referencedTable: row.referencedtable,
      referencedColumn: row.referencedcolumn,
    }));
  }

  async readStream(table: string, columns: string[]): Promise<Readable> {
    const client = await this.pool.connect();
    const quotedColumns = [];
    for(const column of columns){
      quotedColumns.push('"'+column+'"');
    }
    const columnString = quotedColumns.join(', ');
    const queryText = 'SELECT ' + columnString + ' FROM "'+ table + '"';
   
    const queryStream = new QueryStream(queryText);
    
    const stream = client.query(queryStream);

    const cleanup = () => {
      client.release();
    };

    stream.on('end', cleanup);
    stream.on('close', cleanup);
    stream.on('error', (err) => {
      cleanup();
    });

    return stream;
  }

  async writeBatch(table: string, rows: any[]): Promise<void> {
    if (rows.length === 0) return;

    const columns = Object.keys(rows[0]);
    const values: any[] = [];
    
    const placeholders = rows.map((row, rowIndex) => {
      const rowPlaceholders = columns.map((col, colIndex) => {
        values.push(row[col]);
        return `$${rowIndex * columns.length + colIndex + 1}`;
      });
      return `(${rowPlaceholders.join(', ')})`;
    }).join(', ');

    const queryText = `
      INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')}) 
      VALUES ${placeholders};
    `;

    await this.pool.query(queryText, values);
  }
}
