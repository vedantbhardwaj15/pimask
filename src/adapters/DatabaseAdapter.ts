import { Readable } from 'stream';

export interface ForeignKeyRelation {
  table: string;
  column: string;
  referencedTable: string;
  referencedColumn: string;
}

export interface DatabaseAdapter {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  getTables(): Promise<string[]>;
  getColumns(table: string): Promise<string[]>;
  getForeignKeys(): Promise<ForeignKeyRelation[]>;
  readStream(table: string, columns: string[]): Promise<Readable>;
  writeBatch(table: string, rows: any[]): Promise<void>;
}
