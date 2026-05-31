import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';

export interface DatabaseConnection {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
}

export interface AppConfig {
  source: {
    type: string;
    connection: DatabaseConnection;
  };
  target: {
    type: string;
    connection: DatabaseConnection;
  };
  redis: {
    host: string;
    port: number;
  };
  masking: {
    salt: string;
    rules: {
      [tableName: string]: {
        [columnName: string]: string;
      };
    };
  };
}

const configPath = path.resolve(process.cwd(), 'config.yaml');

function loadConfig(): AppConfig {
  try {
    const fileContents = fs.readFileSync(configPath, 'utf8');
    const doc = yaml.load(fileContents) as any;

    if (!doc.source || !doc.target || !doc.redis || !doc.masking) {
      throw new Error('Config is missing mandatory sections: source, target, redis, or masking');
    }

    return doc as AppConfig;
  } catch (e) {
    console.error(`Failed to load config.yaml at ${configPath}:`, e);
    process.exit(1);
  }
}

export const config = loadConfig();
