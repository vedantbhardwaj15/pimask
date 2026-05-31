import { ForeignKeyRelation } from '../adapters/DatabaseAdapter';

/**
 * Kahn's Algorithm for topological sorting.
 * Builds a directed graph from FK relations (parent → child),
 * then peels off nodes with zero in-degree to produce
 * a parent-first execution order.
 */
export function topologicalSort(
  tables: string[],
  foreignKeys: ForeignKeyRelation[]
): string[] {
  // Step 1: Build adjacency list and in-degree map
  const adjacency = new Map<string, string[]>();
  const inDegree = new Map<string, number>();

  for (const table of tables) {
    adjacency.set(table, []);
    inDegree.set(table, 0);
  }

  for (const fk of foreignKeys) {
    // Edge direction: referencedTable (parent) → table (child)
    const parent = fk.referencedTable;
    const child = fk.table;

    // skip if either table is not in our list
    if (!adjacency.has(parent) || !adjacency.has(child)) continue;

    adjacency.get(parent)!.push(child);
    inDegree.set(child, (inDegree.get(child) || 0) + 1);
  }

  // Step 2: append the queue with all zero in-degree nodes (parents)
  const queue: string[] = [];
  for (const [table, degree] of inDegree) {
    if (degree === 0) {
      queue.push(table);
    }
  }

  // Step 3: Process queue — remove node, decrement children's in-degree
  const sorted: string[] = [];
  while (queue.length > 0) {
    const current = queue.shift()!;
    sorted.push(current);

    for (const child of adjacency.get(current)!) {
      const newDegree = inDegree.get(child)! - 1;
      inDegree.set(child, newDegree);
      if (newDegree === 0) {
        queue.push(child);
      }
    }
  }

  // Step 4: Cycle detection if sorted < tables, there's a circular FK
  if (sorted.length !== tables.length) {
    const missing = tables.filter(t => !sorted.includes(t));
    throw new Error(
      `Circular foreign key dependency detected among: ${missing.join(', ')}`
    );
  }

  return sorted;
}
