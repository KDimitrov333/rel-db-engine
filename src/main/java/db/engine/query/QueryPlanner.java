package db.engine.query;

import java.util.List;

import db.engine.catalog.CatalogManager;
import db.engine.catalog.TableSchema;
import db.engine.catalog.ColumnSchema;
import db.engine.exec.Operator;
import db.engine.exec.SeqScanOperator;
import db.engine.exec.FilterOperator;
import db.engine.exec.ProjectionOperator;
import db.engine.exec.Predicate;
import db.engine.exec.IndexScanOperator;
import db.engine.index.IndexManager;
import db.engine.storage.StorageManager;

/**
 * Planner: builds physical pipeline for a SelectQuery.
 * Current strategy:
 *  1. If there's an INT equality condition on an indexed column: use IndexScanOperator (no extra filter).
 *  2. Otherwise: full table scan (SeqScanOperator) + optional FilterOperator.
 *  3. Apply ProjectionOperator if a non-empty column list was specified (empty list means SELECT *).
 */
public class QueryPlanner {
    private final CatalogManager catalog;
    private final StorageManager storage;
    private final PredicateCompiler predicateCompiler;
    private final IndexManager indexManager; // optional index metadata

    public QueryPlanner(CatalogManager catalog, StorageManager storage,
                        PredicateCompiler predicateCompiler, IndexManager indexManager) {
        this.catalog = catalog;
        this.storage = storage;
        this.predicateCompiler = predicateCompiler;
        this.indexManager = indexManager;
    }

    public Operator plan(SelectQuery query) {
        TableSchema ts = catalog.getTableSchema(query.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + query.tableName());
        List<ColumnSchema> schema = ts.columns();

        Operator root;
        Predicate pred = null;

        // Decide on index usage: only INT equality supported.
        if (canUseIntEqualityIndex(query, schema)) {
            // Acquire index name and run index scan; predicate fully satisfied.
            String indexName = findIndexForColumn(query.tableName(), query.condition().columnName());
            int key = (Integer) query.condition().literalValue();
            root = new IndexScanOperator(indexManager, storage, indexName, key);
        } else {
            root = new SeqScanOperator(storage, query.tableName());
            pred = predicateCompiler.compile(query, schema); // may be null
            if (pred != null) {
                root = new FilterOperator(root, pred);
            }
        }

        List<String> cols = query.columns();
        if (cols != null && !cols.isEmpty()) {
            int[] idxs = new int[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                String name = cols.get(i);
                int found = -1;
                for (int j = 0; j < schema.size(); j++) {
                    if (schema.get(j).name().equals(name)) { found = j; break; }
                }
                if (found == -1) throw new IllegalArgumentException("Projection column not found: " + name);
                idxs[i] = found;
            }
            root = new ProjectionOperator(root, idxs);
        }
        return root;
    }

    private boolean canUseIntEqualityIndex(SelectQuery query, List<ColumnSchema> schema) {
        if (query.condition() == null || indexManager == null) return false;
        Condition c = query.condition();
        if (c.op() != Condition.Op.EQ) return false;
        // Find column & type
        for (ColumnSchema col : schema) {
            if (col.name().equals(c.columnName())) {
                if (col.type() != db.engine.catalog.DataType.INT) return false;
                if (!(c.literalValue() instanceof Integer)) return false;
                return findIndexForColumn(query.tableName(), c.columnName()) != null;
            }
        }
        return false; // column not found triggers predicate compiler error later
    }

    private String findIndexForColumn(String tableName, String columnName) {
        if (indexManager == null) return null;
        var all = catalog.allIndexSchemas();
        for (var entry : all.entrySet()) {
            var idx = entry.getValue();
            if (idx.table().equals(tableName) && idx.column().equals(columnName)) return idx.name();
        }
        return null;
    }
}
