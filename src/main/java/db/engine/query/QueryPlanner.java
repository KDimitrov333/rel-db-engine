package db.engine.query;

import java.util.ArrayList;
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
import db.engine.exec.JoinOperator;
import db.engine.exec.Row;
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
        // If join present, build left & right sources first; else single-table plan.
        Operator root;
        List<ColumnSchema> finalSchema;
        if (query.join() != null) {
            // Left side
            TableSchema leftTs = catalog.getTableSchema(query.tableName());
            if (leftTs == null) throw new IllegalArgumentException("Unknown table (left): " + query.tableName());
            List<ColumnSchema> leftSchema = leftTs.columns();
            Operator leftSource = new SeqScanOperator(storage, query.tableName());
            // Right side
            String rightTable = query.join().rightTable();
            TableSchema rightTs = catalog.getTableSchema(rightTable);
            if (rightTs == null) throw new IllegalArgumentException("Unknown table (right): " + rightTable);
            List<ColumnSchema> rightSchema = rightTs.columns();
            Operator rightSource = new SeqScanOperator(storage, rightTable);
            // Build join
            root = new JoinOperator(withSchema(leftSource, leftSchema), withSchema(rightSource, rightSchema),
                                    query.join().leftColumn(), query.join().rightColumn());
            finalSchema = concatSchemas(leftSchema, rightSchema);
                if (query.where() != null) {
                    // Compile predicate against combined schema by constructing synthetic SelectQuery with base table only (join ignored by compiler) and combined schema passed explicitly.
                    Predicate pred = predicateCompiler.compile(new SelectQuery(query.tableName(), List.of(), query.where(), null), finalSchema);
                    root = new FilterOperator(root, pred);
                }
        } else {
            TableSchema ts = catalog.getTableSchema(query.tableName());
            if (ts == null) throw new IllegalArgumentException("Unknown table: " + query.tableName());
            List<ColumnSchema> schema = ts.columns();
            Predicate pred = null;
            RangePlan rangePlan = tryRangeIndex(query, schema);
            if (rangePlan != null) {
                root = IndexScanOperator.range(indexManager, storage, rangePlan.indexName, rangePlan.low, rangePlan.high);
            } else if (canUseIntEqualityIndex(query, schema)) {
                Condition c = query.where().conditions().get(0);
                String indexName = findIndexForColumn(query.tableName(), c.columnName());
                int key = (Integer) c.literalValue();
                root = new IndexScanOperator(indexManager, storage, indexName, key);
            } else {
                root = new SeqScanOperator(storage, query.tableName());
                pred = predicateCompiler.compile(query, schema); // may be null
                if (pred != null) root = new FilterOperator(root, pred);
            }
            finalSchema = schema;
        }

        List<String> cols = query.columns();
        if (cols != null && !cols.isEmpty()) {
            // Build projection indexes over final schema
            int[] idxs = new int[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                String name = cols.get(i);
                int found = -1;
                for (int j = 0; j < finalSchema.size(); j++) {
                    if (finalSchema.get(j).name().equals(name)) { found = j; break; }
                }
                if (found == -1) throw new IllegalArgumentException("Projection column not found: " + name);
                idxs[i] = found;
            }
            root = new ProjectionOperator(root, idxs);
        }
        return root;
    }

    private Operator withSchema(Operator op, List<ColumnSchema> schema) {
        // Wrap operator rows to inject schema if operator doesn't provide it.
        if (op.schema() != null) return op;
        return new Operator() {
            @Override public void open() { op.open(); }
            @Override public Row next() { return op.next(); }
            @Override public void close() { op.close(); }
            @Override public List<ColumnSchema> schema() { return schema; }
        };
    }

    private List<ColumnSchema> concatSchemas(List<ColumnSchema> left, List<ColumnSchema> right) {
        ArrayList<ColumnSchema> list = new ArrayList<>(left.size()+right.size());
        list.addAll(left); list.addAll(right); return list;
    }

    private boolean canUseIntEqualityIndex(SelectQuery query, List<ColumnSchema> schema) {
        if (query.where() == null || indexManager == null) return false;
        WhereClause w = query.where();
        if (!w.isSingle()) return false; // only single atomic condition eligible
        Condition c = w.conditions().get(0);
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

    // Attempt a range index usage from multiple AND-only comparisons on same INT indexed column.
    private RangePlan tryRangeIndex(SelectQuery query, List<ColumnSchema> schema) {
        if (query.where() == null || indexManager == null) return null;
        WhereClause w = query.where();
        if (w.conditions().size() < 2) return null;
        // All connectors must be AND.
        for (String conn : w.connectors()) if (!conn.equals("AND")) return null;
        String candidateCol = null;
        Integer low = null; Integer high = null;
        for (Condition c : w.conditions()) {
            if (c.negated()) return null; // skip NOT complexity
            if (!(c.literalValue() instanceof Integer)) return null;
            ColumnSchema colSchema = schema.stream().filter(cs -> cs.name().equals(c.columnName())).findFirst().orElse(null);
            if (colSchema == null || colSchema.type() != db.engine.catalog.DataType.INT) return null;
            String colName = colSchema.name();
            if (candidateCol == null) candidateCol = colName; else if (!candidateCol.equals(colName)) return null;
            int val = (Integer) c.literalValue();
            switch (c.op()) {
                case GT -> low = (low == null) ? val + 1 : Math.max(low, val + 1);
                case GTE -> low = (low == null) ? val : Math.max(low, val);
                case LT -> high = (high == null) ? val - 1 : Math.min(high, val - 1);
                case LTE -> high = (high == null) ? val : Math.min(high, val);
                case EQ -> {
                    low = (low == null) ? val : Math.max(low, val);
                    high = (high == null) ? val : Math.min(high, val);
                }
            }
        }
        if (candidateCol == null) return null;
        String indexName = findIndexForColumn(query.tableName(), candidateCol);
        if (indexName == null) return null;
        if (low != null && high != null && low.equals(high)) return null; // equality path covers this
        if (low == null && high == null) return null;
        if (low == null) low = Integer.MIN_VALUE;
        if (high == null) high = Integer.MAX_VALUE;
        if (low > high) return new RangePlan(indexName, 1, 0); // empty range
        return new RangePlan(indexName, low, high);
    }

    private static final class RangePlan {
        final String indexName; final int low; final int high;
        RangePlan(String indexName, int low, int high) { this.indexName = indexName; this.low = low; this.high = high; }
    }
}
