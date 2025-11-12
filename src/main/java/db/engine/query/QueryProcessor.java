package db.engine.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.engine.catalog.CatalogManager;
import db.engine.exec.Operator;
import db.engine.exec.Predicate;
import db.engine.exec.Row;
import db.engine.index.IndexManager;
import db.engine.storage.RID;
import db.engine.storage.StorageManager;
import db.engine.storage.Record;

/**
 * Processor combining parsing, planning, and streaming execution.
 */
public class QueryProcessor {
    private final QueryParser parser = new QueryParser();
    private final QueryPlanner planner;
    private final QueryExecutor executor = new QueryExecutor();
    private final StorageManager storage;

    public QueryProcessor(CatalogManager catalog, StorageManager storage, IndexManager indexManager) {
        PredicateCompiler compiler = new PredicateCompiler();
        this.planner = new QueryPlanner(catalog, storage, compiler, indexManager);
        this.storage = storage;
    }

    public Iterable<Row> stream(String sql) {
        SelectQuery logical = parser.parseSelect(sql);
        Operator physical = planner.plan(logical);
        return executor.stream(physical);
    }

    /**
     * Unified execution entry point.
     * SELECT  -> returns streamed rows.
     * INSERT  -> returns single diagnostic row: ["INSERT", pageId, slotId].
     * DELETE  -> returns single diagnostic row: ["DELETE", deletedCount].
     */
    public Iterable<Row> execute(String sql) {
        if (sql == null) throw new IllegalArgumentException("sql must not be null");
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) trimmed = trimmed.substring(0, trimmed.length()-1).trim();
        String upper = trimmed.toUpperCase();
        if (upper.startsWith("SELECT")) {
            // Stream rows directly
            return stream(trimmed);
        } else if (upper.startsWith("INSERT")) {
            InsertQuery iq = parser.parseInsert(trimmed);
            RID rid = executeInsert(iq); // single parse
            Row r = Row.of(new Record(List.of("INSERT", rid.pageId(), rid.slotId())), rid);
            return List.of(r);
        } else if (upper.startsWith("DELETE")) {
            DeleteQuery dq = parser.parseDelete(trimmed);
            int deleted = executeDelete(dq);
            Row r = Row.of(new Record(List.of("DELETE", deleted)), new RID(-1, -1));
            return List.of(r);
        } else {
            throw new IllegalArgumentException("Unrecognized statement (expected SELECT/INSERT/DELETE): " + sql);
        }
    }

    // Expose for testing/verifying parser only.
    public SelectQuery parseSelect(String sql) { return parser.parseSelect(sql); }

    /** Execute an INSERT statement and return the RID of the inserted record. */
    public RID executeInsert(String sql) {
        InsertQuery iq = parser.parseInsert(sql);
        // Resolve table schema
        var ts = storage.getCatalog().getTableSchema(iq.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + iq.tableName());
        // Map columns to positions, build values in schema order.
        var schemaCols = ts.columns();
        Map<String,Integer> posMap = new HashMap<>();
        for (int i=0;i<schemaCols.size();i++) posMap.put(schemaCols.get(i).name(), i);
        Object[] full = new Object[schemaCols.size()];
        for (int i=0;i<iq.columns().size();i++) {
            String col = iq.columns().get(i);
            Integer pos = posMap.get(col);
            if (pos == null) throw new IllegalArgumentException("Column not found in table schema: " + col);
            full[pos] = iq.values().get(i);
        }
        // Ensure all columns provided
        for (int i=0;i<full.length;i++) if (full[i] == null)
            throw new IllegalArgumentException("Missing value for column '" + schemaCols.get(i).name() + "' (no default support)");
        var rec = new Record(Arrays.asList(full));
        return storage.insert(iq.tableName(), rec);
    }

    // Internal helper avoiding re-parse
    private RID executeInsert(InsertQuery iq) {
        var ts = storage.getCatalog().getTableSchema(iq.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + iq.tableName());
        var schemaCols = ts.columns();
        Map<String,Integer> posMap = new HashMap<>();
        for (int i=0;i<schemaCols.size();i++) posMap.put(schemaCols.get(i).name(), i);
        Object[] full = new Object[schemaCols.size()];
        for (int i=0;i<iq.columns().size();i++) {
            String col = iq.columns().get(i);
            Integer pos = posMap.get(col);
            if (pos == null) throw new IllegalArgumentException("Column not found in table schema: " + col);
            full[pos] = iq.values().get(i);
        }
        for (int i=0;i<full.length;i++) if (full[i] == null)
            throw new IllegalArgumentException("Missing value for column '" + schemaCols.get(i).name() + "' (no default support)");
        var rec = new Record(Arrays.asList(full));
        return storage.insert(iq.tableName(), rec);
    }

    /** Execute a DELETE statement; returns count of deleted rows. */
    public int executeDelete(String sql) {
        DeleteQuery dq = parser.parseDelete(sql);
        var ts = storage.getCatalog().getTableSchema(dq.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + dq.tableName());
        var cols = ts.columns();

        // Build row-level predicate if WHERE present
        final Predicate rowPred;
        if (dq.where() != null) {
            PredicateCompiler compiler = new PredicateCompiler();
            SelectQuery synthetic = new SelectQuery(dq.tableName(), cols.stream().map(c -> c.name()).toList(), dq.where());
            rowPred = compiler.compile(synthetic, cols);
        } else {
            rowPred = null;
        }

        final int[] counter = new int[1];
        storage.scan(dq.tableName(), (rid, record) -> {
            if (rowPred == null || rowPred.test(Row.of(record, rid, cols))) {
                storage.delete(dq.tableName(), rid);
                counter[0]++;
            }
        });
        return counter[0];
    }

    // Internal helper avoiding re-parse
    private int executeDelete(DeleteQuery dq) {
        var ts = storage.getCatalog().getTableSchema(dq.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + dq.tableName());
        var cols = ts.columns();
        final Predicate rowPred;
        if (dq.where() != null) {
            PredicateCompiler compiler = new PredicateCompiler();
            SelectQuery synthetic = new SelectQuery(dq.tableName(), cols.stream().map(c -> c.name()).toList(), dq.where());
            rowPred = compiler.compile(synthetic, cols);
        } else {
            rowPred = null;
        }
        final int[] counter = new int[1];
        storage.scan(dq.tableName(), (rid, record) -> {
            if (rowPred == null || rowPred.test(Row.of(record, rid, cols))) {
                storage.delete(dq.tableName(), rid);
                counter[0]++;
            }
        });
        return counter[0];
    }
}
