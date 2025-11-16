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
    private final PredicateCompiler compiler = new PredicateCompiler();

    public QueryProcessor(CatalogManager catalog, StorageManager storage, IndexManager indexManager) {
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
            return stream(trimmed);
        } else if (upper.startsWith("INSERT")) {
            return List.of(executeInsert(trimmed));
        } else if (upper.startsWith("DELETE")) {
            return List.of(executeDelete(trimmed));
        } else {
            throw new IllegalArgumentException("Unrecognized statement (expected SELECT/INSERT/DELETE): " + sql);
        }
    }

    /** Parse and execute an INSERT; returns diagnostic row. */
    public Row executeInsert(String sql) {
        InsertQuery iq = parser.parseInsert(sql.trim().endsWith(";") ? sql.trim().substring(0, sql.trim().length()-1).trim() : sql.trim());
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
        RID rid = storage.insert(iq.tableName(), rec);
        return Row.of(new Record(List.of("INSERT", rid.pageId(), rid.slotId())), rid);
    }

    /** Parse and execute a DELETE; returns diagnostic row. */
    public Row executeDelete(String sql) {
        DeleteQuery dq = parser.parseDelete(sql.trim().endsWith(";") ? sql.trim().substring(0, sql.trim().length()-1).trim() : sql.trim());
        var ts = storage.getCatalog().getTableSchema(dq.tableName());
        if (ts == null) throw new IllegalArgumentException("Unknown table: " + dq.tableName());
        var cols = ts.columns();
        final Predicate rowPred;
        if (dq.where() != null) {
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
        int deleted = counter[0];
        return Row.of(new Record(List.of("DELETE", deleted)), new RID(-1, -1));
    }
}
