package db.engine.query;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import db.engine.catalog.CatalogManager;
import db.engine.exec.Operator;
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
        SelectQuery logical = parser.parse(sql);
        Operator physical = planner.plan(logical);
        return executor.stream(physical);
    }

    // Expose for testing/verifying parser only.
    public SelectQuery parse(String sql) { return parser.parse(sql); }

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
}
