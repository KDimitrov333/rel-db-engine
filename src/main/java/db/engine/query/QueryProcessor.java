package db.engine.query;

import db.engine.catalog.CatalogManager;
import db.engine.exec.Operator;
import db.engine.exec.Row;
import db.engine.index.IndexManager;
import db.engine.storage.StorageManager;

/**
 * Processor combining parsing, planning, and streaming execution.
 */
public class QueryProcessor {
    private final QueryParser parser = new QueryParser();
    private final QueryPlanner planner;
    private final QueryExecutor executor = new QueryExecutor();

    public QueryProcessor(CatalogManager catalog, StorageManager storage, IndexManager indexManager) {
        PredicateCompiler compiler = new PredicateCompiler();
        this.planner = new QueryPlanner(catalog, storage, compiler, indexManager);
    }

    public Iterable<Row> stream(String sql) {
        SelectQuery logical = parser.parse(sql);
        Operator physical = planner.plan(logical);
        return executor.stream(physical);
    }

    // Expose for testing/verifying parser only.
    public SelectQuery parse(String sql) { return parser.parse(sql); }
}
