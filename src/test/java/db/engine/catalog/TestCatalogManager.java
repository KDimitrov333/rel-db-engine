package db.engine.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * In-memory only catalog for tests: avoids loading/saving JSON files that can
 * accumulate state across test runs and skew counts.
 */
public class TestCatalogManager extends CatalogManager {
    private final Map<String, TableSchema> tables = new HashMap<>();
    private final Map<String, IndexSchema> indexes = new HashMap<>();

    public TestCatalogManager() {
        // Intentionally do nothing to skip file load
    }

    @Override
    public boolean registerTable(TableSchema tSchema) {
        if (tables.containsKey(tSchema.name())) return false;
        tables.put(tSchema.name(), tSchema);
        return true;
    }

    @Override
    public TableSchema getTableSchema(String name) { return tables.get(name); }

    @Override
    public boolean registerIndex(IndexSchema iSchema) {
        if (indexes.containsKey(iSchema.name())) return false;
        indexes.put(iSchema.name(), iSchema);
        return true;
    }

    @Override
    public IndexSchema getIndexSchema(String name) { return indexes.get(name); }

    @Override
    public Map<String, IndexSchema> allIndexSchemas() { return Collections.unmodifiableMap(indexes); }
}
