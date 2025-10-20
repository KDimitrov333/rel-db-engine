package db.engine.exec;

import java.util.Iterator;
import java.util.List;

import db.engine.index.IndexManager;
import db.engine.storage.RID;
import db.engine.storage.Record;
import db.engine.catalog.ColumnSchema;
import db.engine.catalog.TableSchema;
import db.engine.storage.StorageManager;

/**
 * Index scan operator using RID APIs in IndexManager.
 * Supports equality or range scan over INT key columns.
 */
public class IndexScanOperator implements Operator {
    private final IndexManager indexManager;
    private final StorageManager storage;
    private final String indexName;
    private final Integer equalityKey; // if non-null => equality scan
    private final Integer rangeLow;
    private final Integer rangeHigh;

    private List<RID> rids;
    private Iterator<RID> iter;
    private String tableName;
    private List<ColumnSchema> schema; // table schema for produced rows
    private boolean opened;

    // Equality constructor
    public IndexScanOperator(IndexManager indexManager, StorageManager storage, String indexName, int key) {
        this(indexManager, storage, indexName, key, null, null);
    }

    // Range factory
    public static IndexScanOperator range(IndexManager indexManager, StorageManager storage, String indexName, int lowInclusive, int highInclusive) {
        return new IndexScanOperator(indexManager, storage, indexName, null, lowInclusive, highInclusive);
    }

    private IndexScanOperator(IndexManager indexManager, StorageManager storage, String indexName,
                              Integer equalityKey, Integer rangeLow, Integer rangeHigh) {
        this.indexManager = indexManager;
        this.storage = storage;
        this.indexName = indexName;
        this.equalityKey = equalityKey;
        this.rangeLow = rangeLow;
        this.rangeHigh = rangeHigh;
    }

    @Override
    public void open() {
        if (opened) return;
        // Validate index existence early
        this.tableName = indexManager.getTableForIndex(indexName);
        // Acquire schema metadata for projection propagation
        TableSchema ts = storage.getCatalog().getTableSchema(tableName);
        this.schema = (ts != null) ? ts.columns() : null;
        if (equalityKey != null) {
            rids = indexManager.searchRids(indexName, equalityKey);
        } else if (rangeLow != null && rangeHigh != null) {
            rids = indexManager.rangeSearchRids(indexName, rangeLow, rangeHigh);
        } else {
            throw new IllegalStateException("IndexScanOperator requires equality key or range bounds");
        }
        iter = rids.iterator();
        opened = true;
    }

    @Override
    public Row next() {
        if (!opened || iter == null) return null;
        if (!iter.hasNext()) return null;
        RID rid = iter.next();
        Record rec = storage.read(tableName, rid);
        return Row.of(rec, rid, schema);
    }

    @Override
    public void close() {
        rids = null;
        iter = null;
        schema = null;
        opened = false;
    }
    
    @Override
    public List<ColumnSchema> schema() { return schema; }
}
