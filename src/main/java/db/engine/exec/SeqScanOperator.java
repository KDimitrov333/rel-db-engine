package db.engine.exec;

import java.util.Iterator;
import java.util.List;

import db.engine.storage.Record;
import db.engine.storage.StorageManager;

/**
 * Physical operator that performs a full table scan
 * Used when no index is applied or when the query requests all rows
 */
public class SeqScanOperator implements Operator {
    private final StorageManager storage;
    private final String tableName;
    private List<Record> records;
    private Iterator<Record> iter;

    public SeqScanOperator(StorageManager storage, String tableName) {
        this.storage = storage;
        this.tableName = tableName;
    }

    @Override
    public void open() {
        this.records = storage.scanTable(tableName);
        this.iter = records.iterator();
    }

    @Override
    public Record next() {
        if (iter == null) return null;
        return iter.hasNext() ? iter.next() : null;
    }

    @Override
    public void close() {
        records = null;
        iter = null;
    }
}
